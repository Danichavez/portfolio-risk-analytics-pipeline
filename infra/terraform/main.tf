# =====================================================
# Terraform — Pipeline de Métricas de Riesgo
# Infraestructura: Glue Jobs, S3, IAM, EventBridge, Step Functions
# =====================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# =====================================================
# VARIABLES
# =====================================================

variable "aws_region" {
  default = "us-east-1"
}

variable "environment" {
  default = "prod"
}

variable "project_name" {
  default = "risk-analytics"
}

variable "s3_bucket_name" {
  default = "risk-analytics-pipeline"
}

variable "redshift_cluster_id" {
  description = "ID del cluster Redshift existente"
  type        = string
}

variable "source_jdbc_url" {
  description = "URL JDBC de la fuente MySQL (vía Secrets Manager)"
  type        = string
  sensitive   = true
}

locals {
  prefix = "${var.project_name}-${var.environment}"
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# =====================================================
# S3 — ALMACENAMIENTO DEL PIPELINE
# =====================================================

resource "aws_s3_bucket" "pipeline" {
  bucket = "${local.prefix}-${var.s3_bucket_name}"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  rule {
    id     = "cleanup-staging"
    status = "Enabled"
    filter {
      prefix = "staging/"
    }
    expiration {
      days = 90
    }
  }

  rule {
    id     = "archive-history"
    status = "Enabled"
    filter {
      prefix = "output_final/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# =====================================================
# IAM — ROL PARA GLUE JOBS
# =====================================================

resource "aws_iam_role" "glue_execution" {
  name = "${local.prefix}-glue-role"
  tags = local.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "s3-access"
  role = aws_iam_role.glue_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:${local.prefix}-*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# =====================================================
# GLUE JOBS
# =====================================================

resource "aws_glue_job" "job1_staging" {
  name     = "${local.prefix}-job1-staging"
  role_arn = aws_iam_role.glue_execution.arn
  tags     = local.tags

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/job1_staging.py"
    python_version  = "3"
    name            = "glueetl"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  default_arguments = {
    "--SOURCE_JDBC_URL"  = var.source_jdbc_url
    "--S3_BUCKET"        = aws_s3_bucket.pipeline.id
    "--job-language"     = "python"
    "--enable-metrics"   = "true"
  }
}

resource "aws_glue_job" "job2_historia" {
  name     = "${local.prefix}-job2-historia-real"
  role_arn = aws_iam_role.glue_execution.arn
  tags     = local.tags

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/job2_historia_real.py"
    python_version  = "3"
    name            = "glueetl"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  default_arguments = {
    "--S3_INPUT_PATH"  = "s3://${aws_s3_bucket.pipeline.id}/staging/returns/"
    "--S3_OUTPUT_PATH" = "s3://${aws_s3_bucket.pipeline.id}/output_real/"
    "--job-language"   = "python"
    "--enable-metrics" = "true"
  }
}

resource "aws_glue_job" "job3_imputacion" {
  name     = "${local.prefix}-job3-imputacion"
  role_arn = aws_iam_role.glue_execution.arn
  tags     = local.tags

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/job3_imputacion.py"
    python_version  = "3"
    name            = "glueetl"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 3
  timeout           = 45

  default_arguments = {
    "--S3_INPUT_PATH"    = "s3://${aws_s3_bucket.pipeline.id}/output_real/"
    "--S3_OUTPUT_PATH"   = "s3://${aws_s3_bucket.pipeline.id}/output_final/"
    "--S3_CALENDAR_PATH" = "s3://${aws_s3_bucket.pipeline.id}/master/calendario/"
    "--job-language"     = "python"
    "--enable-metrics"   = "true"
  }
}

resource "aws_glue_job" "job4_analitica" {
  name     = "${local.prefix}-job4-analitica"
  role_arn = aws_iam_role.glue_execution.arn
  tags     = local.tags

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.id}/glue-scripts/job4_analitica.py"
    python_version  = "3"
    name            = "glueetl"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 60

  default_arguments = {
    "--S3_INPUT_PATH"      = "s3://${aws_s3_bucket.pipeline.id}/output_final/"
    "--SCHEMA_ANALYTICS"   = "analytics"
    "--TE_WINDOW_DAYS"     = "120"
    "--RET_CAP"            = "0.20"
    "--BENCHMARK_MODE"     = "AVG"
    "--job-language"       = "python"
    "--enable-metrics"     = "true"
  }
}

# =====================================================
# EVENTBRIDGE — TRIGGER DIARIO
# =====================================================

resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "${local.prefix}-daily-13h"
  description         = "Trigger diario a las 13:00 hora local"
  schedule_expression = "cron(0 16 ? * MON-FRI *)" # 16:00 UTC = 13:00 Chile
  tags                = local.tags
}

# =====================================================
# OUTPUTS
# =====================================================

output "s3_bucket" {
  value = aws_s3_bucket.pipeline.id
}

output "glue_role_arn" {
  value = aws_iam_role.glue_execution.arn
}

output "job_names" {
  value = {
    job1 = aws_glue_job.job1_staging.name
    job2 = aws_glue_job.job2_historia.name
    job3 = aws_glue_job.job3_imputacion.name
    job4 = aws_glue_job.job4_analitica.name
  }
}
