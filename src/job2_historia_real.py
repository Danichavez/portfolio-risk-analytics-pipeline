# =====================================================
# JOB 2 — CONSTRUCCIÓN DE HISTORIA REAL
#
# Responsabilidad:
# - Leer staging (S3 Parquet)
# - Filtrar instrumentos excluidos del modelo
# - Identificar universo activo al día de corte
# - Escribir historia real en S3
#
# Decisiones de diseño:
# - Excluye FWD, MTM, CAJA, CC2, CC3 (no generan retorno de mercado)
# - Solo instrumentos con dato real al corte entran al universo
# - Salida particionada por corte para lectura eficiente en Job 3
# =====================================================

import sys
import time
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# -----------------------------------------------------
# PARÁMETROS
# -----------------------------------------------------
start_time = time.time()

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "FECHA_PROCESO", "S3_INPUT_PATH", "S3_OUTPUT_PATH"],
)

FECHA_PROCESO = datetime.strptime(args["FECHA_PROCESO"], "%Y-%m-%d").date()
S3_INPUT = args.get("S3_INPUT_PATH", "s3://risk-analytics-bucket/staging/returns/")
S3_OUTPUT = args.get("S3_OUTPUT_PATH", "s3://risk-analytics-bucket/output_real/")

# -----------------------------------------------------
# SPARK / GLUE
# -----------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Iniciando Job 2 — Historia Real | Corte: {FECHA_PROCESO}")

# -----------------------------------------------------
# 1. LECTURA DE STAGING
# -----------------------------------------------------
df_staging = (
    spark.read.parquet(S3_INPUT)
    .withColumn("fecha", F.to_date(F.col("fecha")))
)

# -----------------------------------------------------
# 2. EXCLUSIÓN DE INSTRUMENTOS NO MODELABLES
# -----------------------------------------------------
# Instrumentos que no generan retorno de mercado:
# - FWD: forwards (valorización MTM reflejada en otras posiciones)
# - MTM: mark-to-market (no es posición real)
# - ACT+CAJA: caja (sin retorno de mercado)
# - ACT+CC2/CC3: cuentas corrientes

cond_excluye = (
    F.upper(F.trim(F.col("clase"))).isin("FWD", "MTM", "FWD - MTM")
    | (
        (F.upper(F.trim(F.col("clase"))) == "ACT")
        & F.upper(F.trim(F.col("C1"))).isin("CAJA", "MTM", "CC2", "CC3")
    )
)

df_filtrado = df_staging.filter(~cond_excluye)

# -----------------------------------------------------
# 3. IDENTIFICAR UNIVERSO ACTIVO AL CORTE
# -----------------------------------------------------
# Solo instrumentos que tienen dato real en la fecha de corte
df_universo_corte = (
    df_filtrado
    .filter(F.col("fecha") == F.lit(str(FECHA_PROCESO)))
    .select("nemotecnico", "tipo_fondo", "afp", "tipo_instr", "clase", "C1", "C2", "C3", "C4", "C5")
    .distinct()
)

n_instrumentos = df_universo_corte.count()
print(f"Universo activo al corte: {n_instrumentos} instrumentos")

if n_instrumentos == 0:
    raise Exception(f"No hay instrumentos con dato real al corte {FECHA_PROCESO}")

# -----------------------------------------------------
# 4. FILTRAR HISTORIA SOLO PARA UNIVERSO ACTIVO
# -----------------------------------------------------
# Inner join: solo retornos de instrumentos que existen al corte
df_historia = (
    df_filtrado
    .join(
        F.broadcast(df_universo_corte),
        on=["nemotecnico", "tipo_fondo", "afp"],
        how="inner",
    )
    .withColumn("corte", F.lit(str(FECHA_PROCESO)).cast("date"))
)

# -----------------------------------------------------
# 5. ESCRITURA EN S3
# -----------------------------------------------------
output_path = f"{S3_OUTPUT.rstrip('/')}/corte={FECHA_PROCESO}"

df_historia.write.mode("overwrite").parquet(output_path)

n_registros = df_historia.count()
print(f"Historia real escrita: {n_registros:,} registros en {output_path}")

# -----------------------------------------------------
# FIN
# -----------------------------------------------------
job.commit()
duration = round((time.time() - start_time) / 60, 2)
print(f"Tiempo total: {duration} minutos")
