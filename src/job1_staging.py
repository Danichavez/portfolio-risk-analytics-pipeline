# =====================================================
# JOB 1 — STAGING INCREMENTAL (IDEMPOTENTE)
#
# Responsabilidad:
# - Extraer registros nuevos desde MySQL hacia S3
# - Escritura idempotente con dynamic partition overwrite
# - Detección automática de rango incremental
#
# Decisiones de diseño:
# - partitionOverwriteMode = "dynamic" → re-ejecuciones no duplican
# - Si no hay datos nuevos → sale limpio sin modificar staging
# - Primera ejecución: carga últimos 30 días como bootstrap
# - Particionado por year/month para lecturas eficientes
# =====================================================

import sys
import time
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_date, year, month, max as spark_max

# -----------------------------------------------------
# INICIALIZACIÓN
# -----------------------------------------------------
start_time = time.time()

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_JDBC_URL", "SOURCE_USER", "SOURCE_PASSWORD", "S3_BUCKET"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Idempotencia: solo sobreescribir particiones presentes en el output
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

staging_path = f"s3://{args['S3_BUCKET']}/staging/returns/"

print("Iniciando Job 1 — Staging Incremental")

# -----------------------------------------------------
# 1. ÚLTIMA FECHA EN ORIGEN
# -----------------------------------------------------
query_max_origen = "SELECT MAX(fecha) as max_fecha FROM consolidated_returns"

df_max_origen = (
    spark.read.format("jdbc")
    .option("url", args["SOURCE_JDBC_URL"])
    .option("dbtable", f"({query_max_origen}) t")
    .option("user", args["SOURCE_USER"])
    .option("password", args["SOURCE_PASSWORD"])
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

max_fecha_origen = df_max_origen.collect()[0]["max_fecha"]
if max_fecha_origen is None:
    raise Exception("No existe data en origen")
if isinstance(max_fecha_origen, datetime):
    max_fecha_origen = max_fecha_origen.date()

# -----------------------------------------------------
# 2. ÚLTIMA FECHA EN STAGING (S3)
# -----------------------------------------------------
try:
    df_staging = spark.read.parquet(staging_path).select(to_date(col("fecha")).alias("fecha"))
    max_fecha_staging = df_staging.agg(spark_max("fecha")).collect()[0][0]
except Exception:
    max_fecha_staging = None

print(f"Última fecha en staging: {max_fecha_staging}")

# -----------------------------------------------------
# 3. DETERMINAR RANGO INCREMENTAL
# -----------------------------------------------------
if max_fecha_staging is None:
    start_date = max_fecha_origen - timedelta(days=30)
    print(f"Primera carga desde: {start_date}")
else:
    start_date = max_fecha_staging + timedelta(days=1)
    print(f"Carga incremental desde: {start_date}")

if start_date > max_fecha_origen:
    print("No hay data nueva — saliendo")
    job.commit()
    sys.exit(0)

# -----------------------------------------------------
# 4. EXTRACCIÓN INCREMENTAL
# -----------------------------------------------------
query = f"""
SELECT fecha, nemotecnico, tipo_fondo, tipo_instr, afp, clase,
       C1, C2, C3, C4, C5, moneda, aporte_retorno, retorno,
       peso_valorizacion_economica_sub, peso_exposicion_economica
FROM consolidated_returns
WHERE fecha >= '{start_date}' AND fecha <= '{max_fecha_origen}'
"""

df_new = (
    spark.read.format("jdbc")
    .option("url", args["SOURCE_JDBC_URL"])
    .option("dbtable", f"({query}) t")
    .option("user", args["SOURCE_USER"])
    .option("password", args["SOURCE_PASSWORD"])
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("fetchsize", "50000")
    .load()
)

if df_new.count() == 0:
    print("Sin registros nuevos — saliendo")
    job.commit()
    sys.exit(0)

# -----------------------------------------------------
# 5. AGREGAR COLUMNAS DE PARTICIÓN
# -----------------------------------------------------
df_new = (
    df_new
    .withColumn("fecha", to_date(col("fecha")))
    .withColumn("year", year(col("fecha")))
    .withColumn("month", month(col("fecha")))
)

# -----------------------------------------------------
# 6. ESCRITURA IDEMPOTENTE
# -----------------------------------------------------
df_new.write.mode("overwrite").partitionBy("year", "month").parquet(staging_path)

print("Staging actualizado (idempotente — dynamic partition overwrite)")

# -----------------------------------------------------
# FIN
# -----------------------------------------------------
job.commit()
duration = round((time.time() - start_time) / 60, 2)
print(f"Tiempo total: {duration} minutos")
