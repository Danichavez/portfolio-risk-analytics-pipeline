# =====================================================
# JOB 3 — IMPUTACIÓN POR JERARQUÍA DE EQUIVALENCIA
#
# Responsabilidad:
# - Construir matriz completa de 252 días hábiles × instrumento
# - Completar retornos faltantes con jerarquía de 9 niveles
# - Aplicar reglas especiales: MON (CLP=0), SWP TASA c5=0
# - Registrar trazabilidad (origen: REAL / EQUIVALENTE / NO_EQUIVALENTE)
#
# Decisiones de diseño:
# - 9 niveles de generalización (del más específico al más general)
# - MON: retorno spot promedio por moneda, moneda local = 0
# - SWP TASA con plazo cero: retorno forzado a 0
# - Cada registro lleva columna de origen para auditoría
# =====================================================

import sys
import time
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------
# PARÁMETROS
# -----------------------------------------------------
start_time = time.time()

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "FECHA_PROCESO", "S3_INPUT_PATH", "S3_OUTPUT_PATH", "S3_CALENDAR_PATH"],
)

FECHA_PROCESO = datetime.strptime(args["FECHA_PROCESO"], "%Y-%m-%d").date()
S3_INPUT = args.get("S3_INPUT_PATH", "s3://risk-analytics-bucket/output_real/")
S3_OUTPUT = args.get("S3_OUTPUT_PATH", "s3://risk-analytics-bucket/output_final/")
S3_CALENDAR = args.get("S3_CALENDAR_PATH", "s3://risk-analytics-bucket/master/calendario/")

HISTORY_DAYS = 252  # Días hábiles de historia requeridos

# -----------------------------------------------------
# SPARK / GLUE
# -----------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Iniciando Job 3 — Imputación | Corte: {FECHA_PROCESO}")

# -----------------------------------------------------
# 1. LECTURA DE HISTORIA REAL + CALENDARIO
# -----------------------------------------------------
input_path = f"{S3_INPUT.rstrip('/')}/corte={FECHA_PROCESO}"
df_real = spark.read.parquet(input_path)

df_calendario = (
    spark.read.parquet(S3_CALENDAR)
    .filter(F.col("es_habil") == True)
    .filter(F.col("fecha") <= F.lit(str(FECHA_PROCESO)))
    .orderBy(F.col("fecha").desc())
    .limit(HISTORY_DAYS)
)

n_dias = df_calendario.count()
print(f"Días hábiles en ventana: {n_dias}")

# -----------------------------------------------------
# 2. UNIVERSO × TIEMPO (CROSS JOIN)
# -----------------------------------------------------
df_universo = (
    df_real
    .filter(F.col("fecha") == F.lit(str(FECHA_PROCESO)))
    .select("nemotecnico", "tipo_instr", "tipo_fondo", "clase", "afp",
            "C1", "C2", "C3", "C4", "C5")
    .distinct()
)

df_matriz = df_universo.crossJoin(df_calendario.select("fecha"))

# -----------------------------------------------------
# 3. LEFT JOIN CON DATOS REALES
# -----------------------------------------------------
df_con_reales = (
    df_matriz.alias("m")
    .join(
        df_real.alias("r"),
        on=[
            F.col("m.nemotecnico") == F.col("r.nemotecnico"),
            F.col("m.tipo_fondo") == F.col("r.tipo_fondo"),
            F.col("m.afp") == F.col("r.afp"),
            F.col("m.fecha") == F.col("r.fecha"),
        ],
        how="left",
    )
    .withColumn(
        "tiene_real",
        F.when(F.col("r.retorno").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
    )
)

# -----------------------------------------------------
# 4. JERARQUÍA DE EQUIVALENCIA (9 NIVELES)
# -----------------------------------------------------
# Definición de niveles: cada nivel relaja una columna de clasificación
HIERARCHY = [
    ["tipo_instr", "tipo_fondo", "clase", "afp", "C1", "C2", "C3", "C4", "C5"],  # Nivel 1: match exacto
    ["tipo_instr", "tipo_fondo", "clase", "afp", "C1", "C2", "C3", "C4"],         # Nivel 2: relaja C5
    ["tipo_instr", "tipo_fondo", "clase", "afp", "C1", "C2", "C3"],               # Nivel 3: relaja C4,C5
    ["tipo_instr", "tipo_fondo", "clase", "afp", "C1", "C2"],                     # Nivel 4
    ["tipo_instr", "tipo_fondo", "clase", "afp", "C1"],                           # Nivel 5
    ["tipo_instr", "tipo_fondo", "clase", "afp"],                                 # Nivel 6
    ["tipo_instr", "tipo_fondo", "clase"],                                        # Nivel 7
    ["tipo_instr", "tipo_fondo"],                                                 # Nivel 8
    ["tipo_instr"],                                                               # Nivel 9: más general
]

# Calcular retornos promedio por cada nivel de agrupación
# para usar como fuente de imputación
df_promedios_por_nivel = {}
for nivel_idx, cols in enumerate(HIERARCHY, start=1):
    df_promedios_por_nivel[nivel_idx] = (
        df_real
        .groupBy(["fecha"] + cols)
        .agg(F.avg("retorno").alias(f"retorno_nivel_{nivel_idx}"))
    )

# Aplicar jerarquía: buscar en cada nivel hasta encontrar un retorno
df_resultado = df_con_reales

for nivel_idx, cols in enumerate(HIERARCHY, start=1):
    col_retorno = f"retorno_nivel_{nivel_idx}"
    df_avg = df_promedios_por_nivel[nivel_idx]

    df_resultado = (
        df_resultado
        .join(df_avg, on=["fecha"] + cols, how="left")
        .withColumn(
            "retorno_imputado",
            F.coalesce(F.col("retorno_imputado") if "retorno_imputado" in df_resultado.columns else F.lit(None),
                       F.col(col_retorno)),
        )
        .withColumn(
            "jerarquia_usada",
            F.when(
                F.col("jerarquia_usada").isNull() & F.col(col_retorno).isNotNull(),
                F.lit(nivel_idx),
            ).otherwise(F.col("jerarquia_usada") if "jerarquia_usada" in df_resultado.columns else F.lit(None)),
        )
        .drop(col_retorno)
    )

# -----------------------------------------------------
# 5. RETORNO FINAL + ORIGEN
# -----------------------------------------------------
df_final = (
    df_resultado
    .withColumn(
        "retorno_final",
        F.when(F.col("tiene_real"), F.col("r.retorno"))
        .otherwise(F.col("retorno_imputado")),
    )
    .withColumn(
        "origen_completado",
        F.when(F.col("tiene_real"), F.lit("REAL"))
        .when(F.col("retorno_imputado").isNotNull(), F.lit("EQUIVALENTE"))
        .otherwise(F.lit("NO_EQUIVALENTE")),
    )
)

# -----------------------------------------------------
# 6. REGLAS ESPECIALES
# -----------------------------------------------------
# MON con moneda local (CLP): retorno = 0
df_final = df_final.withColumn(
    "retorno_final",
    F.when(
        (F.upper(F.trim(F.col("clase"))) == "MON") & (F.upper(F.trim(F.col("C1"))) == "CLP"),
        F.lit(0.0),
    ).otherwise(F.col("retorno_final")),
)

# SWP TASA con plazo cero (C5=0): retorno = 0
df_final = df_final.withColumn(
    "retorno_final",
    F.when(
        (F.upper(F.trim(F.col("clase"))) == "SWP")
        & (F.upper(F.trim(F.col("C2"))) == "TASA")
        & (F.regexp_replace(F.regexp_replace(F.trim(F.col("C5")), r"\.0+$", ""), r"Y$", "") == "0"),
        F.lit(0.0),
    ).otherwise(F.col("retorno_final")),
)

# -----------------------------------------------------
# 7. ESCRITURA
# -----------------------------------------------------
output_path = f"{S3_OUTPUT.rstrip('/')}/corte={FECHA_PROCESO}"

(
    df_final
    .select(
        "fecha", "nemotecnico", "tipo_instr", "tipo_fondo", "clase", "afp",
        "C1", "C2", "C3", "C4", "C5",
        F.col("retorno_final").alias("retorno"),
        "aporte_retorno",
        "peso_valorizacion_economica_sub",
        "peso_exposicion_economica",
        "origen_completado",
        "jerarquia_usada",
    )
    .write.mode("overwrite")
    .parquet(output_path)
)

print(f"Historia completada escrita en: {output_path}")

# -----------------------------------------------------
# FIN
# -----------------------------------------------------
job.commit()
duration = round((time.time() - start_time) / 60, 2)
print(f"Tiempo total: {duration} minutos")
