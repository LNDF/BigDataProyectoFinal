from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round, regexp_replace, lag, when
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os, re

APP_NAME = "script7"

load_dotenv()

DATA_DIR = str(os.getenv("DATA_DIR"))
OUTPUT_DIR = str(os.getenv("OUTPUT_DIR"))

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("yarn") \
    .getOrCreate()

# Ruta de entrada y salida
data_dir = DATA_DIR
output_dir = f"{OUTPUT_DIR}/{APP_NAME}/"

# Leer los datos
df = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(data_dir)

# Asegurarse de que las columnas relevantes sean numéricas
df = df.withColumn("CENSO", col("CENSO").cast("integer"))

# Definir los partidos con todas sus variaciones posibles
partidos = {
    "EAJ_PNV": ["EAJ_PNV", "EAJ_PNV___EA"],
    "EH_BILDU": ["EH_BILDU"],
    "PP": ["PP", "PP_Cs"],
    "PSE_EE_PSOE": ["PSE_EE_PSOE"],
    "VOX": ["VOX"],
    "SUMAR": ["SUMAR"],
    "PODEMOS": ["PODEMOS", "PODEMOS_AHAL_DUGU_IU", "PODEMOS_AHALDUGU_EZKER_AN", "PODEMOS_AHAL_DUGU___ALIANZA_VERDE"]
}

# Crear columnas con los votos de cada partido y sumar variantes correctamente
for partido, variantes in partidos.items():
    # Filtrar columnas existentes que coinciden con las variantes
    columnas_partido = [c for c in df.columns if any(re.search(variant, c) for variant in variantes)]
    if columnas_partido:
        # Sumar solo si existen columnas coincidentes
        expr_sum = sum([col(c).cast("integer").alias(f"{partido}_{c}") for c in columnas_partido])
        df = df.withColumn(partido, expr_sum)
    else:
        # Crear columna con valor 0 si no existe ninguna variante
        df = df.withColumn(partido, when(col("CENSO").isNotNull(), 0).otherwise(0))

# Agrupar para evitar duplicados antes de calcular los porcentajes
df_aggregated = df.groupBy("TH", "FECHA").agg(
    spark_sum("CENSO").alias("TOTAL_CENSO"),
    *[spark_sum(partido).alias(f"{partido}_TOTAL_VOTOS") for partido in partidos]
)

# Calcular porcentaje de votos por partido sobre el censo total
for partido in partidos:
    df_aggregated = df_aggregated.withColumn(
        f"{partido}_PCT", round((col(f"{partido}_TOTAL_VOTOS") / col("TOTAL_CENSO")) * 100, 2)
    )

# Agregar ventana para calcular el crecimiento porcentual
window_spec = Window.partitionBy("TH").orderBy("FECHA")

# Calcular crecimiento porcentual con lag
for partido in partidos:
    df_aggregated = df_aggregated.withColumn(
        f"{partido}_CRECIMIENTO", 
        round(col(f"{partido}_PCT") - lag(f"{partido}_PCT", 1).over(window_spec), 2)
    )

# Reemplazar puntos por comas para formato europeo
for partido in partidos:
    df_aggregated = df_aggregated.withColumn(f"{partido}_PCT", regexp_replace(col(f"{partido}_PCT").cast("string"), "\\.", ","))
    df_aggregated = df_aggregated.withColumn(f"{partido}_CRECIMIENTO", regexp_replace(col(f"{partido}_CRECIMIENTO").cast("string"), "\\.", ","))

# Guardar los resultados en HDFS
df_aggregated.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Mostrar resultados
df_aggregated.show(truncate=False)

# Finalizar sesión
spark.stop()
