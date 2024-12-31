# ¿Cómo variaron los principales indicadores electorales (participación, abstención y distribución de votos por partido) entre las elecciones de 2020 y 2024 a nivel de municipio?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round
from dotenv import load_dotenv
import os

APP_NAME = "script4"

load_dotenv()

DATA_DIR = str(os.getenv("DATA_DIR"))
OUTPUT_DIR = str(os.getenv("OUTPUT_DIR"))

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("yarn") \
    .getOrCreate()

data_dir = DATA_DIR
output_dir = f"{OUTPUT_DIR}/{APP_NAME}/"

# Leer los datos desde HDFS
df = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(data_dir)

# Asegurarse de que las columnas relevantes sean numéricas
df = df.withColumn("CENSO", col("CENSO").cast("integer")) \
       .withColumn("VOTOS", col("VOTOS").cast("integer")) \
       .withColumn("ABSTENCION", col("ABSTENCION").cast("integer"))

# Filtrar datos para los años 2020 y 2024
df_filtered = df.filter((col("FECHA") == "2020") | (col("FECHA") == "2024"))

# Calcular participación y abstención porcentual
df_filtered = df_filtered.withColumn(
    "PARTICIPACION_PORCENTAJE", 
    round((col("VOTOS") / col("CENSO")) * 100, 2)
).withColumn(
    "ABSTENCION_PORCENTAJE", 
    round((col("ABSTENCION") / col("CENSO")) * 100, 2)
)

# Incluir todas las columnas de partidos como numéricas
partidos = [col_name for col_name in df.columns if col_name not in ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA"]]
df = df.select("AMBITO", "FECHA", *partidos).na.fill(0)
for partido in partidos:
    df_filtered = df_filtered.withColumn(partido, col(partido).cast("integer"))

# Sumar indicadores por municipio y año
df_aggregated = df_filtered.groupBy("AMBITO", "FECHA").agg(
    spark_sum("CENSO").alias("TOTAL_CENSO"),
    spark_sum("VOTOS").alias("TOTAL_VOTOS"),
    spark_sum("ABSTENCION").alias("TOTAL_ABSTENCION"),
    *[spark_sum(partido).alias(f"TOTAL_{partido}") for partido in partidos]
)

# Calcular indicadores porcentuales por municipio y año
df_aggregated = df_aggregated.withColumn(
    "PARTICIPACION_PORCENTAJE", 
    round((col("TOTAL_VOTOS") / col("TOTAL_CENSO")) * 100, 2)
).withColumn(
    "ABSTENCION_PORCENTAJE", 
    round((col("TOTAL_ABSTENCION") / col("TOTAL_CENSO")) * 100, 2)
)

for partido in partidos:
    df_aggregated = df_aggregated.withColumn(
        f"PORCENTAJE_{partido}", 
        round((col(f"TOTAL_{partido}") / col("TOTAL_VOTOS")) * 100, 2)
    )

# Guardar resultados en HDFS
df_aggregated.write.mode("overwrite").option("header", "true").csv(output_dir)

# Mostrar una muestra de los datos procesados
df_aggregated.show(truncate=False)

# Finalizar sesión
spark.stop()