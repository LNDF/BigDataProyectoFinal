# ¿Cómo varió el porcentaje de votos en blanco y nulos entre 1994 y 2024?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round, lag
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

APP_NAME = "script6"

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

# Leer los datos desde HDFS
df = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(data_dir)

# Asegurarse de que las columnas relevantes sean numéricas
df = df.withColumn("VOTOS", col("VOTOS").cast("integer")) \
       .withColumn("NULOS", col("NULOS").cast("integer")) \
       .withColumn("BLANCOS", col("BLANCOS").cast("integer"))

# Filtrar años relevantes (entre 1994 y 2024)
df_filtered = df.filter((col("FECHA") >= "1994") & (col("FECHA") <= "2024"))

# Sumar votos nulos, blancos y totales por año
df_aggregated = df_filtered.groupBy("FECHA").agg(
    spark_sum("NULOS").alias("TOTAL_NULOS"),
    spark_sum("BLANCOS").alias("TOTAL_BLANCOS"),
    spark_sum("VOTOS").alias("TOTAL_VOTANTES")
)

# Calcular el porcentaje de votos nulos y blancos por año
df_aggregated = df_aggregated.withColumn(
    "PORCENTAJE_NULOS",
    round((col("TOTAL_NULOS") / col("TOTAL_VOTANTES")) * 100, 2)
).withColumn(
    "PORCENTAJE_BLANCOS",
    round((col("TOTAL_BLANCOS") / col("TOTAL_VOTANTES")) * 100, 2)
)

# Ordenar por año
df_aggregated = df_aggregated.orderBy("FECHA")

# Calcular la variación porcentual año por año
window_spec = Window.orderBy("FECHA")
df_variation = df_aggregated.withColumn(
    "VARIACION_NULOS",
    round(col("PORCENTAJE_NULOS") - lag("PORCENTAJE_NULOS").over(window_spec), 2)
).withColumn(
    "VARIACION_BLANCOS",
    round(col("PORCENTAJE_BLANCOS") - lag("PORCENTAJE_BLANCOS").over(window_spec), 2)
)

# Mostrar resultados
df_variation.show(truncate=False)

# Guardar los resultados en HDFS
df_variation.write.mode("overwrite").option("header", "true").csv(output_dir)

# Finalizar sesión
spark.stop()