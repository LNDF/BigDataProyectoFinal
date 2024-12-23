# ¿Cómo han evolucionado los votos nulos entre 1994 y 2024 por provincia?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round
from dotenv import load_dotenv
import os

APP_NAME = "script0"

load_dotenv()

INPUT_DIR = str(os.getenv("DATA_DIR"))
OUTPUT_DIR = str(os.getenv("OUTPUT_DIR"))

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("yarn") \
    .getOrCreate()

# Ruta de entrada y salida
data_dir = INPUT_DIR
output_dir = f"{OUTPUT_DIR}/{APP_NAME}/"

# Leer los datos desde HDFS
df = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(data_dir)

# Asegurarse de que las columnas relevantes sean numéricas
df = df.withColumn("VOTOS", col("VOTOS").cast("integer")) \
       .withColumn("NULOS", col("NULOS").cast("integer"))

# Sumar los valores de votos nulos y votantes por provincia y año
df_aggregated = df.groupBy("TH", "FECHA").agg(
    spark_sum("NULOS").alias("TOTAL_NULOS"),
    spark_sum("VOTOS").alias("TOTAL_VOTANTES")
)

# Calcular el porcentaje de votos nulos por provincia y año
df_aggregated = df_aggregated.withColumn(
    "PORCENTAJE_NULOS",
    round((col("TOTAL_NULOS") / col("TOTAL_VOTANTES")) * 100, 2)
)

# Guardar los resultados en HDFS
df_aggregated.write.mode("overwrite").option("header", "true").csv(output_dir)

# Mostrar una muestra de los datos procesados
df_aggregated.show(truncate=False)

# Finalizar sesión
spark.stop()