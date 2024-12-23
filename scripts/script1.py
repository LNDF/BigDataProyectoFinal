# Este script analiza la evolución de la participación electoral en cada provincia (TH) entre 1994 y 2024.
# Genera un conjunto de datos que muestra cómo ha evolucionado la participación promedio por provincia
# a lo largo de los años y proporciona una visión completa de los cambios más significativos.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg as spark_avg
from dotenv import load_dotenv
import os

APP_NAME = "script1"

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
       .withColumn("CENSO", col("CENSO").cast("integer")) \
       .withColumn("FECHA", col("FECHA").cast("integer"))

# Calcular la participación como porcentaje
df = df.withColumn("PARTICIPACION", (col("VOTOS") / col("CENSO")) * 100)

# Filtrar los datos para los años entre 1994 y 2024
data_filtered = df.filter((col("FECHA") >= 1994) & (col("FECHA") <= 2024))

# Calcular la participación promedio por provincia (TH) y año
data_grouped = data_filtered.groupBy("TH", "FECHA").agg(spark_avg("PARTICIPACION").alias("PARTICIPACION"))

# Almacenar los datos procesados en HDFS
data_grouped.write.mode("overwrite").option("header", "true").csv(output_dir)

# Mostrar una muestra de los datos procesados
data_grouped.show(truncate=False)

# Finalizar sesión
spark.stop()
