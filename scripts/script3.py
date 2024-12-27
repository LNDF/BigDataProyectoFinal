from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round, regexp_replace, format_number
from dotenv import load_dotenv
import os

APP_NAME = "script3"

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
df = df.withColumn("CENSO", col("CENSO").cast("integer")) \
       .withColumn("ABSTENCION", col("ABSTENCION").cast("integer"))

# Calcular el porcentaje de abstención para cada municipio y año
df = df.withColumn("PORCENTAJE_ABSTENCION", round((col("ABSTENCION") / col("VOTOS")) * 100, 2))

# Sumar los valores de abstención y censo por municipio y año
df_aggregated = df.groupBy("TH", "AMBITO", "FECHA").agg(
    spark_sum("ABSTENCION").alias("TOTAL_ABSTENCION"),
    spark_sum("VOTOS").alias("TOTAL_VOTOS")
)
df_aggregated = df_aggregated.withColumn("TOTAL_VOTOS", col("TOTAL_VOTOS").cast("integer"))

# Recalcular el porcentaje de abstención a nivel de municipio y año
df_aggregated = df_aggregated.withColumn(
    "PORCENTAJE_ABSTENCION", round((col("TOTAL_ABSTENCION") / col("TOTAL_VOTOS")) * 100, 2)
)
df_aggregated = df_aggregated.withColumn("PORCENTAJE_ABSTENCION", regexp_replace(col("PORCENTAJE_ABSTENCION"), "\\.", ","))

# Guardar los resultados en HDFS
df_aggregated.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Mostrar una muestra de los datos procesados
df_aggregated.show(truncate=False)

# Finalizar sesión
spark.stop()
