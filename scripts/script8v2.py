from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, sum as _sum, round, abs as F_abs, when, format_number, regexp_replace
from dotenv import load_dotenv
import os

APP_NAME = "script8"

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
df = df.withColumn("EH_BILDU", col("EH_BILDU").cast("integer")) \
       .withColumn("EAJ_PNV", col("EAJ_PNV").cast("integer")) \
       .withColumn("EAJ_PNV___EA", col("EAJ_PNV___EA").cast("integer")) \
       .withColumn("VOTOS_CANDIDATOS", col("VOTOS_CANDIDATOS").cast("integer")) 

# Filtrar las filas desde el 2012 en adelante
df = df.filter(col("FECHA") >= "2012")

# Unificar las columnas de EAJ_PNV y EAJ_PNV___EA
df = df.withColumn("EAJ_PNV_UNIFICADO", coalesce(col("EAJ_PNV"), col("EAJ_PNV___EA")))

# Agrupar por ámbito y fecha y sumar las columnas relevantes
grouped = df.groupBy("AMBITO", "FECHA").agg(
    _sum("EH_BILDU").alias("EH_BILDU_SUM"),
    _sum("EAJ_PNV_UNIFICADO").alias("EAJ_PNV_UNIFICADO_SUM"),
    _sum("VOTOS_CANDIDATOS").alias("VOTOS_CANDIDATOS_SUM")
)

# Calcular el porcentaje por partido
grouped = grouped.withColumn(
    "EH_BILDU_PORCENTAJE", 
    round((col("EH_BILDU_SUM") / col("VOTOS_CANDIDATOS_SUM")) * 100, 2)
).withColumn(
    "EAJ_PNV_PORCENTAJE", 
    round((col("EAJ_PNV_UNIFICADO_SUM") / col("VOTOS_CANDIDATOS_SUM")) * 100, 2)
)

# Calcular la diferencia de porcentajes (valores absolutos)
grouped = grouped.withColumn(
    "DIFERENCIA_PORCENTAJES", 
    round(F_abs(col("EAJ_PNV_PORCENTAJE") - col("EH_BILDU_PORCENTAJE")), 2)
)

# Identificar el partido ganador (el que tiene el mayor porcentaje)
grouped = grouped.withColumn(
    "PARTIDO_GANADOR", 
    when(col("EH_BILDU_PORCENTAJE") > col("EAJ_PNV_PORCENTAJE"), "EH_BILDU")
    .when(col("EH_BILDU_PORCENTAJE") < col("EAJ_PNV_PORCENTAJE"), "EAJ_PNV")
    .otherwise("EMPATE")  # En caso de empate, asignar un ganador predeterminado
)

# Formatear los porcentajes y la diferencia para tener coma como separador decimal
grouped = grouped.withColumn(
    "EH_BILDU_PORCENTAJE", 
    regexp_replace(format_number(col("EH_BILDU_PORCENTAJE"), 2), "\\.", ",")
).withColumn(
    "EAJ_PNV_PORCENTAJE", 
    regexp_replace(format_number(col("EAJ_PNV_PORCENTAJE"), 2), "\\.", ",")
).withColumn(
    "DIFERENCIA_PORCENTAJES", 
    regexp_replace(format_number(col("DIFERENCIA_PORCENTAJES"), 2), "\\.", ",")
)

# Eliminar filas con valores NULL en las columnas relevantes
grouped_cleaned = grouped.na.drop(subset=["EH_BILDU_PORCENTAJE", "EAJ_PNV_PORCENTAJE", "DIFERENCIA_PORCENTAJES"])

# Ordenar los resultados de menor a mayor según la columna "DIFERENCIA_PORCENTAJES"
grouped_sorted = grouped_cleaned.orderBy("DIFERENCIA_PORCENTAJES", ascending=True)

# Mostrar los resultados filtrados
df_final=grouped_sorted.select("AMBITO", "FECHA", "EH_BILDU_PORCENTAJE", "EAJ_PNV_PORCENTAJE","DIFERENCIA_PORCENTAJES", "PARTIDO_GANADOR")

# Guardar los resultados en HDFS
df_final.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Mostrar una muestra de los datos procesados
df_final.show(truncate=False)

# Finalizar sesión
spark.stop()