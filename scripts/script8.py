from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs, round, coalesce, lag, format_string, regexp_replace
from pyspark.sql.window import Window
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
       .withColumn("VOTOS", col("VOTOS").cast("integer"))

# Unificar las columnas de EAJ_PNV y EAJ_PNV___EA
df = df.withColumn("EAJ_PNV_UNIFICADO", coalesce(col("EAJ_PNV"), col("EAJ_PNV___EA")))

# Calcular el porcentaje de votos por partido
df = df.withColumn("PORCENTAJE_EH_BILDU", round((col("EH_BILDU") / col("VOTOS")) * 100, 2)) \
       .withColumn("PORCENTAJE_EAJ_PNV", round((col("EAJ_PNV_UNIFICADO") / col("VOTOS")) * 100, 2))

# Filtrar solo datos relevantes para EH Bildu (desde 2012) y EAJ-PNV
df_filtered = df.filter((col("FECHA") >= 2012) & (col("EH_BILDU").isNotNull()) & (col("EAJ_PNV_UNIFICADO").isNotNull()))

# Calcular la diferencia de porcentajes entre ambos partidos
df_filtered = df_filtered.withColumn(
    "DIFERENCIA",
    abs(col("PORCENTAJE_EH_BILDU") - col("PORCENTAJE_EAJ_PNV"))
)

# Identificar el partido ganador por municipio y año
df_filtered = df_filtered.withColumn(
    "PARTIDO_GANADOR",
    when(col("PORCENTAJE_EH_BILDU") > col("PORCENTAJE_EAJ_PNV"), "EH_BILDU")
    .otherwise("EAJ_PNV")
)

# Ventana para calcular cambios entre años por municipio
window_spec = Window.partitionBy("AMBITO").orderBy("FECHA")
df_filtered = df_filtered.withColumn("DIFERENCIA_ANTERIOR", lag("DIFERENCIA").over(window_spec))

# Calcular el cambio en la diferencia respecto al año anterior
df_filtered = df_filtered.withColumn(
    "CAMBIO_DIFERENCIA",
    when(col("DIFERENCIA_ANTERIOR").isNotNull(), col("DIFERENCIA") - col("DIFERENCIA_ANTERIOR"))
    .otherwise(None)
)

# Filtrar datos para el año 2024 y mostrar la comparación con inicios de EH Bildu (2012)
df_final = df_filtered.filter(col("FECHA") == 2024).select(
    "AMBITO",
    "FECHA",
    "PORCENTAJE_EH_BILDU",
    "PORCENTAJE_EAJ_PNV",
    "DIFERENCIA",
    "PARTIDO_GANADOR",
    "CAMBIO_DIFERENCIA"
)

# Limitar decimales y convertir números a formato con coma
df_final = df_final.withColumn(
    "PORCENTAJE_EH_BILDU", format_string("%.2f", col("PORCENTAJE_EH_BILDU"))
).withColumn(
    "PORCENTAJE_EAJ_PNV", format_string("%.2f", col("PORCENTAJE_EAJ_PNV"))
).withColumn(
    "DIFERENCIA", format_string("%.2f", col("DIFERENCIA"))
).withColumn(
    "CAMBIO_DIFERENCIA", format_string("%.2f", col("CAMBIO_DIFERENCIA"))
)

# Reemplazar puntos por comas
for column in ["PORCENTAJE_EH_BILDU", "PORCENTAJE_EAJ_PNV", "DIFERENCIA", "CAMBIO_DIFERENCIA"]:
    df_final = df_final.withColumn(column, regexp_replace(col(column), r'\.', ','))

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