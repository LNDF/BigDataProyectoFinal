from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, stddev, format_number, regexp_replace
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

# Unificar las columnas de EAJ_PNV y EAJ_PNV___EA
df = df.withColumn("EAJ_PNV_UNIFICADO", coalesce(col("EAJ_PNV"), col("EAJ_PNV___EA")))

# Definir las columnas que no corresponden a partidos
no_partidos = ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA", "EAJ_PNV", "EAJ_PNV___EA"]

# Obtener todas las columnas que podrían ser de partidos
partidos = [col_name for col_name in df.columns if col_name not in no_partidos]

# Verificar que todas las columnas sean numéricas
for partido in partidos:
    df = df.withColumn(partido, col(partido).cast("integer"))

# Calcular la desviación estándar por municipio y fecha
result = (
    df.groupBy("AMBITO", "FECHA")
    .agg(
        *[
            format_number(stddev(col(partido)), 2).alias(f"STD_{partido}")
            for partido in partidos  # Calcular y formatear a 2 decimales
        ],
    )
)

# Reemplazar puntos por comas en los resultados
result = result.select(
    *[
        regexp_replace(col(c).cast("string"), r'\.', ',').alias(c)
        if c.startswith("STD_") else col(c)
        for c in result.columns
    ]
)

# Guardar los resultados en HDFS
result.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Mostrar una muestra de los datos procesados
result.show(truncate=False)

# Finalizar sesión
spark.stop()

result.columns


