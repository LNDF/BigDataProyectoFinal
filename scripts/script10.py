from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, coalesce
from functools import reduce
from dotenv import load_dotenv
import os

APP_NAME = "script10"

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

# Definir el umbral de votos significativos (5%)
umbral = 0.05

# Unificar las columnas de EAJ_PNV y EAJ_PNV___EA
df = df.withColumn("EAJ_PNV_UNIFICADO", coalesce(col("EAJ_PNV"), col("EAJ_PNV___EA")))

# Definir las columnas que no corresponden a partidos
no_partidos = ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA", "EAJ_PNV", "EAJ_PNV___EA"]

# Obtener todas las columnas que podrían ser de partidos
partidos = [col_name for col_name in df.columns if col_name not in no_partidos]

# Verificar que todas las columnas sean numéricas 
for partido in partidos:
    df = df.withColumn(partido, col(partido).cast("double"))

# Añadir columna para cada partido si tiene representación significativa (mayor al 5% de los votos)
for partido in partidos:
    df = df.withColumn(f"{partido}_significativo", when(col(partido) > umbral, 1).otherwise(0))

# Calcular la diversidad (número de partidos significativos) por municipio y año
df_diversidad = df.withColumn(
    "diversidad_partidos", 
    reduce(lambda a, b: a + b, [col(f"{partido}_significativo") for partido in partidos])
).groupBy('AMBITO', 'FECHA').agg(
    sum('diversidad_partidos').alias('diversidad_partidos')
)

# Filtrar los datos para los años 1994 y 2024
df_1994_2024 = df_diversidad.filter((col('FECHA') == 1994) | (col('FECHA') == 2024))

# Pivotar para comparar la diversidad entre 1994 y 2024
df_diversidad_pivot = df_1994_2024.groupBy('AMBITO').pivot('FECHA').agg(sum('diversidad_partidos'))

# Calcular el incremento en la diversidad de partidos entre 2024 y 1994
df_diversidad_pivot = df_diversidad_pivot.withColumn(
    "incremento_diversidad", 
    col('2024') - col('1994')
)

# Ordenar por el mayor incremento en la diversidad
df_diversidad_pivot = df_diversidad_pivot.orderBy(col('incremento_diversidad'), ascending=False)

# Mostrar los resultados
df_diversidad_pivot.show(truncate=False)

# Guardar los resultados en HDFS
df_diversidad_pivot.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Finalizar sesión
spark.stop()
