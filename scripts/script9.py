from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, coalesce, regexp_replace, format_number
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

APP_NAME = "script9"

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

# Filtrar los datos solo para el año 2024
df_2024 = df.filter(col('FECHA') == 2024)

# Identificar columnas con solo valores null en el DataFrame filtrado
columns_to_drop = [column for column in df_2024.columns if df_2024.select(column).distinct().count() == 1 and df_2024.select(column).first()[0] is None]

# Eliminar estas columnas
df_2024 = df_2024.drop(*columns_to_drop)

# Definir las columnas que no corresponden a partidos
no_partidos = ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA", "EAJ_PNV", "EAJ_PNV___EA"]

# Obtener todas las columnas que podrían ser de partidos
partidos = [col_name for col_name in df_2024.columns if col_name not in no_partidos]

# Verificar que todas las columnas sean numéricas
for partido in partidos:
    df_2024 = df_2024.withColumn(partido, col(partido).cast("double"))

# Calculamos la desviación estándar de cada partido para cada municipio (AMBITO)
desviaciones = {}
for partido in partidos:
    desviaciones[partido] = stddev(partido).over(Window.partitionBy('AMBITO'))

# Agregamos las desviaciones estándar de cada partido al DataFrame
df_2024_con_desviaciones = df_2024.select(
    'AMBITO',
    *[desviaciones[partido].alias(f'desviacion_{partido}') for partido in partidos]
)

# Redondear a 2 decimales y reemplazar puntos por comas
for partido in partidos:
    columna_desviacion = f"desviacion_{partido}"
    df_2024_con_desviaciones = df_2024_con_desviaciones.withColumn(
        columna_desviacion,
        regexp_replace(format_number(col(columna_desviacion), 2), r'\.', ',')
    )

# Guardar los resultados en HDFS
df_2024_con_desviaciones.write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(output_dir)

# Mostrar una muestra de los datos procesados
df_2024_con_desviaciones.show(20,truncate=False)

# Finalizar sesión
spark.stop()
