from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
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

# Definir las columnas que no son partidos
no_partidos = ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", 
               "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA"]

# Identificar las columnas de partidos
partidos = [col_name for col_name in df.columns if col_name not in no_partidos]

# Convertir las columnas de partidos a numérico
for partido in partidos:
    df = df.withColumn(partido, col(partido).cast("double"))

# Agrupar los datos por AMBITO y MESA, luego sumar los votos de cada partido
df_grouped = df.groupBy("AMBITO", "MESA").agg(
    *[spark_sum(col(partido)).alias(f"{partido}_TOTAL") for partido in partidos]
)

# Luego agrupar por AMBITO para sumar los totales por partido
df_grouped_final = df_grouped.groupBy("AMBITO").agg(
    *[spark_sum(col(f"{partido}_TOTAL")).alias(f"{partido}_TOTAL") for partido in partidos]
)

# Mostrar las sumas de votos por partido por AMBITO
df_grouped_final.show(truncate=False)




import os

# Ruta de salida
output_dir = "/home/ec2-user/BigDataProyectoFinal/resultados_partidos"
final_file_path = "/home/ec2-user/BigDataProyectoFinal/resultados_partidos.csv"

# Verificar si el directorio existe, si no, crearlo
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
