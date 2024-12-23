from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, lit, struct, expr, lag, lower, trim, sum as spark_sum
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

APP_NAME = "script2"

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

# Seleccionar columnas relevantes (municipio, año y partidos)
partidos_invalidos = ["ABSTENCIONES", "CODMUN", "VOTOS", "CENSO", "VALIDOS", "VOTANTES", "NULOS", "VOTOS_CANDIDATOS", "ABSTENCION", "NULL"]
partidos = [col_name for col_name in df.columns if col_name not in ["AMBITO", "FECHA", "TH"] + partidos_invalidos]

# Normalizar nombres de municipios
df = df.withColumn("AMBITO", trim(col("AMBITO")))

# Convertir las columnas de partidos a un array de structs (partido, votos)
partidos_structs = array(*[struct(lit(party).alias("PARTIDO"), col(party).cast("integer").alias("VOTOS")) for party in partidos]).alias("PARTIDOS")

df = df.select("AMBITO", "FECHA", partidos_structs)

# Explode para obtener filas con partido y votos por municipio y año
df_exploded = df.select("AMBITO", "FECHA", expr("inline(PARTIDOS)").alias("PARTIDO", "VOTOS"))

# Sumar los votos por partido para cada municipio y año
df_aggregated = df_exploded.groupBy("AMBITO", "FECHA", "PARTIDO").agg(spark_sum("VOTOS").alias("TOTAL_VOTOS"))

# Obtener el partido ganador por municipio y año
ganadores = df_aggregated.groupBy("AMBITO", "FECHA").agg(expr("max_by(PARTIDO, TOTAL_VOTOS)").alias("PARTIDO_GANADOR"), expr("max(TOTAL_VOTOS)").alias("VOTOS_GANADORES"))

# Identificar cambios de partido entre 1994 y 2024
window_spec = Window.partitionBy("AMBITO").orderBy("FECHA")
ganadores = ganadores.withColumn("PARTIDO_ANTERIOR", lag("PARTIDO_GANADOR").over(window_spec))
ganadores = ganadores.withColumn("CAMBIO_PARTIDO", expr("PARTIDO_GANADOR != PARTIDO_ANTERIOR"))

# Almacenar los datos procesados en HDFS
ganadores.write.mode("overwrite").option("header", "true").csv(output_dir)

# Mostrar una muestra de los datos procesados
ganadores.show(truncate=False)

# Finalizar sesión
spark.stop()