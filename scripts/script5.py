from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, struct, expr, countDistinct, collect_set
from dotenv import load_dotenv
import os

APP_NAME = "script5"

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

# Asegurar que las columnas relevantes sean numéricas
partidos_cols = [col_name for col_name in df.columns if col_name not in ["TH", "CODMUN", "AMBITO", "DISTRITO", "SECCION", "MESA", "CENSO", "VOTOS", "NULOS", "VALIDOS", "BLANCOS", "VOTOS_CANDIDATOS", "ABSTENCION", "FECHA"]]
df = df.select("AMBITO", "FECHA", *partidos_cols).na.fill(0)
for partido in partidos_cols:
    df = df.withColumn(partido, col(partido).cast("integer"))

# Crear una columna estructurada con los partidos y sus votos
df_partidos = df.withColumn(
    "PARTIDOS_VOTOS",
    array(*[struct(lit(partido).alias("PARTIDO"), col(partido).alias("VOTOS")) for partido in partidos_cols])
)

# Determinar el partido ganador para cada municipio y elección
df_winner = df_partidos.withColumn(
    "GANADOR",
    expr("filter(PARTIDOS_VOTOS, x -> x.VOTOS = array_max(transform(PARTIDOS_VOTOS, y -> y.VOTOS)))")[0]["PARTIDO"]
).select("AMBITO", "FECHA", "GANADOR")

# Agrupar por municipio para verificar la constancia del ganador
df_constancia = df_winner.groupBy("AMBITO").agg(
    countDistinct("GANADOR").alias("PARTIDOS_DISTINTOS"),
    collect_set("GANADOR").alias("GANADORES")
)

# Clasificar municipios según su constancia
df_constancia = df_constancia.withColumn(
    "CATEGORIA",
    when(col("PARTIDOS_DISTINTOS") == 1, "Siempre constante")
    .when(col("PARTIDOS_DISTINTOS") <= 2, "Mayormente constante")
    .otherwise("Altamente variable")
)

# Mostrar el resultado
df_constancia.show(truncate=False)

# Guardar el resultado en HDFS
df_constancia.write.mode("overwrite").option("header", "true").csv(output_dir)

# Finalizar sesión
spark.stop()