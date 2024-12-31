from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import col, expr, sum as spark_sum, when, greatest
import os
# ADD MORE IF NEEDED

APP_NAME = "script7"

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


# Asegurarse de que las columnas de votos sean numéricas
partidos_columns = [
    "PP", "PSE_EE_PSOE", "EAJ_PNV", "EH_BILDU", "VOX", "CS", "PODEMOS_AHAL_DUGU_IU", "SUMAR"
]

for partido in partidos_columns:
    df = df.withColumn(partido, col(partido).cast("integer"))

# Filtrar los datos de 1994 y 2024
df_filtered = df.filter(col("FECHA").isin("1994", "2024"))

# Calcular el total de votos por partido, provincia (TH) y año (FECHA)
df_grouped = df_filtered.groupBy("TH", "FECHA").agg(
    *[spark_sum(partido).alias(f"TOTAL_{partido}") for partido in partidos_columns]
)

# Calcular el crecimiento porcentual de cada partido entre 1994 y 2024
growth_expressions = [
    expr(f"(MAX(TOTAL_{partido}) - MIN(TOTAL_{partido})) / MIN(TOTAL_{partido}) * 100").alias(f"GROWTH_{partido}")
    for partido in partidos_columns
]

df_growth = df_grouped.groupBy("TH").agg(*growth_expressions)

# Crear una lista de condiciones 'when' de manera dinámica
when_conditions = [
    when(col(f"GROWTH_{partido}") == greatest(*[col(f"GROWTH_{p}") for p in partidos_columns]), partido)
    for partido in partidos_columns
]

# Unir todas las condiciones con un 'otherwise' para el caso por defecto
df_max_growth = df_growth.withColumn(
    "MAX_PARTY",
    when_conditions[0].otherwise("UNKNOWN")
)

# Aplicar dinámicamente las condiciones de 'when' en orden
for condition in when_conditions[1:]:
    df_max_growth = df_max_growth.withColumn("MAX_PARTY", when(condition, condition.alias()).otherwise("UNKNOWN"))

# Seleccionamos las columnas deseadas
df_max_growth = df_max_growth.select("TH", "MAX_PARTY")

# Mostrar los resultados
df_max_growth.show(truncate=False)

# Guardar los resultados
df_max_growth.write.mode("overwrite").option("header", "true").csv(output_dir)