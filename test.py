from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisElecciones") \
    .master("yarn") \
    .getOrCreate()

# Leer los archivos CSV desde HDFS
df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs:///datos/elecciones/*.csv")

print("Datos cargados:")
df.show()

# Mostrar el esquema de los datos
print("Esquema de los datos:")
df.printSchema()

# Ejemplo de análisis: contar registros
print("Número total de registros:")
print(df.count())

# Ejemplo de transformación
cols = str(df.columns).split(";")
if "EAJ-PNV" in cols:
    print("Conteo de votos por candidato:")
    df.groupBy("EAJ-PNV").count().show()

# Finalizar sesión de Spark
spark.stop()