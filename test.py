from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("TestVerDatos2024") \
    .master("yarn") \
    .getOrCreate()

# Ruta en HDFS donde están los datos procesados
hdfs_dir = "hdfs:///datos/elecciones/"

# Leer los datos desde HDFS
print(f"Leyendo datos desde HDFS: {hdfs_dir}")
df = spark.read.option("header", "true") \
    .option("delimiter", ";") \
    .option("encoding", "latin1") \
    .csv(hdfs_dir)

# Mostrar algunos datos
print("Mostrando datos:")
df.show(truncate=False)

# Finalizar la sesión
spark.stop()
