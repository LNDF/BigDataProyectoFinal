import pandas as pd
import os
import re
import subprocess

# Directorio local donde están los CSV
local_dir = "/home/ec2-user/BigDataProyectoFinal/data/"

# Directorio temporal para guardar los CSV procesados
temp_dir = "/home/ec2-user/BigDataProyectoFinal/temp/"

# Directorio en HDFS donde se subirán los CSV procesados
hdfs_dir = "hdfs:///datos/elecciones/"

# Crear el directorio temporal si no existe
os.makedirs(temp_dir, exist_ok=True)

# Función para extraer el año del nombre del archivo
def extraer_anio(nombre_archivo):
    match = re.search(r'MesP(\d{2})', nombre_archivo)
    if match:
        return f"20{match.group(1)}" if int(match.group(1)) <= 50 else f"19{match.group(1)}"
    return None

# Procesar cada archivo CSV en el directorio local
for file_name in os.listdir(local_dir):
    if file_name.endswith(".csv"):
        file_path = os.path.join(local_dir, file_name)
        print(f"Procesando archivo: {file_name}")
        
        # Leer el archivo CSV con pandas
        df = pd.read_csv(file_path, delimiter=";", encoding="latin1")
        
        # Reemplazar valores "NULL" explícitos con 0
        df.fillna(0, inplace=True)
        
        # Extraer el año del nombre del archivo y añadir la columna "FECHA"
        year = extraer_anio(file_name)
        if year:
            df["FECHA"] = year
        else:
            print(f"Advertencia: No se pudo extraer el año del archivo {file_name}.")
            continue
        
        # Guardar el archivo procesado en el directorio temporal
        processed_file_path = os.path.join(temp_dir, file_name)
        df.to_csv(processed_file_path, index=False, sep=";", encoding="latin1")
        print(f"Archivo procesado guardado temporalmente en: {processed_file_path}")

# Subir los archivos procesados a HDFS
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
for file_name in os.listdir(temp_dir):
    file_path = os.path.join(temp_dir, file_name)
    print(f"Subiendo archivo a HDFS: {file_name}")
    subprocess.run(["hdfs", "dfs", "-put", "-f", file_path, hdfs_dir])

# Limpiar el directorio temporal
for file_name in os.listdir(temp_dir):
    os.remove(os.path.join(temp_dir, file_name))

print("Todos los archivos procesados y subidos a HDFS.")
