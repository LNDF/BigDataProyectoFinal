import pandas as pd
import os
import re
import subprocess

# Directorio local donde están los CSV
local_dir = "/home/ec2-user/BigDataProyectoFinal/data/"

# Archivo temporal para guardar el CSV combinado
temp_combined_file = "/home/ec2-user/BigDataProyectoFinal/temp/dataset.csv"

# Directorio en HDFS donde se subirá el CSV combinado
hdfs_dir = "hdfs:///datos/elecciones/"
hdfs_file_path = f"{hdfs_dir}dataset.csv"

# Crear una lista para almacenar los DataFrames procesados
dataframes = []

# Función para limpiar los nombres de las columnas
def limpiar_nombres_columnas(df):
    df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col) for col in df.columns]
    return df

# Función para extraer el año del nombre del archivo
def extraer_anio(nombre_archivo):
    match = re.search(r'MesP(\d{2})', nombre_archivo)
    if match:
        return f"20{match.group(1)}" if int(match.group(1)) <= 50 else f"19{match.group(1)}"
    return None

def uniformizar_provincia(th):
    if isinstance(th, str):
        th = th.strip().upper()
        th = re.sub(r"[?¿ï¿½]", "", th)
        if "GIPUZKOA" in th:
            return "GIPUZKOA"
        elif "BIZKAIA" in th:
            return "BIZKAIA"
        elif "ARABA" in th or "ÁLAVA" in th or "ALAVA" in th or "LAVA" in th:
            return "ARABA-ÁLAVA"
    return th


# Procesar cada archivo CSV en el directorio local
for file_name in os.listdir(local_dir):
    if file_name.endswith(".csv"):
        file_path = os.path.join(local_dir, file_name)
        print(f"Procesando archivo: {file_name}")
        
        # Leer el archivo CSV con pandas
        df = pd.read_csv(file_path, delimiter=";", encoding="latin1")
        
        # Limpiar los nombres de las columnas
        df = limpiar_nombres_columnas(df)
        
        # Reemplazar valores "NULL" explícitos con 0
        df.replace("NULL", 0, inplace=True)
        df.fillna(0, inplace=True)
        
        # Extraer el año del nombre del archivo y añadir la columna "FECHA"
        year = extraer_anio(file_name)
        if year:
            df["FECHA"] = year
        else:
            print(f"Advertencia: No se pudo extraer el año del archivo {file_name}.")
            continue
        
        # Uniformizar los nombres de las provincias (TH)
        if "TH" in df.columns:
            df["TH"] = df["TH"].apply(uniformizar_provincia)
        
        # Añadir el DataFrame a la lista
        dataframes.append(df)

# Combinar todos los DataFrames en uno solo
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Guardar el archivo combinado temporalmente
    try:
        os.makedirs("/home/ec2-user/BigDataProyectoFinal/temp")
    except FileExistsError:
        pass

    combined_df.to_csv(temp_combined_file, index=False, sep=";", encoding="latin1")
    print(f"Archivo combinado guardado temporalmente en: {temp_combined_file}")

    # Crear el directorio en HDFS si no existe
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    
    # Subir el archivo combinado a HDFS
    subprocess.run(["hdfs", "dfs", "-put", "-f", temp_combined_file, hdfs_file_path])
    subprocess.run(["rm", "-rf", "/home/ec2-user/BigDataProyectoFinal/temp"])
    print(f"Archivo combinado subido a HDFS: {hdfs_file_path}")
else:
    print("No se encontraron archivos válidos para combinar.")
