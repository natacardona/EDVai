from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("CSV to DataFrame") \
    .getOrCreate()

file_2021 = f'hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv'
file_2022 = f'hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv'
file_details = f'hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv'

# Leer los archivos CSV especificando el delimitador
df_2021 = spark.read.option("delimiter", ";").option("header", "true").csv(file_2021)
df_2022 = spark.read.option("delimiter", ";").option("header", "true").csv(file_2022)
df_details = spark.read.option("delimiter", ";").option("header", "true").csv(file_details)

# Mostrar las columnas de cada archivo
print("Columnas del archivo 2021:")
df_2021.printSchema()
print("\nColumnas del archivo 2022:")
df_2022.printSchema()
print("\nColumnas del archivo de detalles:")
df_details.printSchema()

# Unir los datos de 2021 y 2022
df_union = df_2021.unionByName(df_2022)

# Eliminar columnas que no se utilizarán para el análisis
columns_to_drop = ["inhab", "fir", "Calidad dato"]
df_union = df_union.drop(*columns_to_drop)

# Filtrar los vuelos internacionales (asumiendo que hay una columna 'Tipo de Movimiento')
df_domestic = df_union.filter(col('Clasificación Vuelo') == 'Doméstico')

# Convertir valores NULL en 0 en las columnas 'Pasajeros' y 'distancia_ref'
df_domestic = df_domestic.withColumn('Pasajeros', when(col('Pasajeros').isNull(), 0).otherwise(col('Pasajeros').cast("int")))
df_details = df_details.withColumn('distancia_ref', when(col('distancia_ref').isNull(), 0).otherwise(col('distancia_ref').cast("int")))

# Mostrar tipos de datos de las columnas
print("Tipos de datos de las columnas:")
df_domestic.printSchema()

# Mostrar los primeros 5 registros del DataFrame
print("Primeros 5 registros del DataFrame transformado:")
df_domestic.show(5)

# Detener la SparkSession
spark.stop()
