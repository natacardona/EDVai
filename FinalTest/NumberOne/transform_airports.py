import os
from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

file_details = 'hdfs://localhost:9000/ingest/aeropuertos_detalle.csv'

# Leer los archivos CSV especificando el delimitador
df_airport_details = spark.read.option("delimiter", ";").option("header", "true").csv(file_details)

# Mostrar las columnas de cada archivo
print("Columnas del archivo detalles:")
df_airport_details.printSchema()

df_airport = df_airport_details.drop('inhab', 'fir')
df_airports_fixed = df_airport.fillna({"distancia_ref": 0})

# Renombrar y normalizar nombres de columnas
df_airports_fixed = df_airports_fixed.withColumnRenamed("local", "aeropuerto")
df_airports_fixed = df_airports_fixed.withColumnRenamed("oaci", "oac")
df_airports_fixed = df_airports_fixed.withColumnRenamed("iata", "iata")

# Seleccionar y castear las columnas necesarias
df_airports_fixed = df_airports_fixed.select(
    col('aeropuerto').cast("string"), 
    col('oac').cast("string"),
    col('iata').cast("string"),
    col('tipo').cast("string"),
    col('denominacion').cast("string"),
    col('coordenadas').cast("string"),
    col('latitud').cast("string"),
    col('longitud').cast("string"),
    col('elev').cast("float"),
    col('uom_elev').cast("string"),
    col('ref').cast("string"),
    col('distancia_ref').cast("float"),
    col('direccion_ref').cast("string"),
    col('condicion').cast("string"),
    col('control').cast("string"),
    col('region').cast("string"),
    col('uso').cast("string"),
    col('trafico').cast("string"),
    col('sna').cast("string"),
    col('concesionado').cast("string"),
    col('provincia').cast("string")
)

# Mostrar tipos de datos de las columnas
df_airports_fixed.printSchema()

# Mostrar los primeros 5 registros del DataFrame transformado
df_airports_fixed.show(5)

# Crear una vista temporal
df_airports_fixed.createOrReplaceTempView("tmp_airport_details_data")

# Insertamos datos:
hc.sql("insert into fligthsdb.detalle_aeropuertos select * from tmp_airport_details_data;")
