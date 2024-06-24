from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

file_2021 = 'hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv'
file_2022 = 'hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv'

# Leer los archivos CSV especificando el delimitador
df_2021 = spark.read.option("delimiter", ";").option("header", "true").csv(file_2021)
df_2022 = spark.read.option("delimiter", ";").option("header", "true").csv(file_2022)

# Mostrar las columnas de cada archivo
print("Columnas del archivo 2021:")
df_2021.printSchema()
print("\nColumnas del archivo 2022:")
df_2022.printSchema()

# Unir los datos de 2021 y 2022
df_union = df_2021.unionByName(df_2022)

# Eliminar columnas que no se utilizarán para el análisis
columns_to_drop = ["Calidad dato"]
df_union = df_union.drop(*columns_to_drop)

# Filtrar los vuelos domésticos 'Clasificación Vuelo')
df_domestic = df_union.filter(col('Clasificación Vuelo') == 'Doméstico')

# Convertir valores NULL en 0 en las columnas 'Pasajeros' y 'distancia_ref'
df_domestic = df_domestic.withColumn('Pasajeros', when(col('Pasajeros').isNull(), 0).otherwise(col('Pasajeros').cast("int")))

# Mostrar tipos de datos de las columnas
print("Tipos de datos de las columnas:")
df_domestic.printSchema()

# Mostrar los primeros 5 registros del DataFrame
print("Primeros 5 registros del DataFrame transformado:")
df_domestic.show(5)

# Renombrar columnas para que coincidan con la estructura del schema destino
df_domestic = df_domestic.withColumnRenamed('Fecha', 'fecha') \
                         .withColumnRenamed('Hora UTC', 'horaUTC') \
                         .withColumnRenamed('Clase de Vuelo (todos los vuelos)', 'clase_de_vuelo') \
                         .withColumnRenamed('Clasificación Vuelo', 'clasificacion_de_vuelo') \
                         .withColumnRenamed('Tipo de Movimiento', 'tipo_de_movimiento') \
                         .withColumnRenamed('Aeropuerto', 'aeropuerto') \
                         .withColumnRenamed('Origen / Destino', 'origen_destino') \
                         .withColumnRenamed('Aerolinea Nombre', 'aerolinea_nombre') \
                         .withColumnRenamed('Aeronave', 'aeronave') \
                         .withColumnRenamed('Pasajeros', 'pasajeros')

# Convertir la columna 'fecha' en un tipo de dato date
df_domestic = df_domestic.withColumn('fecha', to_date(col('fecha'), 'dd/MM/yyyy'))

# Seleccionar y castear las columnas necesarias
df_domestic = df_domestic.select(
    col('fecha'),
    col('horaUTC').cast("string"),
    col('clase_de_vuelo').cast("string"),
    col('clasificacion_de_vuelo').cast("string"),
    col('tipo_de_movimiento').cast("string"),
    col('aeropuerto').cast("string"),
    col('origen_destino').cast("string"),
    col('aerolinea_nombre').cast("string"),
    col('aeronave').cast("string"),
    col('pasajeros').cast("int")
)

# Mostrar tipos de datos de las columnas
df_domestic.printSchema()

# Mostrar los primeros 5 registros del DataFrame transformado
df_domestic.show(5)

# Crear una vista temporal
df_domestic.createOrReplaceTempView("tmp_flights_data")

# Insertamos datos. 
hc.sql("insert into fligthsdb.vuelos select * from tmp_flights_data")

