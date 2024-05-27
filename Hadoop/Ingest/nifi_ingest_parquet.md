## Practica nifi parquet:

## Ejercicio Clase 5

# 1. En el container de Nifi, crear un .sh que permita descargar el archivo yellow_tripdata_2021-01.parquet desde 
wget -O /home/fpineyro/test/yellow_tripdata_2021-01.parquet 
https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01 .parquet y lo guarde en /home/nifi/ingest. 
Ejecutarlo 

Ingresamos al container con el siguiente commando
```
docker exec -it nifi bash 
```
Una vez estamos adentro nos cambiamos al directorio correcto para crear el script
```
nifi@98902409275d:/opt/nifi/nifi-current$ ls
bin   content_repository   docs        flowfile_repository  LICENSE  NOTICE                 README  state
conf  database_repository  extensions  lib                  logs     provenance_repository  run     work
nifi@98902409275d:/opt/nifi/nifi-current$ pwd
/opt/nifi/nifi-current
nifi@98902409275d:/opt/nifi/nifi-current$ cd /home/nifi/ingest
nifi@98902409275d:~/ingest$ pwd
/home/nifi/ingest
nifi@98902409275d:~/ingest$ cat > nifi_ingest_parquet.sh
#!/bin/bash

# Download the file to the local landing directory
wget -O /home/nifi/ingest/yellow_tripdata_2021-01.parquet https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

# Check if the download was successful
if [[ -f "/home/nifi/ingest/yellow_tripdata_2021-01.parquet" ]]; then
    echo "Download successful."
else
    echo "Download failed."
fi
```
luego le damos permisos para poder ejecutarlo:

nifi@98902409275d:~/ingest$ chmod +x nifi_ingest_parquet.sh

```
lo ejecutamos:

nifi@98902409275d:~/ingest$ ./nifi_ingest_parquet.sh
--2024-05-23 20:50:05--  https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet
Resolving dataengineerpublic.blob.core.windows.net (dataengineerpublic.blob.core.windows.net)... 20.150.25.164
Connecting to dataengineerpublic.blob.core.windows.net (dataengineerpublic.blob.core.windows.net)|20.150.25.164|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 21686067 (21M) [application/octet-stream]
Saving to: ‘/home/nifi/ingest/yellow_tripdata_2021-01.parquet’

/home/nifi/ingest/yellow_trip 100%[==============================================>]  20.68M   320KB/s    in 45s     

2024-05-23 20:50:51 (474 KB/s) - ‘/home/nifi/ingest/yellow_tripdata_2021-01.parquet’ saved [21686067/21686067]

Download successful.
nifi@98902409275d:~/ingest$ 
```
Validamos la extracción exitosa del archivo mediante el script shell
```
nifi@98902409275d:~/ingest$ ls
nifi_ingest_parquet.sh  nifi_ingest.sh  starwars.csv  yellow_tripdata_2021-01.parquet
nifi@98902409275d:~/ingest$ pwd
/home/nifi/ingest
nifi@98902409275d:~/ingest$ 

```
# 2. Por medio de la interfaz gráfica de Nifi, crear un job que tenga dos procesos. 
a) GetFile para obtener el archivo del punto 1 (/home/nifi/ingest) 
b) putHDFS para ingestarlo a HDFS (directorio nifi) 

![Nifi ETL parquet](https://github.com/natacardona/EDVai/blob/main/Hadoop/Ingest/Nifi_ETL_parquet.png)

Despues de correr el job en nifi validamos que el archivo este en la ruta correctamente.
```
hadoop@d4437f61daec:/$ hdfs dfs -ls /ingest
Found 1 items
-rw-r--r--   1 hadoop supergroup  125981363 2022-05-09 17:58 /ingest/yellow_tripdata_2021-01.csv
```
```
hadoop@d4437f61daec:/$ hdfs dfs -ls /nifi
Found 2 items
-rw-r--r--   1 nifi supergroup        307 2024-05-22 20:52 /nifi/nifi_ingest.sh
-rw-r--r--   1 nifi supergroup       5462 2024-05-22 21:04 /nifi/starwars.csv
hadoop@d4437f61daec:/$ hdfs dfs -ls /nifi
Found 3 items
-rw-r--r--   1 nifi supergroup        307 2024-05-22 20:52 /nifi/nifi_ingest.sh
-rw-r--r--   1 nifi supergroup       5462 2024-05-22 21:04 /nifi/starwars.csv
-rw-r--r--   1 nifi supergroup   21686067 2024-05-23 18:22 /nifi/yellow_tripdata_2021-01.parquet
hadoop@d4437f61daec:/$ 
```

# 3. Con el archivo ya ingestado en HDFS/nifi, escribir las consultas y agregar captura de pantalla del resultado. Para los ejercicios puedes usar SQL mediante la creación de una vista llamada yellow_tripdata. 
También debes chequear el diccionario de datos por cualquier duda que tengas respecto a las columnas del archivo 
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf 

Ingresamos a pyspark
```
pyspark
```
creamos el dataframe usando como input el archivo parquet

```
>>> df = spark.read.parquet("/nifi/yellow_tripdata_2021-01.parquet")
>>> df.printSchema()
root
 |-- VendorID: long (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: double (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: long (nullable = true)
 |-- DOLocationID: long (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)

>>> df.show()
```
creamos la vista temporal
```
df.createOrReplaceTempView("yellow_tripdata")
```
## 3.1) Mostrar los resultados siguientes 
a. VendorId Integer 
b. Tpep_pickup_datetime date 
c. Total_amount double 
d. Donde el total (total_amount sea menor a 10 dólares)

```
# Execute the SQL query
>>> results = spark.sql("""
... SELECT
...     VendorID AS VendorId,
...     tpep_pickup_datetime AS Tpep_pickup_datetime,
...     total_amount AS Total_amount
... FROM
...     yellow_tripdata
... WHERE
...     total_amount < 10.0
... """)
>>> 
>>> # Display the results
>>> results.show()
+--------+--------------------+------------+                                    
|VendorId|Tpep_pickup_datetime|Total_amount|
+--------+--------------------+------------+
|       1| 2020-12-31 21:51:20|         4.3|
|       2| 2020-12-31 21:42:11|         8.3|
|       2| 2020-12-31 21:04:21|        9.96|
|       2| 2020-12-31 21:43:41|         9.3|
|       2| 2020-12-31 21:36:08|         5.8|
|       1| 2020-12-31 21:03:13|         0.0|
|       1| 2020-12-31 21:30:32|         9.3|
|       2| 2020-12-31 21:16:19|         9.8|
|       2| 2020-12-31 21:57:26|         8.8|
|       2| 2020-12-31 21:33:33|        9.96|
|       2| 2020-12-31 21:39:08|         9.3|
|       2| 2020-12-31 21:45:29|         7.8|
|       1| 2020-12-31 21:43:00|        9.55|
|       2| 2020-12-31 21:52:28|         4.8|
|       1| 2020-12-31 21:39:50|         9.8|
|       2| 2020-12-31 21:17:39|         8.8|
|       1| 2020-12-31 21:49:10|         7.8|
|       2| 2020-12-31 21:47:51|        9.36|
|       2| 2020-12-31 21:11:53|         8.3|
|       1| 2020-12-31 21:34:38|         9.3|
+--------+--------------------+------------+
only showing top 20 rows

>>> 
```
## 3.2) Mostrar los 10 días que más se recaudó dinero(tpep_pickup_datetime, total amount)

```
# Execute the SQL query to find the top 10 revenue days
>>> top_revenue_days = spark.sql("""
... SELECT
...     DATE(tpep_pickup_datetime) AS pickup_day,
...     SUM(total_amount) AS daily_total
... FROM
...     yellow_tripdata
... GROUP BY
...     DATE(tpep_pickup_datetime)
... ORDER BY
...     daily_total DESC
... LIMIT 10
... """)
>>> 
>>> # Display the results
>>> top_revenue_days.show()
+----------+-----------------+                                                  
|pickup_day|      daily_total|
+----------+-----------------+
|2021-01-28|961322.5600002451|
|2021-01-22|942205.9300002148|
|2021-01-29|937373.5100002222|
|2021-01-21|932444.4500002082|
|2021-01-15|931628.1900002063|
|2021-01-14|926664.0400001821|
|2021-01-27|  895259.87000017|
|2021-01-19|890581.4500001629|
|2021-01-07|887670.1600001527|
|2021-01-08| 878002.730000146|
+----------+-----------------+
```
## 3.3) Mostrar los 10 viajes que menos dinero recaudó en viajes mayores a 10 millas (trip_distance, total_amount)

```
>>> # Execute the SQL query to find the 10 least revenue-generating trips over 10 miles
>>> least_revenue_trips = spark.sql("""
... SELECT
...     trip_distance,
...     total_amount
... FROM
...     yellow_tripdata
... WHERE
...     trip_distance > 10.0
... ORDER BY
...     total_amount ASC
... LIMIT 10
... """)
>>> 
>>> # Display the results
>>> least_revenue_trips.show()
+-------------+------------+                                                    
|trip_distance|total_amount|
+-------------+------------+
|        12.68|      -252.3|
|        34.35|     -176.42|
|        14.75|      -152.8|
|        33.96|     -127.92|
|         29.1|      -119.3|
|        26.94|      -111.3|
|        20.08|      -107.8|
|        19.55|      -102.8|
|        19.16|      -90.55|
|        25.83|      -88.54|
+-------------+------------+
```
## 3.4) Mostrar los viajes de más de dos pasajeros que hayan pagado con tarjeta de crédito (mostrar solo las columnas trip_distance y tpep_pickup_datetime)
# Execute the SQL query to display only the date part

```
>>> # Execute the SQL query to display only the date part
>>> result = spark.sql("""
... SELECT
...     trip_distance,
...     DATE(tpep_pickup_datetime) AS pickup_date  -- Extracts only the date
... FROM
...     yellow_tripdata
... WHERE
...     passenger_count > 2 AND
...     payment_type = 1
... """)
>>> 
>>> # Show the results
>>> result.show()
+-------------+-----------+
|trip_distance|pickup_date|
+-------------+-----------+
|         6.11| 2020-12-31|
|          1.7| 2020-12-31|
|         3.15| 2020-12-31|
|        10.74| 2020-12-31|
|         2.01| 2020-12-31|
|         2.85| 2020-12-31|
|         1.68| 2020-12-31|
|         0.77| 2020-12-31|
|          0.4| 2020-12-31|
|        16.54| 2020-12-31|
|          2.3| 2020-12-31|
|          2.5| 2020-12-31|
|         1.29| 2020-12-31|
|          1.3| 2020-12-31|
|          0.8| 2020-12-31|
|         1.49| 2020-12-31|
|         1.62| 2020-12-31|
|         5.04| 2020-12-31|
|          2.5| 2020-12-31|
|         3.13| 2020-12-31|
+-------------+-----------+
only showing top 20 rows

>>> 
```

## 3.5) Mostrar los 7 viajes con mayor propina en distancias mayores a 10 millas (mostrar campos tpep_pickup_datetime, trip_distance, passenger_count, tip_amount)

```
>>> # Run the SQL query to find the top 7 trips with the highest tips over 10 miles
>>> top_tips_trips = spark.sql("""
... SELECT
...     tpep_pickup_datetime,
...     trip_distance,
...     passenger_count,
...     tip_amount
... FROM
...     yellow_tripdata
... WHERE
...     trip_distance > 10.0
... ORDER BY
...     tip_amount DESC
... LIMIT 7
... """)
>>> 
>>> # Display the results
>>> top_tips_trips.show()
[Stage 8:>                                            [Stage 8:=============================>                                                                     +--------------------+-------------+---------------+----------+
|tpep_pickup_datetime|trip_distance|passenger_count|tip_amount|
+--------------------+-------------+---------------+----------+
| 2021-01-20 08:22:05|        427.7|            1.0|   1140.44|
| 2021-01-03 08:36:52|        267.7|            1.0|     369.4|
| 2021-01-12 09:57:36|        326.1|            0.0|    192.61|
| 2021-01-19 08:38:47|        260.5|            1.0|    149.03|
| 2021-01-31 20:48:50|         11.1|            0.0|     100.0|
| 2021-01-01 12:26:43|        14.86|            2.0|      99.0|
| 2021-01-18 12:50:24|         13.0|            0.0|      90.0|
+--------------------+-------------+---------------+----------+
```
## 3.6) Mostrar para cada uno de los valores de RateCodeID, el monto total y el monto promedio. Excluir los viajes en donde RateCodeID es ‘Group Ride’

```
>>> rate_code_summary = spark.sql("""
... SELECT
...     RateCodeID,
...     SUM(total_amount) AS Total_Amount,
...     AVG(total_amount) AS Average_Amount
... FROM
...     yellow_tripdata
... WHERE
...     RateCodeID != 6
... GROUP BY
...     RateCodeID
... ORDER BY
...     RateCodeID
... """)
>>> 
>>> # Display the results
>>> rate_code_summary.show()
+----------+--------------------+------------------+                            
|RateCodeID|        Total_Amount|    Average_Amount|
+----------+--------------------+------------------+
|       1.0|1.9496468430212937E7|15.606626116946773|
|       2.0|   973635.4700000732| 65.52937609369182|
|       3.0|   67363.26000000043| 78.69539719626219|
|       4.0|   90039.93000000082| 74.90842762063296|
|       5.0|  255075.08999999086|48.939963545662096|
|      99.0|  1748.0699999999997| 48.55749999999999|
+----------+--------------------+------------------+

>>> 
```
