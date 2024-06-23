## EXAMEN FINAL

Aviación Civil

La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al
ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino,<b> como puede ser:* </b> cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades
de partidas y aterrizajes entre fechas determinadas, etc.
Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y
realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y
hacer sus recomendaciones con respecto al estado actual.

Listado de vuelos realizados:
https://datos.gob.ar/lv/dataset/transporte-aterrizajes-despegues-procesados-por-administracionnacional-

aviacion-civil-anac
Listado de detalles de aeropuertos de Argentina:
https://datos.transporte.gob.ar/dataset/lista-aeropuertos

# Para este punto vamos a utilizar esta arquitectura propuesta:

![Arquitectura:](https://github.com/natacardona/EDVai/blob/main/Examen%20Final/Ejercicio_1/Images/Arquitectura.png)


## <p aling="center"><b>TAREAS</b></p>
1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina :

2021:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D

2022:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

Aeropuertos_detalles:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-


2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022
(2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de
los aeropuertos (aeropuertos_detalle.csv)


![Schema Tabla 1:](https://github.com/natacardona/EDVai/blob/d4b4195277776a22743a1bd0d5c2e36222c9b744/Examen%20Final/Ejercicio_1/Images/Schema_Vuelos.png)

![Schema Tabla 2:](https://github.com/natacardona/EDVai/blob/d4b4195277776a22743a1bd0d5c2e36222c9b744/Examen%20Final/Ejercicio_1/Images/Schema_Detalle_Aeropuertos.png)

Creamos las siguientes tablas en Hive 

```
CREATE TABLE vuelos (
    fecha DATE,
    horaUTC STRING,
    clase_de_vuelo STRING,
    clasificacion_de_vuelo STRING,
    tipo_de_movimiento STRING,
    aeropuerto STRING,
    origen_destino STRING,
    aerolinea_nombre STRING,
    aeronave STRING,
    pasajeros INT
);

CREATE TABLE detalle_aeropuertos (
    aeropuerto STRING,
    oac STRING,
    iata STRING,
    tipo STRING,
    denominacion STRING,
    coordenadas STRING,
    latitud STRING,
    longitud STRING,
    elev FLOAT,
    uom_elev STRING,
    ref STRING,
    distancia_ref FLOAT,
    direccion_ref STRING,
    condicion STRING,
    control STRING,
    region STRING,
    uso STRING,
    trafico STRING,
    sna STRING,
    concesionado STRING,
    provincia STRING,
);
```
hive> create database fligthsdb;
OK

hive> use fligthsdb;
OK
Time taken: 0.045 seconds
hive> 
    > CREATE TABLE vuelos (
    >     fecha DATE,
    >     horaUTC STRING,
    >     clase_de_vuelo STRING,
    >     clasificacion_de_vuelo STRING,
    >     tipo_de_movimiento STRING,
    >     aeropuerto STRING,
    >     origen_destino STRING,
    >     aerolinea_nombre STRING,
    >     aeronave STRING,
    >     pasajeros INT
    > );
OK
Time taken: 1.728 seconds
hive> 

hive> CREATE TABLE detalle_aeropuertos (
    >     aeropuerto STRING,
    >     oac STRING,
    >     iata STRING,
    >     tipo STRING,
    >     denominacion STRING,
    >     coordenadas STRING,
    >     latitud STRING,
    >     longitud STRING,
    >     elev FLOAT,
    >     uom_elev STRING,
    >     ref STRING,
    >     distancia_ref FLOAT,
    >     direccion_ref STRING,
    >     condicion STRING,
    >     control STRING,
    >     region STRING,
    >     uso STRING
    > );
OK
Time taken: 0.411 seconds
hive> 

5. Mostrar mediante una impresión de pantalla, que los tipos de campos de las tablas
sean los solicitados en el datawarehouse (ej: fecha date, aeronave string, pasajeros
integer, etc.)

hive> describe formatted vuelos;
OK
# col_name            	data_type           	comment             
	 	 
fecha               	date                	                    
horautc             	string              	                    
clase_de_vuelo      	string              	                    
clasificacion_de_vuelo	string              	                    
tipo_de_movimiento  	string              	                    
aeropuerto          	string              	                    
origen_destino      	string              	                    
aerolinea_nombre    	string              	                    
aeronave            	string              	                    
pasajeros           	int                 	                    
	 	 
# Detailed Table Information	 	 
Database:           	fligthsdb           	 
Owner:              	hadoop              	 
CreateTime:         	Tue Jun 18 14:24:02 ART 2024	 
LastAccessTime:     	UNKNOWN             	 
Retention:          	0                   	 
Location:           	hdfs://172.17.0.2:9000/user/hive/warehouse/fligthsdb.db/vuelos	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	COLUMN_STATS_ACCURATE	{\"BASIC_STATS\":\"true\"}
	numFiles            	0                   
	numRows             	0                   
	rawDataSize         	0                   
	totalSize           	0                   
	transient_lastDdlTime	1718731442          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	 
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat	 
OutputFormat:       	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	serialization.format	1                   
Time taken: 0.503 seconds, Fetched: 39 row(s)
hive> 

hive> describe formatted detalle_aeropuertos;
OK
# col_name            	data_type           	comment             
	 	 
aeropuerto          	string              	                    
oac                 	string              	                    
iata                	string              	                    
tipo                	string              	                    
denominacion        	string              	                    
coordenadas         	string              	                    
latitud             	string              	                    
longitud            	string              	                    
elev                	float               	                    
uom_elev            	string              	                    
ref                 	string              	                    
distancia_ref       	float               	                    
direccion_ref       	string              	                    
condicion           	string              	                    
control             	string              	                    
region              	string              	                    
uso                 	string              	                    
	 	 
# Detailed Table Information	 	 
Database:           	fligthsdb           	 
Owner:              	hadoop              	 
CreateTime:         	Tue Jun 18 14:25:18 ART 2024	 
LastAccessTime:     	UNKNOWN             	 
Retention:          	0                   	 
Location:           	hdfs://172.17.0.2:9000/user/hive/warehouse/fligthsdb.db/detalle_aeropuertos	 
Table Type:         	MANAGED_TABLE       	 
Table Parameters:	 	 
	COLUMN_STATS_ACCURATE	{\"BASIC_STATS\":\"true\"}
	numFiles            	0                   
	numRows             	0                   
	rawDataSize         	0                   
	totalSize           	0                   
	transient_lastDdlTime	1718731518          
	 	 
# Storage Information	 	 
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	 
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat	 
OutputFormat:       	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	 
Compressed:         	No                  	 
Num Buckets:        	-1                  	 
Bucket Columns:     	[]                  	 
Sort Columns:       	[]                  	 
Storage Desc Params:	 	 
	serialization.format	1                   
Time taken: 0.176 seconds, Fetched: 46 row(s)

hive> show tables;
OK
detalle_aeropuertos
vuelos
Time taken: 0.199 seconds, Fetched: 2 row(s)
hive> 

Crear el archivo ingest_aviation.sh que nos permita descargar los archivos mencionados abajo e ingestarlos en HDFS.

hadoop@d4437f61daec:~/scripts$ ./ingest_aviation.sh 
--2024-06-18 19:39:54--  https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D
Resolving edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)... 20.150.42.196
Connecting to edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)|20.150.42.196|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 32322556 (31M) [text/csv]
Saving to: '/home/hadoop/landing/2021-informe-ministerio.csv'

/home/hadoop/landing/2021-informe-mini 100%[=========================================================================>]  30.82M   489KB/s    in 1m 40s  

2024-06-18 19:41:35 (316 KB/s) - '/home/hadoop/landing/2021-informe-ministerio.csv' saved [32322556/32322556]

Download of 2021-informe-ministerio.csv successful.
File size: 32322556 bytes
File 2021-informe-ministerio.csv successfully copied to HDFS, overwriting the existing one.
Local file 2021-informe-ministerio.csv removed.
--2024-06-18 19:41:38--  https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D
Resolving edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)... 20.150.42.196
Connecting to edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)|20.150.42.196|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 22833520 (22M) [text/csv]
Saving to: '/home/hadoop/landing/202206-informe-ministerio.csv'

/home/hadoop/landing/202206-informe-mi 100%[=========================================================================>]  21.78M   199KB/s    in 77s     

2024-06-18 19:42:55 (291 KB/s) - '/home/hadoop/landing/202206-informe-ministerio.csv' saved [22833520/22833520]

Download of 202206-informe-ministerio.csv successful.
File size: 22833520 bytes
File 202206-informe-ministerio.csv successfully copied to HDFS, overwriting the existing one.
Local file 202206-informe-ministerio.csv removed.
--2024-06-18 19:42:58--  https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D
Resolving edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)... 20.150.42.196
Connecting to edvaibucket.blob.core.windows.net (edvaibucket.blob.core.windows.net)|20.150.42.196|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 136007 (133K) [text/csv]
Saving to: '/home/hadoop/landing/aeropuertos_detalle.csv'

/home/hadoop/landing/aeropuertos_detal 100%[=========================================================================>] 132.82K   259KB/s    in 0.5s    

2024-06-18 19:42:59 (259 KB/s) - '/home/hadoop/landing/aeropuertos_detalle.csv' saved [136007/136007]

Download of aeropuertos_detalle.csv successful.
File size: 136007 bytes
File aeropuertos_detalle.csv successfully copied to HDFS, overwriting the existing one.
Local file aeropuertos_detalle.csv removed.
hadoop@d4437f61daec:~/scripts$ 
Validación de archivos .sh
hadoop@d4437f61daec:~/scripts$ hdfs dfs -ls /ingest
Found 4 items
-rw-r--r--   1 hadoop supergroup   32322556 2024-06-18 19:41 /ingest/2021-informe-ministerio.csv
-rw-r--r--   1 hadoop supergroup   22833520 2024-06-18 19:42 /ingest/202206-informe-ministerio.csv
-rw-r--r--   1 hadoop supergroup     136007 2024-06-18 19:43 /ingest/aeropuertos_detalle.csv
-rw-r--r--   1 hadoop supergroup  125981363 2022-05-09 17:58 /ingest/yellow_tripdata_2021-01.csv
hadoop@d4437f61daec:~/scripts$ 