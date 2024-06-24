## EXAMEN FINAL

### Aviación Civil

## Requerimiento:

La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al
ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino,<b> como puede ser:* </b> cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades
de partidas y aterrizajes entre fechas determinadas, etc.
Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y
realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y
hacer sus recomendaciones con respecto al estado actual.

#### Listado de vuelos realizados:
https://datos.gob.ar/lv/dataset/transporte-aterrizajes-despegues-procesados-por-administracionnacional-

#### aviacion-civil-anac
#### Listado de detalles de aeropuertos de Argentina:
https://datos.transporte.gob.ar/dataset/lista-aeropuertos

# Para este punto vamos a utilizar esta arquitectura propuesta:

![Arquitectura:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Arquitectura.png)

---

## <p aling="center"><b>TAREAS</b></p>
---
### 1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina :
---

#### 2021:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D

#### 2022:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

#### Aeropuertos_detalles:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-

## Solución:

[Ingest](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/ingest.sh)
---
### 2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022(2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle delos aeropuertos (aeropuertos_detalle.csv)
---
![Schema Tabla 1:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Schema_Vuelos.png)

![Schema Tabla 2:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Schema_Detalle_Aeropuertos.png)

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

```
hive> create database fligthsdb;
OK
```

```
hive> use fligthsdb;
OK
Time taken: 0.045 seconds
```

```
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
```
```
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
    >     trafico STRING,
    >     sna STRING,
    >     concesionado STRING,
    >     provincia STRING    
    > );
OK
Time taken: 0.411 seconds
hive> 
```
---
## 3. Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos columnas creadas.
---
Los archivos 202206-informe-ministerio.csv y 202206-informe-ministerio.csv → en la
tabla aeropuerto_tabla
El archivo aeropuertos_detalle.csv → en la tabla aeropuerto_detalles_tabla

[DAG:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/dag_first_exercise.py)

![Orchestador:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Airflow_Dag_Graph_Excersise_One.png)

---
## 4. Realizar las siguientes transformaciones en los pipelines de datos:
---  
   ## Solución:

[Transform_airports:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/transform_airports.py)

[transform_flights:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/transform_flights.py)

   - Eliminar la columna `inhab` ya que no se utilizará para el análisis.
   - Eliminar la columna `fir` ya que no se utilizará para el análisis.

```
    df_airport = df_airport_details.drop('inhab', 'fir')
    df_airports_fixed = df_airport.fillna({"distancia_ref": 0})
```
   - Eliminar la columna `calidad del dato` ya que no se utilizará para el análisis.
    
```
    # Eliminar columnas que no se utilizarán para el análisis
    columns_to_drop = ["Calidad dato"]
    df_union = df_union.drop(*columns_to_drop)
```
   - Filtrar los vuelos internacionales, ya que solamente se analizarán los vuelos domésticos.
```
    # Filtrar los vuelos domésticos 'Clasificación Vuelo')
    df_domestic = df_union.filter(col('Clasificación Vuelo') == 'Doméstico')
```
   - En el campo `pasajeros`, si se encuentran campos en Null, convertirlos en 0 (cero).
    
```
    # Convertir valores NULL en 0 en las columnas 'Pasajeros' y 'distancia_ref'
    df_domestic = df_domestic.withColumn('Pasajeros', when(col('Pasajeros').isNull(), 0).otherwise(col('Pasajeros').cast("int")))
```
   - En el campo `distancia_ref`, si se encuentran campos en Null, convertirlos en 0 (cero).
```
     df_airports_fixed = df_airport.fillna({"distancia_ref": 0})
```
---
## 5. Mostrar mediante una impresión de pantalla, que los tipos de campos de las tablas sean los solicitados en el datawarehouse (ej: fecha date, aeronave string, pasajeros integer, etc.)
---

```
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
```

```
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
trafico                 string
sna                     string
concesionado            string
provincia               string

	 	 
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
```
```
hive> show tables;
OK
detalle_aeropuertos
vuelos
Time taken: 0.199 seconds, Fetched: 2 row(s)
hive> 
```
---
## 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query

[:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Flights_count_number_six.png)



---
## 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query
---

[:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Passenger_count_by_airlines_number_nine.png)



---
## 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query

[:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Movement_By_Type_And_Dates_eight.png)

## 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización

[:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Passenger_count_by_airlines_number_nine.png)

## 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización

[:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Images/Most_Airlplanes_departing_from_Buenos_Aires.png)

## 11. Qué datos externos agregaría en este dataset que mejoraría el análisis de los datos
---
## 12. Elabore sus conclusiones y recomendaciones sobre este proyecto.
---
## 13. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Sí aplica)
---