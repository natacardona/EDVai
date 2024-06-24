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

![Arquitectura:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Arquitectura.png)

---

## <p aling="center"><b>TAREAS</b></p>
---

### 1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina :

#### 2021:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D

#### 2022:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

#### Aeropuertos_detalles:
https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-

## Solución:
Se creo un archivo que de ingest que descarga los archivos a un directorio de landing y luego mueve los archivos al HDFS.

[Ingest.sh](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/ingest.sh)

---

### 2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022(2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle delos aeropuertos (aeropuertos_detalle.csv)
---
![Schema Tabla 1:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Schema_Vuelos.png)

![Schema Tabla 2:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Schema_Detalle_Aeropuertos.png)

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

## 3. Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas 01/01/2021 y 30/06/2022 en las dos columnas creadas.Los archivos 202206-informe-ministerio.csv y 202206-informe-ministerio.csv → en latabla aeropuerto_tabla El archivo aeropuertos_detalle.csv → en la tabla aeropuerto_detalles_tabla

- Se crea un orquestador 
[dag_first_exercise.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/dag_first_exercise.py)

- ![Evidencia Orchestador en Airflow](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Airflow_Dag_Graph_Excersise_One.png)

---

## 4. Realizar las siguientes transformaciones en los pipelines de datos:

#### Scripting de transformación

[Transform_airports.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/transform_airports.py)

[transform_flights.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/transform_flights.py)

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

![Quey 1:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Flights_count_number_six.png)

---

## 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022. Mostrar consulta y Resultado de la query

![Quey 2](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Aerolineas_passenger_quantity_seven.png)

---

## 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente. Mostrar consulta y Resultado de la query

![Quey 3](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Movement_By_Type_And_Dates_eight.png)

---

## 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre. Mostrar consulta y Visualización

![Query 4](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Passenger_count_by_airlines_number_nine.png)

---

## 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,exceptuando aquellas aeronaves que no cuentan con nombre. Mostrar consulta y Visualización

![Query 5](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Most_Airlplanes_departing_from_Buenos_Aires.png)

---

## 11. Qué datos externos agregaría en este dataset que mejoraría el análisis de los datos

Para enriquecer el análisis de los datos existentes en el dataset y mejorar la capacidad de generar insights más profundos, consideraría agregar los siguientes tipos de datos externos:

- Información meteorológica detallada: Agregar datos sobre las condiciones climáticas, como temperatura, precipitación, y velocidad del viento en el aeropuerto de origen y destino para cada vuelo. Esto puede ayudar a analizar cómo las condiciones meteorológicas afectan los retrasos de vuelos, las cancelaciones y la duración del vuelo.

- Datos sobre eventos significativos: Información sobre eventos locales importantes (como festivales, conferencias, eventos deportivos) que podrían influir en el volumen de pasajeros y la demanda de vuelos en ciertas fechas y regiones.

- Datos económicos y de población: Agregar información sobre la economía y la población de las ciudades de los aeropuertos para entender mejor los patrones de tráfico aéreo relacionados con el tamaño y la actividad económica de la región.

- Información sobre el mantenimiento de las aeronaves: Datos sobre la última fecha de mantenimiento de cada aeronave y el tipo de mantenimiento realizado podrían proporcionar insights sobre la seguridad y la eficiencia operacional.

- Tarifas de vuelo y promociones: Datos sobre las tarifas de los vuelos y las promociones ofrecidas podrían ayudar a entender mejor la estrategia de precios de las aerolíneas y su impacto en la ocupación de los vuelos.

- Índices de satisfacción del cliente y reseñas: Incorporar datos sobre la satisfacción del cliente y reseñas para cada vuelo o aerolínea podría permitir analizar la relación entre la experiencia del pasajero y la lealtad o elección de aerolíneas.

- Datos sobre el tráfico aéreo global: Integrar datos sobre la situación del tráfico aéreo global puede ofrecer una perspectiva sobre cómo los eventos internacionales (como pandemias o crisis económicas) afectan el tráfico local y regional.

- Regulaciones aeroportuarias y cambios en políticas: La información sobre nuevas regulaciones o políticas (como cambios en las reglas de seguridad, nuevas tasas aeroportuarias, o restricciones de viaje) puede afectar el flujo de pasajeros y operaciones en los aeropuertos.

- Datos de competencia entre aerolíneas: Información sobre rutas, frecuencias y capacidades ofrecidas por diferentes aerolíneas en rutas similares puede ayudar a analizar la competencia y estrategia de mercado entre ellas.

- Impacto de las redes sociales y campañas de marketing: Analizar el efecto de las campañas de marketing y la presencia en redes sociales en la demanda de vuelos puede ofrecer insights sobre la eficacia de diferentes estrategias de marketing.

- Desarrollos infraestructurales: Información sobre construcciones o mejoras en la infraestructura de los aeropuertos y sus alrededores, como nuevas terminales, mejoras en el transporte terrestre, o expansión de servicios, que pueden influir en la capacidad del aeropuerto y la comodidad del pasajero.

- Información de seguimiento de vuelos en tiempo real: Datos en tiempo real sobre el estado de los vuelos (retrasados, en tiempo, cancelados) proporcionan una base para análisis operativos y pueden ayudar a identificar patrones de interrupciones.

Estos datos pueden ayudar a hacer análisis más robustos y contextuales, lo que resulta en una comprensión más profunda de los factores que influyen en el tráfico aéreo y la experiencia del pasajero.

---

## 12. Elabore sus conclusiones y recomendaciones sobre este proyecto.

Este proyecto representa un flujo de trabajo de procesamiento de datos robusto y escalable que integra varias tecnologías líderes en el ámbito de big data para la ingestión, almacenamiento, procesamiento y visualización de datos. Podemos realizar una breve descripción de cada paso y las tecnologías involucradas:

- Ingesta de Datos con Apache Airflow: Utilizar Airflow para orquestar y automatizar la ingesta de datos asegura que los procesos sean reproducibles y fáciles de monitorear. Airflow ofrece la flexibilidad de programar y manejar dependencias en complejas cadenas de procesamiento de datos, lo que lo hace ideal para este propósito.

- Almacenamiento en HDFS: Al almacenar los datos ingesados en HDFS, el sistema se beneficia de un sistema de archivos distribuido que ofrece alta disponibilidad y escalabilidad, crucial para manejar grandes volúmenes de datos y soportar operaciones de alto rendimiento.

- Procesamiento con Apache Spark: Spark es conocido por su capacidad para procesar grandes conjuntos de datos de forma rápida gracias a su procesamiento en memoria. Es ideal para realizar transformaciones complejas y análisis en grandes datasets, como los típicos en entornos de big data.

- Almacenamiento y consulta en Apache Hive: Hive proporciona una capa de abstracción SQL sobre Hadoop, facilitando consultas complejas, la gestión de datos y optimización de consultas sobre grandes volúmenes de datos, lo que facilita la integración con herramientas de BI y visualización.

- Visualización con Looker: La integración con Looker para la visualización de datos permite convertir los datos procesados en insights accionables a través de dashboards interactivos y reportes. Looker soporta la toma de decisiones basada en datos al proporcionar una interfaz accesible y flexible para explorar los datos.

Conclusión
El proyecto combina tecnologías de vanguardia para construir un pipeline de datos end-to-end que no solo es eficiente en términos de procesamiento y escalabilidad, sino que también facilita una toma de decisiones informada a través de visualizaciones avanzadas. Esta arquitectura no solo es robusta y eficiente, sino que también es flexible, permitiendo futuras expansiones o modificaciones según las necesidades del negocio cambien o crezcan. Al implementar este tipo de arquitectura, la organización está bien posicionada para manejar grandes volúmenes de datos de manera efectiva y obtener insights valiosos a partir de estos, apoyando así una amplia gama de decisiones comerciales y operativas.

---

## 13. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Sí aplica)

una arquitectura alternativa que utilice tanto soluciones on-premise como en la nube, centrándonos en maximizar la eficiencia, escalabilidad y la capacidad de manejar grandes volúmenes de datos de manera efectiva. Se podría proponer una propuesta que incorpore tecnologías de vanguardia tanto en la nube como en entornos locales:

Arquitectura Híbrida en la Nube y On-Premise

### Ingesta de Datos - Apache NiFi/Kafka:

- On-Premise: Utiliza Apache NiFi para la ingestión de datos. NiFi es excelente para la automatización de flujos de datos, con capacidades robustas para la recopilación, transformación y distribución de datos en tiempo real.
- Cloud: Apache Kafka puede ser utilizado para manejar flujos de datos en tiempo real, proporcionando un sistema de mensajería distribuido que es altamente escalable y permite un manejo eficiente de los flujos de entrada.

### Procesamiento de Datos - Apache Flink/Google Cloud Dataflow:

- On-Premise: Apache Flink para procesamiento de flujos de datos en tiempo real. Flink es conocido por su bajo latencia y capacidades de procesamiento en tiempo real.
- Cloud: Google Cloud Dataflow, una plataforma gestionada que simplifica la creación de pipelines de procesamiento de datos, tanto en lotes como en tiempo real.

### Almacenamiento de Datos - Google Cloud Bigtable/Apache HBase:

- On-Premise: Apache HBase, una base de datos NoSQL que proporciona lectura/escritura en tiempo real sobre grandes datasets.
- Cloud: Google Cloud Bigtable, altamente escalable y performante para aplicaciones de análisis y operacionales.

### Almacenamiento y Consulta de Datos - Google BigQuery/Apache Hive:

- On-Premise: Continuar con Apache Hive para consultas basadas en SQL sobre grandes datasets en Hadoop.
- Cloud: Google BigQuery para análisis de datos y ejecución de consultas SQL a gran escala, ofreciendo un almacén de datos serverless y altamente escalable.

### Visualización de Datos - Tableau/Google Data Studio:

- On-Premise: Tableau para visualizaciones avanzadas, conectándose directamente a HBase o Hive para obtener insights en tiempo real.
- Cloud: Google Data Studio, que se integra perfectamente con BigQuery y otras fuentes de datos en Google Cloud para crear dashboards interactivos y compartir insights fácilmente.

### Consideraciones de la Arquitectura

- Escalabilidad y Flexibilidad: La combinación de componentes en la nube y on-premise proporciona una solución flexible que puede escalar según las necesidades de procesamiento y almacenamiento de datos.
- Costo y Rendimiento: Cada componente ha sido seleccionado para equilibrar el costo y el rendimiento, maximizando la eficiencia operativa y reduciendo la latencia.
- Seguridad y Conformidad: Esta arquitectura permite a las organizaciones cumplir con requisitos específicos de seguridad y regulaciones locales al mantener ciertos datos sensibles on-premise mientras se aprovecha la escalabilidad y las capacidades avanzadas de la nube.

Esta arquitectura híbrida proporciona una solución robusta y adaptable que puede manejar diversas cargas de trabajo de procesamiento de datos, desde el análisis en tiempo real hasta el procesamiento de grandes volúmenes de datos históricos, todo ello manteniendo opciones de alta disponibilidad y recuperación ante desastres.

---

# Visualizaciones: 
https://lookerstudio.google.com/s/uT7r6C9b1P0