# Alquiler de automóviles


<P>Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y
reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se
encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de
automóvil, valoración de cada alquiler, etc.
Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos
listos para ser visualizados y responder las preguntas de negocio.</p>

# Para este punto vamos a utilizar esta arquitectura propuesta:

![Arquitectura:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Arquitectura.png)

## 1. Crear en hive una database car_rental_db y dentro una tabla llamada car_rental_analytics, con estos campos:

| Campo             | Tipo     |
|-------------------|----------|
| fuelType          | string   |
| rating            | integer  |
| renterTripsTaken  | integer  |
| reviewCount       | integer  |
| city              | string   |
| state_name        | string   |
| owner_id          | integer  |
| rate_daily        | integer  |
| make              | string   |
| model             | string   |
| year              | integer  |

## 2. Crear script para el ingest de estos dos files

- https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=rst=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

- https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B

- __*Sugerencia:__* descargar el segundo archivo con un comando similar al abajo mencionado, ya que al tener caracteres como ‘&’ falla si no se le asignan comillas. Adicionalmente, el parámetro -O permite asignarle un nombre más legible al archivo descargado

```
wget -P ruta_destino -O ruta_destino/nombre_archivo.csv ruta_al_archivo
```

## Solución:
Se creo un archivo que de ingest que descarga los archivos a un directorio de landing y luego mueve los archivos al HDFS.

[ingest_rents.sh](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/ingest_rents.sh)


## 3. Crear un script para tomar el archivo desde HDFS y hacer las siguientes transformaciones:

Para este punto se creo el siguiente script: [transform_car_rentals.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/transform_car_rentals.py)

- En donde sea necesario, modificar los nombres de las columnas. Evitar espacios
y puntos (reemplazar por _ ). Evitar nombres de columna largos.

- El código para cambiar nombres de columnas se encuentra en las líneas que usan `withColumnRenamed`, que ajustan varios nombres para evitar caracteres no deseados y acortar los nombres.

```
df_rental_locations = df_rental_locations.withColumnRenamed("fuelType", "fueltype") ...
df_georef_location = df_georef_location.withColumnRenamed("Geo Point", "geo_point") ...

```

- Redondear los float de ‘rating’ y castear a int.
- Esta transformación se realiza seleccionando la columna `rating`, redondeando y cambiando el tipo a entero.

```
df_rental_locations = df_rental_locations.select(
    round(col("rating"), 1).cast("int").alias("rating"), ...
)
```

- Joinear ambos files
- El join entre los dataframes `df_rental_locations` y `df_georef_location` se basa en la columna 'state_name'.

```
df_inner_joined_rental_location = df_rental_locations.join(df_georef_location, ...

```

- Eliminar los registros con rating nulo.
- Los registros con rating igual a cero son filtrados justo después del join.


```
df_filtered_rating_diff_than_zero = df_inner_joined_rental_location.filter(df_inner_joined_rental_location["rating"] != 0)

```

- Cambiar mayúsculas por minúsculas en ‘fuelType’
- Esta conversión de mayúsculas a minúsculas se asegura en el renombramiento inicial de las columnas para la consistencia.

```
df_rental_locations = df_rental_locations.withColumnRenamed("fuelType", "fueltype")

```

- Excluir el estado Texas
- Los registros para el estado de Texas se excluyen en el filtro final antes de la inserción en Hive.

```
df_filtered_texas = df_filtered_rating_diff_than_zero.filter(df_filtered_rating_diff_than_zero["state_name"] != "TX")

```

Finalmente insertar en Hive el resultado

- Los datos filtrados se insertan en la tabla Hive usando la función `sql` del contexto Hive.
```
hc.sql("insert into car_rental_db.car_rental_analytics select * from tmp_car_analytics_data;")
```

## 4. Realizar un proceso automático en Airflow que orqueste los pipelines creados en los puntos anteriores. Crear dos tareas:

- ![](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberOne/Files/Airflow_Dag_Graph_Excersise_One.png)
- a. Un DAG padre que ingente los archivos y luego llame al DAG hijo

[dag_second_exercise_child.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/dag_second_exercise_child.py)


- b. Un DAG hijo que procese la información y la cargue en Hive
[dag_second_exercise_child.py](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/dag_second_exercise_parent.py)


## 5. Por medio de consultas SQL al data-warehouse, mostrar:

- a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos
ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.

![Qty_rented_cars:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Qty_rented_cars.png)

- b. Los 5 estados con menor cantidad de alquileres (crear visualización)

![Five_Top_State_less_rents:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Five_Top_State_less_rents.png)

- c. Los 10 modelos (junto con su marca) de autos más rentados (crear visualización)

![Most_Top_10_rented_cars:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Most_Top_10_rented_cars.png)

- d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles
fabricados desde 2010 a 2015

![Rents_By_year:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Rents_By_year.png)

- e. Las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o
electrico)

![Top_5_cities_renting_hybrid_and_electric_cars:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Top_5_cities_renting_hybrid_and_electric_cars.png)

- f. El promedio de reviews, segmentando por tipo de combustible

![Average_of_reviews_depends_of_fuel_type:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberTwo/Files/Average_of_reviews_depends_of_fuel_type.png)

## 6. Elabore sus conclusiones y recomendaciones sobre este proyecto.

- Aceptación de Autos Híbridos y Eléctricos: los autos híbridos y eléctricos tienen una buena aceptación entre los clientes, reflejada en el número de alquileres y las calificaciones promedio.

- Demanda en Ciudades Específicas: San Diego y Las Vegas lideran la demanda de vehículos ecológicos, lo que sugiere que estos mercados están más inclinados hacia la sostenibilidad.

- Oportunidades de Marketing: Algunos estados muestran una baja actividad de alquiler, lo que podría ser una oportunidad para implementar estrategias de marketing específicas.

- Popularidad de Tesla: Tesla se destaca como una de las marcas más populares entre los arrendatarios, con varios de sus modelos en el top 10 de los más alquilados, indicando una tendencia hacia vehículos eléctricos y de alto rendimiento.

- Preferencia por Vehículos Más Nuevos: La distribución de alquileres por año de fabricación muestra una preferencia por vehículos más nuevos, especialmente los fabricados entre 2010 y 2015, con un incremento notable en los alquileres de los modelos más recientes de este rango.

- Tendencia de Alquileres por Año de Fabricación:
Los datos muestran una clara tendencia ascendente en la cantidad de alquileres de vehículos fabricados entre 2010 y 2015. Los modelos más recientes dentro de este rango, especialmente los de 2015, son los más alquilados, con 532 alquileres en comparación con los 144 de los modelos de 2010.

- Valoración de Autos por Tipo de Combustible: Las reseñas de los clientes indican que los autos híbridos son los más valorados, seguidos de cerca por los de gasolina y eléctricos. Los vehículos diésel tienen menos reseñas y una valoración promedio más baja.

## 7. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Si aplica)

### Ingesta de Datos

Se podría utilizar un sistema de ingesta de datos como Apache NiFi o AWS Glue para recopilar datos desde múltiples fuentes (archivos CSV, bases de datos, APIs, etc.). Esto permite una ingesta de datos más flexible y escalable.

### Almacenamiento de Datos Crudos

Se podrían almacenar los datos crudos en un Data Lake utilizando Amazon S3 o Google Cloud Storage en lugar de HDFS. Esto proporciona un almacenamiento más económico y escalable, con integración nativa con otros servicios en la nube.

### Procesamiento de Datos

Seguiríamos usando Apache Spark para el procesamiento de datos, pero desplegado sobre un clúster manejado como Databricks o Google Cloud Dataproc. Esto simplifica la administración del clúster y proporciona mayor escalabilidad y resiliencia.

### Almacenamiento de Datos Procesados

Podemos utilizar una base de datos optimizada para análisis como Amazon Redshift, Google BigQuery o Snowflake para almacenar los datos procesados. Estas soluciones proporcionan un rendimiento de consulta más rápido y una mejor optimización para grandes volúmenes de datos.

### Orquestación de Procesos

Tomando como base Apache Airflow podemos usarlo para la orquestación de flujos de trabajo y la programación de trabajos de procesamiento de datos. Ya que Airflow permite una mejor gestión y monitoreo de las tareas de ETL.

### Visualización y Análisis

Utilizar herramientas de visualización de datos como Tableau o Power BI para crear dashboards interactivos y obtener insights de los datos procesados.

---

## Visualizaciones: https://lookerstudio.google.com/reporting/e6e42751-24ac-4677-abe8-73cbea34f08e