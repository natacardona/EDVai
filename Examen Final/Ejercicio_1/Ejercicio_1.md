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

Para este punto vamos a utilizar esta arquitectura propuesta:

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
CREATE TABLE airports (
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
    uso STRING
);
```





