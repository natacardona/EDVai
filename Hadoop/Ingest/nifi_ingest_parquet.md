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

