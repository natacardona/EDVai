## Practica nifi parquet:

## Ejercicio Clase 5

1. En el container de Nifi, crear un .sh que permita descargar el archivo yellow_tripdata_2021-01.parquet desde 
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
luego le damos permisos para poder ejecutarlo

nifi@98902409275d:~/ingest$ chmod +x nifi_ingest_parquet.sh
nifi@98902409275d:~/ingest$ ls
nifi_ingest_parquet.sh  nifi_ingest.sh  starwars.csv
