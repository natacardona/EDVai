## Practica nifi:

nifi-me permite automatizar el flujo de datos se usa en diversas indrustrias y para una variedad de casos de uso, para este caso vamos a descargar un archivo csv de la nube lo vamos a ubicar en una ruta y con nifi a través de los processors realizaremos un proceso de ingesta mediante GET-PUT y GET-PUT HDFS


## 1) Creación del script .sh que descarga el archivo starwars.csv al directorio

# Pasos
vamos a crear un script llamado nifi_ingest.sh
con el siguiente comando: 

```
nifi@98902409275d:~/ingest$ cat > nifi_ingest.sh
#!/bin/bash

# Download the file to the local landing directory
wget -O /home/nifi/ingest/starwars.csv https://raw.githubusercontent.com/fpineyro/homework-0/master/starwars.csv

# Check if the download was successful
if [[ -f "/home/nifi/ingest/starwars.csv" ]]; then
    echo "Download successful."
```
posterior a esto antes de correrlo debemos darle permisos de ejecución

```
nifi@98902409275d:~/ingest$ chmod +x nifi_ingest.sh 
```
Luego podremos ejecutarlo con el siguiente comando:
```
./home/nifi/ingest/nifi_ingest.sh
```
# Result
```
nifi@98902409275d:~/ingest$ ./nifi_ingest.sh
./nifi_ingest.sh: line 1: Inges#!/bin/bash: No such file or directory
--2024-05-22 23:28:30--  https://raw.githubusercontent.com/fpineyro/homework-0/master/starwars.csv
Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 185.199.110.133, 185.199.109.133, 185.199.111.133, ...
Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|185.199.110.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 5462 (5.3K) [text/plain]
Saving to: ‘/home/nifi/ingest/starwars.csv’

/home/nifi/ingest/starwar 100%[===================================>]   5.33K  --.-KB/s    in 0s      

2024-05-22 23:28:30 (19.4 MB/s) - ‘/home/nifi/ingest/starwars.csv’ saved [5462/5462]

Download successful.

```


## 2) Usando procesos en Nifi:
a) tomar el archivo starwars.csv desde el directorio /home/nifi/ingest.
b) Mover el archivo starwars.csv desde el directorio anterior, a /home/nifi/bucket
(crear el directorio si es necesario)
c) Tomar nuevamente el archivo, ahora desde /home/nifi/bucket
d) Ingestarlo en HDFS/nifi (si es necesario, crear el directorio con hdfs dfs -mkdir
/nifi )


# Flujo de processors en nifi
![Nifi ETL](https://github.com/natacardona/EDVai/blob/main/Hadoop/Ingest/Nifi_ETL.png)

# Visualizando el resultado de la ingesta en el HDFS
```
hadoop@d4437f61daec:/$ hdfs dfs -ls /nifi
Found 2 items
-rw-r--r--   1 nifi supergroup        307 2024-05-22 20:52 /nifi/nifi_ingest.sh
-rw-r--r--   1 nifi supergroup       5462 2024-05-22 21:04 /nifi/starwars.csv
hadoop@d4437f61daec:/$ 
```