# ingest.sh

## Download all the info
wget -O /home/hadoop/landing/2021-informe-ministerio.csv 'https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv'
wget -O /home/hadoop/lading/202206-informe-ministerio.csv 'https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv'
wget -O /home/hadoop/landing/aeropuertos_detalle.csv 'https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv'

# Moving to HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/2021-informe-ministerio.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/202206-informe-ministerio.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/aeropuertos_detalle.csv /ingest

# Removing not needed files
rm /home/hadoop/landing/2021-informe-ministerio.csv
rm /home/hadoop/landing/202206-informe-ministerio.csv
rm /home/hadoop/landing/aeropuertos_detalle.csv