# ingest.sh

## Download data
wget -nc -O /home/hadoop/landing/CarRentalData.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv
wget -nc -O /home/hadoop/landing/georef-united-states-of-america-state.csv https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

# Moving to HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/CarRentalData.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/georef-united-states-of-america-state.csv /ingest

# Removing not needed files
rm /home/hadoop/landing/CarRentalData.csv
rm /home/hadoop/landing/georef-united-states-of-america-state.csv