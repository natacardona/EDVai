#!/bin/bash

# Download the first file to the local landing directory
wget -O /home/hadoop/landing/2021-informe-ministerio.csv 'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D'

# Check if the download was successful
if [[ -f "/home/hadoop/landing/2021-informe-ministerio.csv" ]]; then
    echo "Download of 2021-informe-ministerio.csv successful."

    # Move the file to HDFS and overwrite if it already exists
    hdfs dfs -put -f /home/hadoop/landing/2021-informe-ministerio.csv /ingest
    if [[ $? -eq 0 ]]; then
        echo "File 2021-informe-ministerio.csv successfully copied to HDFS, overwriting the existing one."

        # Remove the file from the local landing directory
        rm /home/hadoop/landing/2021-informe-ministerio.csv
        echo "Local file 2021-informe-ministerio.csv removed."
    else
        echo "Failed to copy file 2021-informe-ministerio.csv to HDFS, local copy retained."
    fi
else
    echo "Download of 2021-informe-ministerio.csv failed, file not found in local landing directory."
fi

# Download the second file to the local landing directory
wget -O /home/hadoop/landing/202206-informe-ministerio.csv 'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D'

# Check if the download was successful
if [[ -f "/home/hadoop/landing/202206-informe-ministerio.csv" ]]; then
    echo "Download of 202206-informe-ministerio.csv successful."

    # Move the file to HDFS and overwrite if it already exists
    hdfs dfs -put -f /home/hadoop/landing/202206-informe-ministerio.csv /ingest
    if [[ $? -eq 0 ]]; then
        echo "File 202206-informe-ministerio.csv successfully copied to HDFS, overwriting the existing one."

        # Remove the file from the local landing directory
        rm /home/hadoop/landing/202206-informe-ministerio.csv
        echo "Local file 202206-informe-ministerio.csv removed."
    else
        echo "Failed to copy file 202206-informe-ministerio.csv to HDFS, local copy retained."
    fi
else
    echo "Download of 202206-informe-ministerio.csv failed, file not found in local landing directory."
fi

# Download the third file to the local landing directory
wget -O /home/hadoop/landing/aeropuertos_detalle.csv 'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D'

# Check if the download was successful
if [[ -f "/home/hadoop/landing/aeropuertos_detalle.csv" ]]; then
    echo "Download of aeropuertos_detalle.csv successful."

    # Move the file to HDFS and overwrite if it already exists
    hdfs dfs -put -f /home/hadoop/landing/aeropuertos_detalle.csv /ingest
    if [[ $? -eq 0 ]]; then
        echo "File aeropuertos_detalle.csv successfully copied to HDFS, overwriting the existing one."

        # Remove the file from the local landing directory
        rm /home/hadoop/landing/aeropuertos_detalle.csv
        echo "Local file aeropuertos_detalle.csv removed."
    else
        echo "Failed to copy file aeropuertos_detalle.csv to HDFS, local copy retained."
    fi
else
    echo "Download of aeropuertos_detalle.csv failed, file not found in local landing directory."
fi
