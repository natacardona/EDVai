#!/bin/bash

# Download the file to the local landing directory
wget -O /home/nifi/ingest/yellow_tripdata_2021-01.parquet https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

# Check if the download was successful
if [[ -f "/home/nifi/ingest/yellow_tripdata_2021-01.parquet" ]]; then
    echo "Download successful."
else
    echo "Download failed."
fi

