#!/bin/bash

# Download the file to the local landing directory
wget -O /home/hadoop/landing/starwars.csv https://raw.githubusercontent.com/fpineyro/homework-0/master/starwars.csv

# Check if the download was successful
if [[ -f "/home/hadoop/landing/starwars.csv" ]]; then
    echo "Download successful."

    # Move the file to HDFS and overwrite if it already exists
    hdfs dfs -put -f /home/hadoop/landing/starwars.csv /ingest
    if [[ $? -eq 0 ]]; then
        echo "File successfully copied to HDFS, overwriting the existing one."

        # Remove the file from the local landing directory
        rm /home/hadoop/landing/starwars.csv
        echo "Local file removed."
    else
        echo "Failed to copy file to HDFS, local copy retained."
    fi
else
    echo "Download failed, file not found in local landing directory."
fi
