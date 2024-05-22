Inges#!/bin/bash

# Download the file to the local landing directory
wget -O /home/nifi/ingest/starwars.csv https://raw.githubusercontent.com/fpineyro/homework-0/master/starwars.csv

# Check if the download was successful
if [[ -f "/home/nifi/ingest/starwars.csv" ]]; then
    echo "Download successful."
fi
