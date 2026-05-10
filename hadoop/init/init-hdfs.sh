#!/bin/bash
# Initializes HDFS directory structure and uploads the vehicle dataset.
# Runs once on first cluster start via the hdfs-init service.

set -e

echo "[init-hdfs] Waiting for NameNode RPC to become available..."
until hdfs dfs -ls / > /dev/null 2>&1; do
  echo "[init-hdfs] NameNode not ready yet, retrying in 5 s..."
  sleep 5
done

echo "[init-hdfs] NameNode is ready. Creating directory structure..."

hdfs dfs -mkdir -p /vehicle/raw
hdfs dfs -mkdir -p /incoming
hdfs dfs -mkdir -p /processed
hdfs dfs -mkdir -p /checkpoint/streaming

echo "[init-hdfs] Uploading vehicle_data.csv to HDFS /vehicle/raw/ ..."
if hdfs dfs -test -e /vehicle/raw/vehicle_data.csv; then
  echo "[init-hdfs] File already exists in HDFS, skipping upload."
else
  hdfs dfs -put /tmp/data/vehicle_data.csv /vehicle/raw/vehicle_data.csv
  echo "[init-hdfs] Upload complete."
fi

echo "[init-hdfs] HDFS layout:"
hdfs dfs -ls /vehicle/raw /incoming /processed /checkpoint
echo "[init-hdfs] Done."
