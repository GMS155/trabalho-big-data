#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# ingest.sh — copy a local CSV file into HDFS /incoming/ to trigger the
#             Spark Structured Streaming job.
#
# Usage:
#   ./scripts/ingest.sh <path/to/file.csv>
#
# Example:
#   ./scripts/ingest.sh data/vehicle_data.csv
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

FILE="${1:-}"

if [[ -z "$FILE" ]]; then
  echo "Usage: $0 <path/to/file.csv>" >&2
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "Error: file not found: $FILE" >&2
  exit 1
fi

FILENAME=$(basename "$FILE")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
HDFS_DEST="/incoming/${TIMESTAMP}_${FILENAME}"

echo "[ingest] Uploading $FILE → hdfs://namenode:9000$HDFS_DEST ..."
docker exec -i namenode hdfs dfs -put - "$HDFS_DEST" < "$FILE"
echo "[ingest] Done. The streaming job will process the file within 30 s."
