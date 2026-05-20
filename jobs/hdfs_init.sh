#!/bin/bash
set -e

echo "Waiting for HDFS NameNode to exit safemode..."
timeout 180 hdfs dfsadmin -safemode wait || { echo "Error: Safemode wait timed out"; exit 1; }

echo "Initializing HDFS directory tree..."
hdfs dfs -mkdir -p /tmp/output
hdfs dfs -chmod -R 777 /tmp

echo "HDFS initialization completed successfully!"

# Keep this init service running so compose health checks remain stable.
tail -f /dev/null
