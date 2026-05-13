import sys
import os
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp

def main():
    if len(sys.argv) < 2:
        print("Usage: tower_heatmap.py <run_id>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    job_name = "tower_utilization_heatmap"
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/{job_name}/{run_id}/"

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_count = df.count()

    # Calculate heatmap: tower_id, hour_of_day, call_count
    # Parse timestamp and extract hour
    df_with_hour = df.withColumn("hour_of_day", hour(to_timestamp(col("timestamp"))))

    result_df = df_with_hour.groupBy("tower_id", "hour_of_day").count() \
        .withColumnRenamed("count", "call_count")

    # Ensure schema order: tower_id, hour_of_day, call_count
    result_df = result_df.select("tower_id", "hour_of_day", "call_count")

    result_df.write.mode("overwrite").csv(output_path, header=False)
    
    output_count = result_df.count()

    # Generate Manifest
    manifest = {
        "job_name": job_name,
        "run_id": run_id,
        "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": input_count,
        "output_record_count": output_count,
        "status": "SUCCESS"
    }

    manifest_path = os.path.join(output_path, "_MANIFEST.json")
    
    # Use Hadoop FileSystem API to ensure HDFS compatibility
    manifest_str = json.dumps(manifest, indent=2)
    URI = spark._jvm.java.net.URI
    Path = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI(output_path), spark._jsc.hadoopConfiguration())
    out = fs.create(Path(manifest_path))
    out.write(bytearray(manifest_str, 'utf-8'))
    out.close()

    spark.stop()

if __name__ == "__main__":
    main()
