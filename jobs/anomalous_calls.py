import sys
import os
import json
import math
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Custom partitioner function
def custom_partitioner(key):
    return hash(key)

def process_partition(iterator):
    # Group all records by caller_id within this partition
    # Since we used partitionBy(..., custom_partitioner) on caller_id, 
    # we are guaranteed that all records for a given caller_id are in the same partition.
    user_data = {}
    for caller_id, record in iterator:
        if caller_id not in user_data:
            user_data[caller_id] = []
        user_data[caller_id].append(record)
    
    results = []
    for caller_id, records in user_data.items():
        if len(records) < 2:
            continue # Standard deviation requires at least 2 data points
        
        durations = [r['duration_sec'] for r in records]
        mean = sum(durations) / len(durations)
        variance = sum((x - mean) ** 2 for x in durations) / (len(durations) - 1)
        stddev = math.sqrt(variance)
        
        # Avoid division by zero issues or 0 stddev
        if stddev == 0:
            continue
            
        for r in records:
            if abs(r['duration_sec'] - mean) > 3 * stddev:
                results.append((
                    caller_id,
                    r['timestamp'],
                    r['duration_sec'],
                    float(round(mean, 2)),
                    float(round(stddev, 2))
                ))
    return iter(results)

def main():
    if len(sys.argv) < 2:
        print("Usage: anomalous_calls.py <run_id>")
        sys.exit(1)
    
    run_id = sys.argv[1]
    job_name = "anomalous_call_detection"
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/{job_name}/{run_id}/"

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_count = df.count()

    # Create Key-Value RDD and apply Custom Partitioner
    kv_rdd = df.rdd.map(lambda row: (row['caller_id'], row.asDict()))
    
    # 20 partitions should be sufficient for our 2M records
    partitioned_rdd = kv_rdd.partitionBy(20, custom_partitioner)
    
    # Process partitions to find anomalies
    anomalous_rdd = partitioned_rdd.mapPartitions(process_partition)

    schema = StructType([
        StructField("caller_id", StringType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("duration_sec", IntegerType(), True),
        StructField("user_mean_duration", FloatType(), True),
        StructField("user_stddev", FloatType(), True)
    ])
    
    result_df = spark.createDataFrame(anomalous_rdd, schema)
    
    # Select in correct order just in case
    result_df = result_df.select("caller_id", "call_timestamp", "duration_sec", "user_mean_duration", "user_stddev")
    
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
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    spark.stop()

if __name__ == "__main__":
    main()
