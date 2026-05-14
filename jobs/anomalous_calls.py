import sys
import os
import json
import math
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mean as _mean, stddev as _stddev, abs as _abs, round as _round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

import zlib

def copy_directory_from_hdfs_to_local(spark, hdfs_dir, output_dir):
    URI = spark._jvm.java.net.URI
    Path = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    IOUtils = spark._jvm.org.apache.hadoop.io.IOUtils
    
    conf = spark._jsc.hadoopConfiguration()
    
    target_full_path = "file://" + os.path.abspath(output_dir)
    target_fs = FileSystem.get(URI(target_full_path), conf)
    
    src_full_path = hdfs_dir
    src_fs = FileSystem.get(URI(src_full_path), conf)
    
    target_path_obj = Path(target_full_path)
    if not target_fs.exists(target_path_obj):
        target_fs.mkdirs(target_path_obj)
        
    src_dir_path = Path(src_full_path)
    if src_fs.exists(src_dir_path):
        file_statuses = src_fs.listStatus(src_dir_path)
        for status in file_statuses:
            file_path = status.getPath()
            file_name = file_path.getName()
            if file_name.startswith(".") or file_name.startswith("_SUCCESS"):
                continue
                
            src_file_path = file_path
            dest_file_path = Path(target_path_obj, file_name)
            
            in_stream = src_fs.open(src_file_path)
            out_stream = target_fs.create(dest_file_path, True)
            
            IOUtils.copyBytes(in_stream, out_stream, conf)
            
            in_stream.close()
            out_stream.close()

# Custom partitioner function
def custom_partitioner(key):
    # Deterministic hash instead of Python's randomized built-in hash()
    return zlib.crc32(key.encode('utf-8')) % 20

def process_partition(iterator):
    # Group all records by caller_id within this partition
    user_data = {}
    for caller_id, record in iterator:
        if caller_id not in user_data:
            user_data[caller_id] = []
        user_data[caller_id].append(record)
    
    results = []
    for caller_id, records in user_data.items():
        if len(records) < 2:
            mean = records[0]['duration_sec'] if records else 0
            stddev = 0.0
        else:
            durations = [r['duration_sec'] for r in records]
            mean = sum(durations) / len(durations)
            variance = sum((x - mean) ** 2 for x in durations) / (len(durations) - 1)
            stddev = math.sqrt(variance)
        
        # Avoid division by zero or 0 stddev
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
        print("Usage: anomalous_calls.py <run_id> [<input_path>]")
        sys.exit(1)
    
    run_id = sys.argv[1]
    input_path = sys.argv[2] if len(sys.argv) > 2 else "/data/cdr_data.csv"
    
    job_name = "anomalous_call_detection"
    output_path = f"/output/{job_name}/{run_id}/"
    hdfs_tmp_path = f"hdfs://namenode:8020/tmp/output/{job_name}/{run_id}/"

    spark = SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_count = df.count()

    # TWO-PASS SKEW MITIGATION STRATEGY:
    # 1. Detect skewed callers dynamically (exceeding 1% of total records)
    threshold = int(input_count * 0.01)
    caller_counts = df.groupBy("caller_id").count()
    skewed_callers_df = caller_counts.filter(col("count") > threshold)
    skewed_callers = [row["caller_id"] for row in skewed_callers_df.select("caller_id").collect()]

    if skewed_callers:
        skewed_df = df.filter(col("caller_id").isin(skewed_callers))
        normal_df = df.filter(~col("caller_id").isin(skewed_callers))
        
        # 2. Compute stats for skewed callers using highly-scalable Spark SQL GroupBy
        skewed_stats = skewed_df.groupBy("caller_id") \
            .agg(_mean("duration_sec").alias("mean"), _stddev("duration_sec").alias("stddev"))
        
        skewed_joined = skewed_df.join(skewed_stats, "caller_id")
        skewed_anomalies_df = skewed_joined.filter((col("stddev") > 0) & (_abs(col("duration_sec") - col("mean")) > 3 * col("stddev"))) \
            .select(
                col("caller_id"),
                col("timestamp").alias("call_timestamp"),
                col("duration_sec"),
                _round(col("mean"), 2).alias("user_mean_duration"),
                _round(col("stddev"), 2).alias("user_stddev")
            )
    else:
        # Create empty DataFrame with same schema if no skewed callers
        skewed_anomalies_schema = StructType([
            StructField("caller_id", StringType(), True),
            StructField("call_timestamp", StringType(), True),
            StructField("duration_sec", IntegerType(), True),
            StructField("user_mean_duration", FloatType(), True),
            StructField("user_stddev", FloatType(), True)
        ])
        skewed_anomalies_df = spark.createDataFrame([], skewed_anomalies_schema)
        normal_df = df

    # 3. For normal callers, apply Custom Partitioner on Key-Value RDD to route to reducers safely
    kv_rdd = normal_df.rdd.map(lambda row: (row['caller_id'], row.asDict()))
    partitioned_rdd = kv_rdd.partitionBy(20, custom_partitioner)
    
    # Process partitions to find normal caller anomalies
    normal_anomalies_rdd = partitioned_rdd.mapPartitions(process_partition)

    normal_anomalies_schema = StructType([
        StructField("caller_id", StringType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("duration_sec", IntegerType(), True),
        StructField("user_mean_duration", FloatType(), True),
        StructField("user_stddev", FloatType(), True)
    ])
    
    normal_anomalies_df = spark.createDataFrame(normal_anomalies_rdd, normal_anomalies_schema)
    
    # 4. Union the two DataFrames to construct the final results
    result_df = skewed_anomalies_df.union(normal_anomalies_df)
    result_df = result_df.select("caller_id", "call_timestamp", "duration_sec", "user_mean_duration", "user_stddev")

    # Cache result to prevent re-computation during count & write
    result_df.cache()

    # Coalesce to 1 to produce single CSV part file in HDFS
    result_df.coalesce(1).write.mode("overwrite").csv(hdfs_tmp_path, header=False)
    
    output_count = result_df.count()

    # Copy files to final target output destination (host-mount)
    copy_directory_from_hdfs_to_local(spark, hdfs_tmp_path, output_path)

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
    
    # Robustly use Hadoop FileSystem API supporting both HDFS and Local schemes
    manifest_str = json.dumps(manifest, indent=2)
    URI = spark._jvm.java.net.URI
    Path = spark._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    
    conf = spark._jsc.hadoopConfiguration()
    if not output_path.startswith("hdfs://") and not output_path.startswith("file://"):
        default_fs = conf.get("fs.defaultFS")
        if default_fs and default_fs.startswith("hdfs://"):
            full_path = default_fs + output_path
        else:
            full_path = "file://" + os.path.abspath(output_path)
    else:
        full_path = output_path
        
    fs = FileSystem.get(URI(full_path), conf)
    out = fs.create(Path(manifest_path), True)
    out.write(bytearray(manifest_str, 'utf-8'))
    out.close()

    # Clean HDFS staging directory after successful execution
    try:
        hdfs_path_obj = Path(hdfs_tmp_path)
        hdfs_fs = FileSystem.get(URI(hdfs_tmp_path), conf)
        if hdfs_fs.exists(hdfs_path_obj):
            hdfs_fs.delete(hdfs_path_obj, True)
    except Exception as e:
        print(f"Warning: Failed to clean HDFS directory: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
