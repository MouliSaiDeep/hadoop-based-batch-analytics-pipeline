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

def make_mapper(bcast_whales):
    def map_to_kv(row):
        caller_id = row['caller_id']
        if caller_id in bcast_whales.value:
            # Add salt suffix to distribute the whale's records across all partitions
            import random
            salt = random.randint(0, 19)
            key = f"{caller_id}_{salt}"
        else:
            key = caller_id
        return (key, row.asDict())

    return map_to_kv


def make_processor(bcast_whales, bcast_whale_stats):
    def process_partition(iterator):
        # Group all records by caller_id within this partition
        user_data = {}
        for key, record in iterator:
            caller_id = record['caller_id']
            if caller_id not in user_data:
                user_data[caller_id] = []
            user_data[caller_id].append(record)

        results = []
        for caller_id, records in user_data.items():
            is_whale = caller_id in bcast_whales.value
            if is_whale:
                mean, stddev = bcast_whale_stats.value.get(caller_id, (0.0, 0.0))
            else:
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
                        int(r['duration_sec']),  # Cast explicitly to int to align with Spark IntegerType
                        float(round(mean, 2)),
                        float(round(stddev, 2))
                    ))
        return iter(results)

    return process_partition

def main():
    if len(sys.argv) < 2:
        print("Usage: anomalous_calls.py <run_id> [<input_path>]")
        sys.exit(1)
    
    run_id = sys.argv[1]
    if not run_id or run_id.lower() == "none" or run_id.strip() == "":
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_fallback")

    input_path = sys.argv[2] if len(sys.argv) > 2 else "/data/cdr_data.csv"
    
    job_name = "anomalous_call_detection"
    output_path = f"/output/{job_name}/{run_id}/"
    hdfs_tmp_path = f"hdfs://namenode:8020/tmp/output/{job_name}/{run_id}/"

    spark = None
    input_count = 0
    
    try:
        spark = SparkSession.builder \
            .appName(job_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

        df = spark.read.csv(input_path, header=True, inferSchema=True)
        input_count = df.count()

        # Identify whale callers (e.g. counts > 50,000)
        counts_df = df.groupBy("caller_id").count()
        whales = [row['caller_id'] for row in counts_df.filter(col("count") > 50000).collect()]
        whales_set = set(whales)
        
        whale_stats = {}
        if whales:
            whale_stats_df = df.filter(col("caller_id").isin(whales)).groupBy("caller_id").agg(
                _mean("duration_sec").alias("mean"),
                _stddev("duration_sec").alias("stddev")
            )
            for row in whale_stats_df.collect():
                whale_stats[row['caller_id']] = (
                    row['mean'] if row['mean'] is not None else 0.0,
                    row['stddev'] if row['stddev'] is not None else 0.0
                )
        
        broadcast_whale_stats = spark.sparkContext.broadcast(whale_stats)
        broadcast_whales = spark.sparkContext.broadcast(whales_set)

        # Apply Custom Partitioner on Key-Value RDD to route all caller records deterministicly to reducers
        kv_rdd = df.rdd.map(make_mapper(broadcast_whales))
        partitioned_rdd = kv_rdd.partitionBy(20, custom_partitioner)
        
        # Process partitions to find caller anomalies
        anomalies_rdd = partitioned_rdd.mapPartitions(make_processor(broadcast_whales, broadcast_whale_stats))

        anomalies_schema = StructType([
            StructField("caller_id", StringType(), True),
            StructField("call_timestamp", StringType(), True),
            StructField("duration_sec", IntegerType(), True),
            StructField("user_mean_duration", FloatType(), True),
            StructField("user_stddev", FloatType(), True)
        ])
        
        result_df = spark.createDataFrame(anomalies_rdd, anomalies_schema)
        result_df = result_df.select("caller_id", "call_timestamp", "duration_sec", "user_mean_duration", "user_stddev")

        # Cache result to prevent re-computation during count & write
        result_df.cache()

        # Obtain output count before coalesce and write
        output_count = result_df.count()

        # Coalesce to 1 to produce single CSV part file in HDFS
        result_df.coalesce(1).write.mode("overwrite").csv(hdfs_tmp_path, header=False)

        # Copy files to final target output destination (host-mount)
        copy_directory_from_hdfs_to_local(spark, hdfs_tmp_path, output_path)

        # Generate Success Manifest
        manifest = {
            "job_name": job_name,
            "run_id": run_id,
            "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "input_path": input_path,
            "output_path": hdfs_tmp_path,
            "input_record_count": input_count,
            "output_record_count": output_count,
            "status": "SUCCESS"
        }

        # Write manifest using local write or Hadoop FS API fallback
        try:
            os.makedirs(output_path, exist_ok=True)
            manifest_path = os.path.join(output_path, "_MANIFEST.json")
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
        except Exception as me:
            print(f"Warning: Failed local manifest write, trying Hadoop FS API: {me}")
            manifest_path = os.path.join(output_path, "_MANIFEST.json")
            manifest_str = json.dumps(manifest, indent=2)
            URI = spark._jvm.java.net.URI
            Path = spark._jvm.org.apache.hadoop.fs.Path
            FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
            conf = spark._jsc.hadoopConfiguration()
            if not output_path.startswith(("hdfs://", "file://")):
                default_fs = conf.get("fs.defaultFS")
                full_path = (default_fs + output_path) if (default_fs and default_fs.startswith("hdfs://")) \
                            else ("file://" + os.path.abspath(output_path))
            else:
                full_path = output_path
            fs = FileSystem.get(URI(full_path), conf)
            out = fs.create(Path(manifest_path), True)
            out.write(bytearray(manifest_str, 'utf-8'))
            out.close()

        # Clean HDFS staging directory after successful execution
        try:
            Path = spark._jvm.org.apache.hadoop.fs.Path
            URI = spark._jvm.java.net.URI
            FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
            conf = spark._jsc.hadoopConfiguration()
            hdfs_path_obj = Path(hdfs_tmp_path)
            hdfs_fs = FileSystem.get(URI(hdfs_tmp_path), conf)
            if hdfs_fs.exists(hdfs_path_obj):
                hdfs_fs.delete(hdfs_path_obj, True)
        except Exception as e:
            print(f"Warning: Failed to clean HDFS directory: {e}")

    except Exception as err:
        print(f"ERROR: PySpark Job '{job_name}' failed: {err}")
        # Generate Failure Manifest
        failure_manifest = {
            "job_name": job_name,
            "run_id": run_id,
            "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "input_path": input_path,
            "output_path": hdfs_tmp_path,
            "input_record_count": input_count,
            "output_record_count": 0,
            "status": "FAILURE"
        }
        try:
            os.makedirs(output_path, exist_ok=True)
            manifest_path = os.path.join(output_path, "_MANIFEST.json")
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(failure_manifest, f, indent=2)
        except Exception as me:
            print(f"Warning: Failed to write failure manifest: {me}")
        raise err
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
