import sys
import os
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

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

def main():
    if len(sys.argv) < 2:
        print("Usage: top_callers.py <run_id> [<input_path>]")
        sys.exit(1)
    
    run_id = sys.argv[1]
    input_path = sys.argv[2] if len(sys.argv) > 2 else "/data/cdr_data.csv"
    
    job_name = "top_callers_by_spend"
    output_path = f"/output/{job_name}/{run_id}/"
    hdfs_tmp_path = f"hdfs://namenode:8020/tmp/output/{job_name}/{run_id}/"

    spark = SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    input_count = df.count()

    # Calculate top callers by spend
    result_df = df.groupBy("caller_id") \
        .agg(_sum("charge_amount").alias("total_spend")) \
        .orderBy(col("total_spend").desc()) \
        .limit(100)

    # Cache result to prevent re-computation during count & write
    result_df.cache()

    # Write output to HDFS (highly reliable, no gRPC FUSE mount bugs)
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
