import sys
import os
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

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
            
            # Fast native JVM copy (avoids Py4J gateway bytearray reference issues)
            IOUtils.copyBytes(in_stream, out_stream, conf)
            
            in_stream.close()
            out_stream.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: revenue_recon.py <run_id> [<input_path>]")
        sys.exit(1)
    
    run_id = sys.argv[1]
    if not run_id or run_id.lower() == "none" or run_id.strip() == "":
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_fallback")

    input_path = sys.argv[2] if len(sys.argv) > 2 else "/data/cdr_data.csv"
    
    job_name = "revenue_reconciliation"
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

        # Calculate total revenue
        result_df = df.agg(_sum("charge_amount").alias("total_revenue"))

        # Cache result to prevent duplicate compute triggers
        result_df.cache()

        # Obtain output count before coalesce and write
        output_count = result_df.count()

        # Write output to HDFS (highly reliable, no gRPC FUSE mount bugs)
        result_df.coalesce(1).write.mode("overwrite").csv(hdfs_tmp_path, header=False)

        # Copy files to final target output destination (host-mount)
        copy_directory_from_hdfs_to_local(spark, hdfs_tmp_path, output_path)

        # Generate Success Manifest
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
            "output_path": output_path,
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
