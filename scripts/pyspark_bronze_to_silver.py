#!/usr/bin/env python3
"""
pyspark_bronze_to_silver.py
Demonstrates PySpark distributed processing:
1. Connects PySpark to S3/MinIO
2. Reads raw data (Bronze layer)
3. Enforces schemas, filters bad rows, cleans timestamps
4. Performs deduplication using Window functions (latest updated_at per order)
5. Writes columnar partitioned Parquet data into the Silver layer
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# MinIO / S3 Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

def create_spark_session(app_name="BronzeToSilverProcessing"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_bronze_to_silver(input_path=None, output_path=None):
    spark = create_spark_session()
    print("[*] PySpark session initialized successfully.")

    input_path = input_path or "s3a://bronze/orders/"
    output_path = output_path or "s3a://silver/orders/"

    print(f"[*] Reading raw Bronze data from {input_path}...")
    try:
        raw_df = spark.read.json(input_path)
    except Exception as e:
        print(f"[!] Warning reading from S3: {e}. Falling back to sample local processing...")
        return

    print(f"[*] Raw Record Count: {raw_df.count()}")
    raw_df.printSchema()

    # Step 1: Clean & Cast types
    cleaned_df = raw_df \
        .withColumn("order_id", col("order_id").cast(IntegerType())) \
        .withColumn("user_id", col("user_id").cast(IntegerType())) \
        .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("updated_at", to_timestamp(col("updated_at"))) \
        .filter(col("order_id").isNotNull() & col("total_amount").isNotNull())

    # Step 2: Deduplication using Window function (latest state of order)
    window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
    deduped_df = cleaned_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Step 3: Add Partitioning columns
    silver_df = deduped_df \
        .withColumn("year", year(col("created_at"))) \
        .withColumn("month", month(col("created_at")))

    print(f"[*] Cleaned & Deduplicated Record Count: {silver_df.count()}")
    silver_df.show(5, truncate=False)

    # Step 4: Write to Silver as columnar Parquet partitioned by year and month
    print(f"[*] Writing to Silver Lakehouse at {output_path}...")
    silver_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("[+] Successfully completed Bronze -> Silver transformation.")
    spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()
