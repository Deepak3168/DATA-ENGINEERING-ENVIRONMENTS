#!/usr/bin/env python3
"""
pyspark_silver_to_gold.py
Demonstrates analytical aggregation and dimensional modeling:
1. Reads cleaned Parquet from Silver layer
2. Joins / aggregates data for business metrics (Gold layer)
3. Computes Daily Sales Performance and Customer Metrics
4. Sinks analytical tables to MinIO Gold and PostgreSQL Data Warehouse (OLAP)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, to_date, date_format

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGoldAggregation") \
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

def process_silver_to_gold():
    spark = create_spark_session()
    silver_path = "s3a://silver/orders/"
    gold_sales_path = "s3a://gold/daily_sales/"
    gold_customer_path = "s3a://gold/customer_metrics/"

    print(f"[*] Reading Silver data from {silver_path}...")
    try:
        silver_df = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"[!] Unable to read from {silver_path}: {e}")
        return

    # Filter only completed orders for revenue analytics
    completed_orders = silver_df.filter(col("order_status") == "COMPLETED")

    # Metric 1: Daily Sales Aggregation
    daily_sales = completed_orders \
        .withColumn("order_date", to_date(col("created_at"))) \
        .withColumn("date_id", date_format(col("order_date"), "yyyyMMdd").cast("int")) \
        .groupBy("date_id", "order_date") \
        .agg(
            _sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("total_amount").alias("avg_order_value")
        )

    print("[*] Calculated Daily Sales Aggregations:")
    daily_sales.show(5)

    # Write to Gold S3 bucket
    print(f"[*] Writing to Gold Lake: {gold_sales_path}...")
    daily_sales.write.mode("overwrite").parquet(gold_sales_path)

    # Metric 2: Customer Aggregation
    customer_metrics = completed_orders \
        .groupBy("user_id") \
        .agg(
            count("order_id").alias("lifetime_orders"),
            _sum("total_amount").alias("lifetime_spend"),
            avg("total_amount").alias("avg_basket_size")
        )

    print("[*] Calculated Customer Metrics:")
    customer_metrics.show(5)
    customer_metrics.write.mode("overwrite").parquet(gold_customer_path)

    print("[+] Silver to Gold transformation finished successfully.")
    spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()
