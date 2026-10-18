# Module 4: Modern Data Warehousing & Medallion Architecture

---

## 1. Learning Objectives

By the end of this module, you will be able to:
1. Explain the evolution of data architecture from **Data Warehouses** to **Data Lakes** to **Modern Data Lakehouses**.
2. Design and implement the three tiers of the **Medallion Architecture (Bronze -> Silver -> Gold)**.
3. Understand the necessity of Lakehouse table formats (**Delta Lake**, **Apache Iceberg**, **Apache Hudi**) for ACID transactions on object storage.
4. Deploy **DuckDB** as a high-performance, embedded vectorized OLAP engine for zero-copy Parquet analytics.
5. Dissect the PySpark aggregation and sink script in [`scripts/pyspark_silver_to_gold.py`](file:///home/ubuntu/agent/scripts/pyspark_silver_to_gold.py).

---

## 2. The Evolution of Data Architecture

```
+----------------------------------------------------------------------------------------------------+
|                               THE THREE GENERATIONS OF DATA ARCHITECTURES                          |
+----------------------------------------------------------------------------------------------------+

1. GENERATION 1: Traditional Data Warehouse (1990s - 2010s)
   [ Operational DBs ] ---> [ Heavy ETL ] ---> [ Proprietary RDBMS Warehouse (Teradata, Oracle) ]
   * Pros: Fast SQL, strict ACID, reliable schemas.
   * Cons: Extremely expensive, tightly coupled storage and compute, cannot store unstructured data.

------------------------------------------------------------------------------------------------------

2. GENERATION 2: Data Lake (2010s - 2020)
   [ Operational DBs / Logs ] ---> [ Apache Kafka ] ---> [ Cheap Object Storage (Hadoop HDFS / S3) ]
   * Pros: Decoupled storage and compute, dirt cheap, stores raw JSON, CSV, logs, images.
   * Cons: "Data Swamp" problem: No ACID guarantees, dirty reads, no schema enforcement, no updates.

------------------------------------------------------------------------------------------------------

3. GENERATION 3: Modern Data Lakehouse (Current Standard)
   [ Raw Ingestion ] ---> [ Object Storage (S3/MinIO) + ACID Table Formats (Iceberg/Delta) ]
                          - Bronze: Raw immutable append-only logs
                          - Silver: Cleaned, deduplicated, partitioned Parquet
                          - Gold: Curated Star Schemas & Business Marts
   * Combines the cheap, decoupled storage of Data Lakes with the ACID reliability and SQL speed of Warehouses!
+----------------------------------------------------------------------------------------------------+
```

---

## 3. The Medallion Lakehouse Architecture in Detail

The Medallion pattern organizes data into three distinct stages of increasing quality and refinement:

```
+-----------------------------------------------------------------------------------------+
|                               MEDALLION LAKEHOUSE ARCHITECTURE                          |
+-----------------------------------------------------------------------------------------+
|  BRONZE LAYER (Raw / Landing Zone)                                                      |
|  - Storage: s3://bronze/orders/orders_batch_YYYYMMDD.json                               |
|  - Characteristics: Raw, unaltered extracts from source DBs, CDC logs, third-party APIs |
|  - Schema: Schema-on-read, append-only, immutable historical audit trail                |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v  (PySpark: Validate, Cast, Deduplicate)
+-----------------------------------------------------------------------------------------+
|  SILVER LAYER (Cleaned / Conformed Enterprise Data)                                     |
|  - Storage: s3://silver/orders/year=YYYY/month=MM/*.parquet                             |
|  - Characteristics: Filtered bad records, strictly typed schemas, deduplicated entities  |
|  - Target Audience: Data Scientists, ML feature pipelines, ad-hoc exploratory queries    |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v  (PySpark / SQL: Aggregations & Star Schema)
+-----------------------------------------------------------------------------------------+
|  GOLD LAYER (Curated / Business Intelligence Marts)                                     |
|  - Storage: s3://gold/daily_sales/*.parquet & PostgreSQL warehouse_db (agg_daily_sales) |
|  - Characteristics: Denormalized Star Schema (Facts/Dims), pre-computed business KPIs    |
|  - Target Audience: Executive dashboards (Tableau, Looker, Superset), business analysts  |
+-----------------------------------------------------------------------------------------+
```

### Layer Comparison Matrix

| Attribute | Bronze (Raw) | Silver (Conformed) | Gold (Curated) |
| :--- | :--- | :--- | :--- |
| **Data Format** | JSON, CSV, Kafka CDC | Partitioned Apache Parquet | Parquet / RDBMS Tables |
| **Data Cleanliness**| Raw, unaltered, may contain errors | Cleaned, validated, nulls handled | Aggregated, business-ready |
| **Duplicates** | Contains all updates/duplicates | Deduplicated (latest valid state) | Aggregated unique dimensions |
| **Update Pattern** | Strictly Append-Only | Partition Overwrite / MERGE | Overwrite / Star Schema Upsert |
| **Primary Users** | Data Engineers | Data Scientists & Engineers | Business Analysts & Leadership |

---

## 4. Lakehouse Table Formats: Delta Lake & Apache Iceberg

Backend engineers often ask: *"If Parquet is so fast, why can't we just write Parquet files to S3 and call it a Data Warehouse?"*

### The Limitations of Raw Parquet on S3:
1. **Lack of Atomicity**: If a Spark job writing 100 Parquet files fails on file 67 due to a network glitch, 66 files remain on S3. Downstream queries now read partial, corrupt data!
2. **Dirty Reads**: S3 does not have read locks. If a query runs while a 20-minute batch write is in progress, it reads an inconsistent blend of old and new files.
3. **Expensive Updates (`UPDATE / DELETE`)**: To update a single user's email in a 200MB Parquet file, you must rewrite the entire 200MB file.
4. **No Historical Rollbacks**: If bad data is loaded, there is no native way to roll back to the state 1 hour ago.

### How Open Table Formats Fix This

Table formats like **Delta Lake** and **Apache Iceberg** add an **ACID Transaction Log** alongside the Parquet data files:

```
s3://lake/orders_table/
├── _delta_log/
│   ├── 00000000000000000000.json  (Commit 0: Added part-01.parquet, part-02.parquet)
│   ├── 00000000000000000001.json  (Commit 1: Added part-03.parquet, Removed part-01.parquet)
├── part-01.parquet                (Marked logically deleted in Commit 1)
├── part-02.parquet                (Active)
└── part-03.parquet                (Active)
```

- **Atomic Commits**: Readers only see files explicitly committed in the transaction log. A failed write is simply not committed and never seen by readers.
- **Time Travel**: You can query exact historical states:
  ```sql
  SELECT * FROM orders VERSION AS OF 1;
  SELECT * FROM orders TIMESTAMP AS OF '2026-10-18 10:00:00';
  ```
- **ACID Upserts (`MERGE INTO`)**:
  ```sql
  MERGE INTO silver_orders target
  USING incoming_orders source
  ON target.order_id = source.order_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```

---

## 5. DuckDB: The Modern In-Process OLAP Engine

While Apache Spark is the standard for distributed data processing across multi-node clusters, **DuckDB** has emerged as the premier single-node analytical engine (often dubbed the *"SQLite for Analytics"*).

```
+----------------------------------------------------------------------------------------------------+
|                                      SPARK vs. DUCKDB COMPARISON                                   |
+----------------------------------------------------------------------------------------------------+
| Dimension                | Apache Spark                            | DuckDB                        |
+--------------------------+-----------------------------------------+-------------------------------+
| Architecture             | Distributed Multi-Node Cluster (JVM)   | Embedded In-Process (C++/Python)|
| Infrastructure Footprint | Heavy (Master, Workers, ZooKeeper/K8s)  | Zero (Single pip install)     |
| Optimal Data Scale       | 100 GB to Petabytes                     | 1 MB to 200 GB                |
| Startup / Query Latency  | 5 to 30 seconds (cluster scheduling)   | Sub-millisecond               |
| Memory Efficiency        | High JVM overhead and GC pauses        | Native Vectorized execution   |
| Primary DE Role          | Heavy ETL, Medallion batch pipelines    | Ad-hoc analytics, local tests |
+----------------------------------------------------------------------------------------------------+
```

### Zero-Copy S3 Analytics with DuckDB

DuckDB includes a native `httpfs` extension that allows running SQL queries directly against Parquet files stored in S3/MinIO without downloading the entire file:

```python
import duckdb

con = duckdb.connect()

# Configure DuckDB to communicate with our local MinIO mock
con.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Execute instant OLAP query directly against Parquet in MinIO S3
df = con.execute("""
    SELECT 
        order_status,
        COUNT(*) AS order_count,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM read_parquet('s3://silver/orders/*/*/*.parquet')
    GROUP BY order_status;
""").df()

print(df)
```

DuckDB reads the Parquet footer from MinIO via an HTTP Range request, fetches only the `order_status` and `total_amount` columns, and computes the aggregation in vectorized C++ memory in milliseconds.

---

## 6. Hands-on Code: Silver to Gold Processing

Review the pipeline script [`scripts/pyspark_silver_to_gold.py`](file:///home/ubuntu/agent/scripts/pyspark_silver_to_gold.py):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, to_date, date_format

spark = SparkSession.builder \
    .appName("SilverToGoldAggregation") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 1. Read cleaned Silver Parquet data
silver_df = spark.read.parquet("s3a://silver/orders/")

# 2. Filter completed orders for revenue metrics
completed_orders = silver_df.filter(col("order_status") == "COMPLETED")

# 3. Gold Mart 1: Daily Sales Performance
daily_sales = completed_orders \
    .withColumn("order_date", to_date(col("created_at"))) \
    .withColumn("date_id", date_format(col("order_date"), "yyyyMMdd").cast("int")) \
    .groupBy("date_id", "order_date") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("total_amount").alias("avg_order_value")
    )

# 4. Sinks: Write to Gold S3 bucket as Parquet
daily_sales.write.mode("overwrite").parquet("s3a://gold/daily_sales/")

# 5. Gold Mart 2: Customer Lifetime Value (LTV) Metrics
customer_metrics = completed_orders \
    .groupBy("user_id") \
    .agg(
        count("order_id").alias("lifetime_orders"),
        _sum("total_amount").alias("lifetime_spend"),
        avg("total_amount").alias("avg_basket_size")
    )

customer_metrics.write.mode("overwrite").parquet("s3a://gold/customer_metrics/")
```

---

## 7. Summary Checklist for Backend Engineers

- [x] Structure Lakehouses using the **Medallion Architecture (Bronze -> Silver -> Gold)**.
- [x] Ingest raw source data untouched into **Bronze** to allow pipeline reprocessing.
- [x] Use **Silver** for schema validation, casting, deduplication, and columnar Parquet storage.
- [x] Pre-aggregate business metrics and dimensional models in the **Gold** layer.
- [x] Understand how open table formats (Delta Lake, Apache Iceberg) bring ACID transactions and time travel to object storage.
- [x] Use **DuckDB** for ultra-fast, zero-copy local analytics against cloud Parquet files.
