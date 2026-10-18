# Master Course: Data Engineering for Backend Engineers
### *From Monoliths & Microservices to Distributed Data Lakes, Warehouses & Orchestration*

---

## Course Overview & Instructor Philosophy

As a backend engineer, you already know API design, database indexing, concurrency, and clean code. However, **Data Engineering (DE) operates under fundamentally different constraints and trade-offs**:

| Dimension | Backend Engineering (OLTP) | Data Engineering (OLAP / Big Data) |
| :--- | :--- | :--- |
| **Primary Metric** | Request Latency (sub-100ms) | Data Throughput (GBs to TBs per second) |
| **Database Architecture** | Row-oriented (PostgreSQL, MySQL) | Columnar (Parquet, ClickHouse, Redshift, Snowflake, BigQuery) |
| **Data Normalization** | 3rd Normal Form (3NF) to eliminate redundancy | Denormalized / Star Schema (Facts & Dimensions) for read efficiency |
| **Processing Paradigm** | Single-node in-memory request-response | Distributed compute (Apache Spark, multi-node clustering) |
| **State Management** | ACID transactions, immediate consistency | Eventual consistency, Idempotent append-only logs, Medallion layers |
| **Scheduling** | Cron jobs, background workers (Celery, BullMQ) | Directed Acyclic Graphs (DAGs) with backfill & state lineage (Airflow) |
| **Failure Handling** | Catch exception & return 500 / retry queue | Deterministic idempotent pipeline reruns over historical partitions |

### How Our Local Cloud Mock Works
This curriculum uses a 100% reproducible, zero-cloud-cost Docker environment that directly mirrors enterprise cloud architectures:

```
[ Backend App (OLTP) ]         [ Cloud Object Storage (Lake) ]      [ Distributed Compute ]       [ Data Warehouse (OLAP) ]
 PostgreSQL (oltp_db)  ----->   MinIO (Mock AWS S3)           ----->  Apache Spark (PySpark) -----> PostgreSQL (warehouse_db)
 (users, orders, items)          - bronze/ (Raw JSON/CSV)              - Driver & Workers            & DuckDB (Embedded OLAP)
                                 - silver/ (Cleaned Parquet)           - Catalyst Optimizer          (Star Schema & Aggregates)
                                 - gold/   (Curated Marts)
                                              ^                                     ^
                                              |                                     |
                                     [ Workflow Orchestration ]                     |
                                     Apache Airflow (DAGs, Sensors, Backfills) -----+
```

| Local Container Mock | Cloud Equivalent (AWS) | Cloud Equivalent (GCP) | Cloud Equivalent (Azure) |
| :--- | :--- | :--- | :--- |
| **MinIO** | Amazon S3 | Google Cloud Storage (GCS) | Azure Data Lake Storage (ADLS Gen2) |
| **Apache Spark** | AWS EMR / Databricks / AWS Glue | Google Cloud Dataproc / Databricks | Azure Synapse / Databricks |
| **PostgreSQL (OLAP) / DuckDB**| Amazon Redshift / Athena | Google BigQuery | Azure Synapse SQL Serverless |
| **Apache Airflow** | AWS MWAA | GCP Cloud Composer | Azure Data Factory / Managed Airflow |

---

## Table of Contents
1. [Module 0: The Mindset Shift (Backend vs. Data Engineering)](#module-0-the-mindset-shift) — [Detailed Guide](file:///home/ubuntu/agent/module0.md)
2. [Module 1: Advanced Analytical SQL & Dimensional Modeling](#module-1-advanced-analytical-sql--dimensional-modeling) — [Detailed Guide](file:///home/ubuntu/agent/module1.md)
3. [Module 2: Object Storage, Data Lakes & Storage Formats (Parquet vs. CSV/JSON)](#module-2-object-storage-data-lakes--storage-formats) — [Detailed Guide](file:///home/ubuntu/agent/module2.md)
4. [Module 3: Distributed Computing with Apache Spark & PySpark](#module-3-distributed-computing-with-apache-spark--pyspark) — [Detailed Guide](file:///home/ubuntu/agent/module3.md)
5. [Module 4: Modern Data Warehousing & Medallion Architecture](#module-4-modern-data-warehousing--medallion-architecture) — [Detailed Guide](file:///home/ubuntu/agent/module4.md)
6. [Module 5: Workflow Orchestration with Apache Airflow (DAGs, Backfills, Idempotency)](#module-5-workflow-orchestration-with-apache-airflow) — [Detailed Guide](file:///home/ubuntu/agent/module5.md)
7. [Module 6: End-to-End Capstone Project](#module-6-end-to-end-capstone-project) — [Detailed Guide](file:///home/ubuntu/agent/module6.md)
8. [Module 7: The Trainer's Pedagogy Playbook](#module-7-the-trainers-pedagogy-playbook)

---

## Module 0: The Mindset Shift

### 1. Learning Objectives
- Contrast OLTP (Online Transaction Processing) with OLAP (Online Analytical Processing).
- Understand why backend microservices fall apart when asked to compute analytics directly.
- Master the concept of storage-compute decoupling.

### 2. The Core Concept Explained
In backend engineering, databases are optimized for **point queries**:
```sql
-- OLTP Query: Read 1 record by primary key (B-Tree index seek, <2ms)
SELECT * FROM users WHERE user_id = 42891;
```
In data engineering, queries scan millions of rows to compute aggregates:
```sql
-- OLAP Query: Scan 100,000,000 orders to calculate quarterly revenue per country
SELECT u.country, SUM(o.total_amount)
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.created_at >= '2026-01-01'
GROUP BY u.country;
```
If you run this on your production Postgres OLTP database:
1. It locks tables or consumes IOPS/RAM, spiking latency for live users.
2. Row-oriented storage forces disk reads of *every single column* (`user_id`, `password_hash`, `address`, `created_at`) even though you only needed `country` and `total_amount`.

### 3. Row-Oriented vs. Columnar Storage
- **Row-Oriented (Postgres/MySQL)**:
  `[Row 1: ID, Name, Country, Amount] | [Row 2: ID, Name, Country, Amount]`
  *Great for:* `INSERT INTO orders VALUES (...)` and single row updates.
  *Terrible for:* `SUM(Amount)` across 50 million rows.
- **Columnar (Parquet/ClickHouse/DuckDB)**:
  `[Country: US, UK, US, DE] | [Amount: 100.5, 25.0, 99.0, 310.2]`
  *Advantage:* To compute `SUM(Amount)`, the query engine reads **only the Amount bytes**. It skips 90% of the disk I/O, and data of the same type compresses dramatically better (Snappy/ZSTD, Dictionary encoding).

---

## Module 1: Advanced Analytical SQL & Dimensional Modeling

### 1. Learning Objectives
- Master Window Functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`, Frame bounds).
- Use Common Table Expressions (CTEs) for readable multi-stage data transformations.
- Design Star Schemas: Distinguish Fact tables from Dimension tables.
- Handle Slowly Changing Dimensions (SCD Type 1 vs SCD Type 2).

### 2. Window Functions: The DE Superpower
Backend engineers often reach for nested loops or multiple queries in Python/Node to calculate running totals or deduplicate records. In SQL, window functions execute this in a single scan.

#### Example: Deduplicating Event Streams (Latest State per Entity)
```sql
-- Connect to Postgres oltp_db or warehouse_db
WITH ranked_orders AS (
    SELECT 
        order_id,
        user_id,
        total_amount,
        order_status,
        updated_at,
        ROW_NUMBER() OVER(
            PARTITION BY order_id 
            ORDER BY updated_at DESC
        ) AS record_version
    FROM orders
)
SELECT * 
FROM ranked_orders 
WHERE record_version = 1;
```

#### Example: Calculating Running Revenue & MoM (Month-over-Month) Growth
```sql
SELECT 
    DATE_TRUNC('month', created_at) AS order_month,
    SUM(total_amount) AS monthly_revenue,
    -- Running cumulative sum over time
    SUM(SUM(total_amount)) OVER (
        ORDER BY DATE_TRUNC('month', created_at)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    -- Previous month revenue for delta comparison
    LAG(SUM(total_amount), 1) OVER (
        ORDER BY DATE_TRUNC('month', created_at)
    ) AS prev_month_revenue
FROM orders
WHERE order_status = 'COMPLETED'
GROUP BY DATE_TRUNC('month', created_at)
ORDER BY order_month;
```

### 3. Dimensional Modeling (Kimball Methodology)
- **Fact Table (`fact_orders`)**: Contains numerical measurements/metrics and foreign keys pointing to dimension tables. Examples: `revenue`, `quantity`, `discount_amount`, `order_timestamp`.
- **Dimension Table (`dim_user`, `dim_product`, `dim_date`)**: Contains context/attributes for filtering and grouping. Examples: `user_name`, `country`, `product_category`, `is_weekend`.
- **Surrogate Keys**: Integer keys generated in the warehouse (e.g. `user_key`) rather than operational natural keys (`user_id`). This allows tracking historical changes when a user updates their profile.

#### Slowly Changing Dimensions (SCD)
- **SCD Type 1**: Overwrite old data. Historical records reflect the newest state only (simple, but history is lost).
- **SCD Type 2**: Maintain full history by adding a new row with `valid_from`, `valid_to`, and `is_current=TRUE`.

---

## Module 2: Object Storage, Data Lakes & Storage Formats

### 1. Learning Objectives
- Understand why Cloud Object Storage (S3 / GCS / MinIO) is the foundational storage tier of big data.
- Analyze the internal structure of Apache Parquet (Row Groups, Column Chunks, Pages, Dictionary Encoding).
- Implement partitioning strategies (`year=YYYY/month=MM`) and avoid the "Small Files Problem".

### 2. Why Object Storage over Filesystems?
Traditional filesystems (NFS, POSIX) require heavy directory metadata locks and fail at petabyte scale. Object Storage:
- Is virtually infinitely scalable and dirt-cheap.
- Decouples storage from compute (you can turn off Spark clusters when not running, while keeping data safe in S3/MinIO).
- Accessed via standard HTTP/S REST APIs (`PUT`, `GET`, `LIST`).

### 3. Hands-on Lab: Inspecting Storage Formats
Let's see how much disk and time Parquet saves compared to JSON/CSV:

```python
# Run inside Jupyter Workspace or Python script
import pandas as pd
import numpy as np

# Generate 500,000 synthetic rows
n = 500_000
df = pd.DataFrame({
    'user_id': np.random.randint(1, 10_000, size=n),
    'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Toys'], size=n),
    'amount': np.random.uniform(5.0, 500.0, size=n).round(2),
    'status': np.random.choice(['COMPLETED', 'PENDING', 'CANCELLED'], size=n)
})

# Save as CSV, JSON, and Parquet
df.to_csv('/workspace/data/benchmark.csv', index=False)
df.to_json('/workspace/data/benchmark.json', orient='records', lines=True)
df.to_parquet('/workspace/data/benchmark.parquet', engine='pyarrow', compression='snappy')
```

**Compare file sizes on disk:**
- `benchmark.json`: ~45 MB
- `benchmark.csv`: ~22 MB
- `benchmark.parquet`: **~3.8 MB (83% - 92% compression ratio!)**

### 4. Partitioning & The "Small Files" Pitfall
- Good partitioning: `s3://lake/orders/year=2026/month=10/` allows query engines to skip 95% of data when scanning a single month (**Partition Pruning**).
- Anti-pattern: Partitioning by high-cardinality keys like `order_id` or `timestamp_to_the_minute`. This generates millions of tiny 2KB files. Spark and S3 will spend 99% of time making HTTP metadata calls instead of reading data!
- **Rule of Thumb**: Aim for partition file sizes between **128 MB and 512 MB**.

---

## Module 3: Distributed Computing with Apache Spark & PySpark

### 1. Learning Objectives
- Understand Spark's distributed architecture: Driver, Cluster Manager, Executors, and Tasks.
- Demystify Lazy Evaluation and the Catalyst Optimizer.
- Master the difference between Narrow transformations (fast) and Wide transformations (shuffles).
- Avoid Out-Of-Memory (OOM) errors and data skew.

### 2. Spark Architecture Demystified
When you run a Python script, it runs on 1 CPU core on 1 machine.
When you run PySpark:
1. **Driver**: The coordinator process running your Python code. It translates your code into a Directed Acyclic Graph (DAG) of stages.
2. **Cluster Manager**: Allocates resources across machines (Spark Standalone, Kubernetes, or YARN).
3. **Executors**: Worker processes running on separate nodes. Each executor holds Java Virtual Machine (JVM) memory divided into **Storage Memory** (caching) and **Execution Memory** (shuffles, joins, aggregations).
4. **Py4J Bridge**: How Python communicates with the underlying Spark JVM.

```
+-------------------------------------------------------+
|                    SPARK DRIVER                       |
|   (Python Script -> SparkSession -> Catalyst DAG)     |
+-------------------------------------------------------+
                           |
       +-------------------+-------------------+
       |                                       |
+-------------------+                   +-------------------+
|  SPARK EXECUTOR 1 |                   |  SPARK EXECUTOR 2 |
|   JVM (Worker)    |                   |   JVM (Worker)    |
| [Task 1] [Task 2] |                   | [Task 3] [Task 4] |
| Partition 0 & 1   |                   | Partition 2 & 3   |
+-------------------+                   +-------------------+
```

### 3. Narrow vs. Wide Transformations & The Shuffle
- **Narrow Transformation**: Each input partition contributes to at most one output partition. No network communication between workers is required!
  - Examples: `df.filter()`, `df.select()`, `df.withColumn()`.
  - Extremely fast.
- **Wide Transformation**: Data from multiple partitions must be grouped and exchanged across the network to different executors.
  - Examples: `df.groupBy()`, `df.join()`, `df.distinct()`, `df.orderBy()`.
  - **This triggers a SHUFFLE**. Shuffles write intermediate data to disk, serialize it, transfer it over the network, and deserialize it. 90% of Spark performance tuning is optimizing shuffles!

### 4. Catalyst Optimizer: Why PySpark is as Fast as Scala
In vanilla Python, `for` loops are slow. In PySpark, when you write:
```python
df.filter(col("total_amount") > 100).select("user_id").groupBy("user_id").count()
```
Spark does **NOT** execute it immediately (**Lazy Evaluation**). Instead, Catalyst analyzes the entire tree:
1. **Predicate Pushdown**: Pushes the `filter(total_amount > 100)` directly to the Parquet reader on S3, reading only matching row groups.
2. **Column Pruning**: Reads only `total_amount` and `user_id`, completely ignoring unused columns.
3. Generates optimized JVM bytecode.

---

## Module 4: Modern Data Warehousing & Medallion Architecture

### 1. Learning Objectives
- Design a scalable Data Lakehouse using the **Medallion Architecture (Bronze -> Silver -> Gold)**.
- Understand the role of Lakehouse table formats (Delta Lake, Apache Iceberg) providing ACID on object storage.
- Build clean analytical marts for business intelligence.

### 2. The Medallion Layers
```
+-----------------------------------------------------------------------------------------+
|                               MEDALLION LAKEHOUSE ARCHITECTURE                          |
+-----------------------------------------------------------------------------------------+
|  BRONZE LAYER (Raw / Landing)                                                           |
|  - Raw, unaltered extract from operational systems (JSON, CSV, CDC events)             |
|  - Append-only, full historical audit trail                                             |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v  (PySpark: Validation, Deduplication, Casting)
+-----------------------------------------------------------------------------------------+
|  SILVER LAYER (Cleaned / Conformed)                                                     |
|  - Validated schemas, null handling, deduplicated records                               |
|  - Stored as optimized, partitioned Parquet                                             |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v  (PySpark / SQL: Aggregations & Star Schema)
+-----------------------------------------------------------------------------------------+
|  GOLD LAYER (Curated / Business Marts)                                                  |
|  - Aggregated KPIs, Star Schema Fact & Dimension tables                                 |
|  - Ready for dashboards, executives, and machine learning                               |
+-----------------------------------------------------------------------------------------+
```

### 3. Hands-on Code: Silver to Gold Processing
Review the included script [`scripts/pyspark_silver_to_gold.py`](file:///home/ubuntu/agent/scripts/pyspark_silver_to_gold.py) to see how daily aggregated sales metrics are computed from raw orders.

---

## Module 5: Workflow Orchestration with Apache Airflow

### 1. Learning Objectives
- Understand why cron and message queues fail as enterprise pipeline orchestrators.
- Master Directed Acyclic Graphs (DAGs) and their topological constraints.
- Implement the cardinal rule of Data Engineering: **Idempotency**.
- Understand Airflow scheduling semantics: `data_interval_start`, `data_interval_end`, and `backfilling`.

### 2. Why Not Cron?
Backend engineers often start by triggering nightly scripts via Cron:
```bash
0 2 * * * python /scripts/nightly_etl.py
```
**Why this breaks in production:**
1. **No Dependency Awareness**: If `extract.py` takes 2 hours instead of 1, `transform.py` fires while data is half-written, corrupting the warehouse.
2. **No Backfill Mechanism**: If the server was down for 4 days, cron runs once when it boots up. It cannot re-run the missing 4 days in chronological sequence.
3. **No Retries with Exponential Backoff**: Transient network blips fail the entire job.
4. **Zero Observability**: You are left grepping through `/var/log/syslog`.

### 3. The Golden Rule of Data Engineering: IDEMPOTENCY
> **A pipeline is idempotent if running it multiple times with the same input produces the exact same result, without duplicates or side effects.**

- **Anti-pattern**: `INSERT INTO fact_orders SELECT ...`
  If the task fails midway or Airflow retries, duplicate rows are created!
- **Idempotent Pattern (Partition Overwrite)**:
  `df.write.mode("overwrite").partitionBy("date").parquet("s3://gold/orders/")`
- **Idempotent SQL Pattern (Upsert / Delete-Insert)**:
  ```sql
  DELETE FROM fact_orders WHERE date_id = 20261018;
  INSERT INTO fact_orders SELECT ... WHERE date_id = 20261018;
  ```

### 4. Anatomy of an Airflow DAG
Review [`dags/ecommerce_medallion_pipeline.py`](file:///home/ubuntu/agent/dags/ecommerce_medallion_pipeline.py):
- **Operators**: Define a single unit of work (`PythonOperator`, `BashOperator`, `SparkSubmitOperator`).
- **Sensors**: Operators that pause until a condition is met (e.g. `S3KeySensor` waiting for a file).
- **XComs**: Cross-Communication mechanism in Airflow.
  > **WARNING FOR BACKEND ENGINEERS**: Never pass dataframes, query results, or large lists through XCom! Airflow stores XComs in its metadata database (Postgres). Passing 50MB of data through XCom will bloat and crash the Airflow database. **Pass metadata only (e.g., S3 paths, row counts)!**

---

## Module 6: End-to-End Capstone Project

### Project Specification
Students will build and verify a complete automated data pipeline from scratch using the local mock environment:

1. **Step 1: Seed OLTP Source Data**
   Run the data generator to simulate e-commerce orders:
   ```bash
   docker exec -it de_workspace python /workspace/scripts/generate_oltp_data.py
   ```
2. **Step 2: Inspect MinIO S3 Mock**
   Open `http://localhost:9001` (user: `minioadmin`, pass: `minioadmin`). Observe buckets: `bronze`, `silver`, `gold`.
3. **Step 3: Trigger the Airflow Medallion DAG**
   - Access Airflow at `http://localhost:8089` (user: `admin`, pass: `admin`).
   - Unpause and trigger `ecommerce_medallion_pipeline`.
   - Inspect the Graph View, Task Logs, and Gantt Chart.
4. **Step 4: Query the Gold Layer with DuckDB**
   Run interactive zero-copy OLAP queries against Parquet stored directly on MinIO:
   ```python
   import duckdb
   con = duckdb.connect()
   con.execute("""
       INSTALL httpfs; LOAD httpfs;
       SET s3_endpoint='minio:9000';
       SET s3_access_key_id='minioadmin';
       SET s3_secret_access_key='minioadmin';
       SET s3_use_ssl=false;
       SET s3_url_style='path';
   """)
   res = con.execute("SELECT * FROM read_parquet('s3://gold/daily_sales/*.parquet');").df()
   print(res)
   ```

---

## Module 7: The Trainer's Pedagogy Playbook
*(Instructions for conducting workshops and training sessions)*

### 1. The 5-Step Teaching Framework
When introducing any Data Engineering tool to backend engineers, follow this progression:
1. **The Backend Instinct**: "How would you solve this in Node/Spring/Django?" (e.g. Write a loop, fetch rows, process in memory).
2. **The Breaking Point**: "What happens when input data scales from 10,000 rows to 100,000,000 rows?" (OOM, database locks, multi-hour runtime).
3. **The Data Engineering Paradigm**: Introduce the distributed or columnar concept (e.g., Spark partitioning, Parquet dictionary encoding).
4. **Live Failure & Fix Demo**: Show an OOM in Spark or duplicate data in SQL, then apply the fix live.
5. **Hands-on Student Lab**: Let students run the code in the Docker workspace and inspect metrics.

### 2. Common Student Traps & How to Coach Them
- **Trap 1: Converting PySpark DataFrames to Pandas too early (`df.toPandas()`)**:
  - *Symptom*: Student writes `spark_df.toPandas().groupby(...)`. The driver node crashes with an OOM.
  - *Teaching Tip*: Remind them that `toPandas()` pulls the *entire distributed dataset* across the network into the single driver memory. PySpark has native `df.groupBy()`!
- **Trap 2: Misunderstanding Airflow Execution Date**:
  - *Symptom*: Student expects a daily DAG scheduled for `@daily` to run immediately at the start of the day.
  - *Teaching Tip*: Airflow runs a pipeline *after* the data interval has closed. A pipeline for `2026-10-18` runs on `2026-10-19 00:00:00` because you cannot process the day's data until the day is over!
- **Trap 3: Not setting `spark.sql.shuffle.partitions`**:
  - *Symptom*: A 1,000-row DataFrame operation runs 200 tasks and takes 30 seconds.
  - *Teaching Tip*: Spark defaults to 200 shuffle partitions regardless of data size. In small/local environments, always set `spark.sql.shuffle.partitions=4`.

### 3. Suggested Workshop Schedules
- **Fast-Track Intensive (3 Days)**:
  - Day 1: Advanced SQL, Dimensional Modeling, Parquet & MinIO.
  - Day 2: PySpark architecture, DataFrames, shuffles, and joins.
  - Day 3: Airflow orchestration, Medallion Architecture, Capstone project.
- **Comprehensive Semester / Corporate Training (6 Weeks)**:
  - Week 1: OLTP vs. OLAP & Relational DB tuning.
  - Week 2: Dimensional Modeling, Star/Snowflake schemas, SCDs.
  - Week 3: Object storage, Parquet internals, compression & partitioning.
  - Week 4: Apache Spark core, Catalyst Optimizer, and PySpark transformations.
  - Week 5: Apache Airflow DAG design, idempotency, sensors, and backfills.
  - Week 6: Production Data Quality, Observability, and Capstone Delivery.
