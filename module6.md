# Module 6: End-to-End Capstone Project — Production Lakehouse Pipeline

---

## 1. Project Overview & Architecture Blueprint

In this capstone project, you will build, execute, monitor, and query a complete, production-grade automated Data Lakehouse pipeline. You will integrate all components covered across Modules 0 through 5 into a unified system running inside our zero-cloud-cost Docker environment.

```
+----------------------------------------------------------------------------------------------------+
|                                    CAPSTONE PIPELINE ARCHITECTURE                                  |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  [ 1. SOURCE: OLTP Application Database ]                                                          |
|   PostgreSQL :5432 (Database: 'oltp_db')                                                           |
|   Tables: users, products, orders, order_items                                                     |
|                                                                                                    |
|                                     |                                                              |
|                  (Batch JSON Snapshot Extraction)                                                  |
|                                     v                                                              |
|  [ 2. STORAGE: MinIO S3 Mock - BRONZE LAYER ]                                                      |
|   Bucket: 'bronze' -> orders/orders_batch_YYYYMMDD.json                                            |
|   (Raw, immutable, append-only JSON audit trail)                                                   |
|                                                                                                    |
|                                     |                                                              |
|                  (PySpark: Validation, Deduplication, Partitioning)                                |
|                                     v                                                              |
|  [ 3. COMPUTE & STORAGE: MinIO S3 Mock - SILVER LAYER ]                                            |
|   Bucket: 'silver' -> orders/year=YYYY/month=MM/*.parquet                                          |
|   (Cleaned, strictly typed, deduplicated columnar Parquet)                                        |
|                                                                                                    |
|                                     |                                                              |
|                  (PySpark: Aggregations, Business KPIs, Star Schema)                               |
|                                     v                                                              |
|  [ 4. CURATED MARTS & WAREHOUSE: GOLD LAYER ]                                                      |
|   - MinIO S3: 'gold' -> daily_sales/*.parquet, customer_metrics/*.parquet                          |
|   - PostgreSQL :5432: 'warehouse_db' (Star Schema: dim_date, dim_user, fact_orders, agg_daily_sales)|
|                                                                                                    |
|                                     ^                                                              |
|                                     |                                                              |
|  [ 5. WORKFLOW ORCHESTRATOR & LINEAGE ]                                                            |
|   Apache Airflow 2.9.3 (:8089 Web UI)                                                              |
|   DAG: ecommerce_medallion_pipeline                                                                |
|                                                                                                    |
|                                     |                                                              |
|                  (Interactive Zero-Copy Vectorized Analytics)                                      |
|                                     v                                                              |
|  [ 6. CONSUMPTION: DuckDB In-Process OLAP Engine ]                                                 |
|   Direct Parquet querying over S3 via httpfs                                                       |
+----------------------------------------------------------------------------------------------------+
```

### Service Map & Credentials

| Service | Endpoint | Web UI URL | Credentials |
| :--- | :--- | :--- | :--- |
| **Apache Airflow** | `localhost:8089` | [http://localhost:8089](http://localhost:8089) | `admin` / `admin` |
| **MinIO S3 Mock** | `localhost:9000` | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Apache Spark** | `spark-master:7077` | [http://localhost:8080](http://localhost:8080) | *No auth* |
| **PostgreSQL** | `localhost:5432` | N/A | `postgres` / `postgres` |
| **JupyterLab** | `localhost:8888` | [http://localhost:8888](http://localhost:8888) | Token: `admin` |

---

## 2. Step-by-Step Lab Execution Guide

### Step 1: Pre-Flight Environment Verification

Ensure all containers in the stack are healthy and running:
```bash
docker compose ps
```

Verify that the three PostgreSQL databases (`oltp_db`, `warehouse_db`, and `airflow`) and MinIO buckets (`bronze`, `silver`, `gold`) are initialized.

---

### Step 2: Seed Operational OLTP Data

Run the transactional data generator script [`scripts/generate_oltp_data.py`](file:///home/ubuntu/agent/scripts/generate_oltp_data.py) inside the workspace container:

```bash
docker exec -it de_workspace python /workspace/scripts/generate_oltp_data.py
```

#### What happens under the hood:
1. Connects to PostgreSQL `oltp_db` on port 5432.
2. Generates mock users across multiple countries (`US`, `CA`, `UK`, `DE`, `FR`, etc.).
3. Generates e-commerce orders with statuses: `COMPLETED`, `SHIPPED`, `PROCESSING`, and `CANCELLED`.
4. Inserts line items into `order_items` linked via foreign keys to `products`.

Verify the records directly in PostgreSQL:
```bash
docker exec -it de_postgres psql -U postgres -d oltp_db -c "SELECT COUNT(*) FROM orders;"
```

---

### Step 3: Trigger & Monitor the Airflow Medallion DAG

1. Open your browser and navigate to the Airflow UI at **[http://localhost:8089](http://localhost:8089)**.
2. Sign in with username `admin` and password `admin`.
3. Locate the DAG named **`ecommerce_medallion_pipeline`**.
4. If paused, toggle the DAG switch to **ON**.
5. Click the **Trigger DAG** button (Play icon on the right).

```
+-----------------------------------------------------------------------------------------+
|                               AIRFLOW DAG EXECUTION TOPOLOGY                            |
+-----------------------------------------------------------------------------------------+
|                                                                                         |
| [pipeline_start]                                                                        |
|        |                                                                                |
|        v                                                                                |
| [verify_oltp_connection]     --> Verifies PostgreSQL connectivity & order row count     |
|        |                                                                                |
|        v                                                                                |
| [extract_oltp_to_bronze]     --> Ingests raw batch JSON into s3://bronze/orders/        |
|        |                                                                                |
|        v                                                                                |
| [process_bronze_to_silver]   --> PySpark: Validates, casts, dedupes, & writes Parquet   |
|        |                                                                                |
|        v                                                                                |
| [aggregate_silver_to_gold]   --> PySpark: Aggregates daily sales & customer LTV metrics |
|        |                                                                                |
|        v                                                                                |
| [data_quality_assertion]     --> Asserts non-empty buckets and schema integrity         |
|        |                                                                                |
|        v                                                                                |
| [pipeline_complete]                                                                     |
+-----------------------------------------------------------------------------------------+
```

6. Click on the DAG run to inspect the **Graph View**, **Task Logs**, and **Gantt Chart**. Verify that all tasks complete with a dark green border (`success`).

---

### Step 4: Inspect Object Storage in MinIO

1. Navigate to the MinIO Web Console at **[http://localhost:9001](http://localhost:9001)**.
2. Sign in with username `minioadmin` and password `minioadmin`.
3. Inspect the three buckets:
   - **`bronze/orders/`**: Contains raw JSON snapshots (e.g. `orders_batch_20261018.json`). Download and view the raw JSON payload.
   - **`silver/orders/`**: Observe the partitioned directory structure: `year=2026/month=10/*.parquet`. Notice how each file is stored as compressed Parquet.
   - **`gold/daily_sales/`**: Contains pre-aggregated business metrics partitioned as Parquet.
   - **`gold/customer_metrics/`**: Contains calculated customer lifetime metrics.

---

### Step 5: Zero-Copy Analytical Queries with DuckDB

Run interactive OLAP queries against the MinIO S3 lakehouse using DuckDB directly inside the workspace container:

```bash
docker exec -it de_workspace python -c "
import duckdb

con = duckdb.connect()
con.execute('''
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
''')

print('\n=== GOLD LAYER: DAILY SALES METRICS ===')
print(con.execute('SELECT * FROM read_parquet(\"s3://gold/daily_sales/*.parquet\") ORDER BY order_date DESC LIMIT 5;').df())

print('\n=== SILVER LAYER: TOP SPENDERS (COLUMN PRUNING IN ACTION) ===')
print(con.execute('''
    SELECT user_id, ROUND(SUM(total_amount), 2) AS total_spent, COUNT(*) AS order_count
    FROM read_parquet(\"s3://silver/orders/*/*/*.parquet\")
    WHERE order_status = 'COMPLETED'
    GROUP BY user_id
    ORDER BY total_spent DESC
    LIMIT 5;
''').df())
"
```

Notice how DuckDB queries the Parquet files stored inside MinIO over HTTP **instantly**, reading only the necessary byte ranges without downloading the entire dataset.

---

## 3. Production Failure Drills & Troubleshooting

To become a resilient Data Engineer, you must know how pipelines behave when things go wrong.

### Drill 1: Handling Late-Arriving & Updated Records
- **Scenario**: An order initially recorded as `PROCESSING` on Monday is updated to `COMPLETED` on Tuesday.
- **Verification**: In [`scripts/pyspark_bronze_to_silver.py`](file:///home/ubuntu/agent/scripts/pyspark_bronze_to_silver.py), observe how the window specification handles this:
  ```python
  window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
  deduped_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1)
  ```
  Even though both records exist in Bronze, Silver retains only the latest `COMPLETED` state!

### Drill 2: Upstream Database Dropout & Airflow Retries
- **Scenario**: PostgreSQL restarts or experiences a network partition while Airflow executes `verify_oltp_connection`.
- **Mechanism**: The DAG configuration in [`dags/ecommerce_medallion_pipeline.py`](file:///home/ubuntu/agent/dags/ecommerce_medallion_pipeline.py) specifies:
  ```python
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
  ```
  Airflow transitions the task state to `up_for_retry`, sleeps for 1 minute, and automatically recovers without engineer intervention.

### Drill 3: Catching Pipeline Failures with Data Quality Assertions
- **Scenario**: The extraction job extracts 0 rows due to a bad SQL filter.
- **Defense**: The `data_quality_assertion` task inspects S3 object counts. If the bucket is empty or rows are missing, it raises an explicit `ValueError`, marking the DAG run as `failed` and preventing downstream corruptions from reaching executive dashboards.

---

## 4. Advanced Capstone Extension Challenges

To test and expand your Data Engineering capabilities:

1. **Challenge 1: SCD Type 2 Customer Dimension Pipeline**
   - Extend the Airflow DAG to extract user profile updates from `oltp_db.users` and populate `warehouse_db.dim_user` using SCD Type 2 logic (`valid_from`, `valid_to`, `is_current`).
2. **Challenge 2: Automated Slack / Email Alerting on Failure**
   - Configure Airflow's `on_failure_callback` to post an alert with error logs to a webhook if any pipeline task fails.
3. **Challenge 3: Upgrade to Open Table Formats (Delta Lake / Apache Iceberg)**
   - Modify the PySpark writer in [`scripts/pyspark_bronze_to_silver.py`](file:///home/ubuntu/agent/scripts/pyspark_bronze_to_silver.py) to write `.format("delta")` or `.format("iceberg")` instead of plain `.parquet()`.
   - Test running an atomic `MERGE INTO` operation and query historical commits via Time Travel!

---

## 5. Capstone Completion Checklist

- [x] Successfully initialized Docker stack (`Postgres`, `MinIO`, `Spark`, `Airflow`, `DuckDB`).
- [x] Generated transactional test data in `oltp_db`.
- [x] Triggered and observed full execution of `ecommerce_medallion_pipeline` in the Airflow UI.
- [x] Inspected raw JSON in MinIO `bronze`, partitioned Parquet in `silver`, and metrics in `gold`.
- [x] Executed zero-copy SQL queries against the lakehouse with DuckDB.
- [x] Understood production failure handling, idempotency, and automated quality assertions.
