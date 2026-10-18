# Cloud Mock Data Engineering Lab & Training Environment

A self-contained, enterprise-grade local data engineering environment mocking modern cloud data architectures (AWS/GCP/Azure) with **PostgreSQL, MinIO (S3), Apache Spark (PySpark), DuckDB, and Apache Airflow**.

---

## Architecture Overview

```
+-----------------------------------------------------------------------------------------+
|                               CLOUD MOCK ARCHITECTURE                                   |
+-----------------------------------------------------------------------------------------+

   [ Backend App (OLTP) ]               [ Cloud Object Storage (Data Lake) ]
    PostgreSQL :5432                     MinIO S3 Mock :9000 (API) / :9001 (Console)
    Database: 'oltp_db'      ------>      Bucket: 'bronze' (raw JSON/CSV snapshots)
    (Users, Products, Orders)             Bucket: 'silver' (cleaned, partitioned Parquet)
                                          Bucket: 'gold'   (aggregated business marts)
                                                          |
                                                          v
   [ Data Warehouse (OLAP) ]             [ Distributed Processing Engine ]
    PostgreSQL :5432                     Apache Spark 3.5.1
    Database: 'warehouse_db' <------     - Master :8080 (UI) / :7077 (RPC)
    (Star Schema, Dim/Facts)             - Worker :8081
    + DuckDB In-Process OLAP             - Interactive PySpark Workspace :8888

                                  ^
                                  |
               [ Workflow Orchestrator & Lineage ]
                Apache Airflow 2.9.3
                Webserver :8089 / Scheduler (DAGs, Backfills, Retries)
+-----------------------------------------------------------------------------------------+
```

---

## Service URLs & Default Credentials

| Service | Host URL | Description | Default Credentials |
| :--- | :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8089](http://localhost:8089) | DAG Orchestration & Monitoring | `admin` / `admin` |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | Spark Cluster & Worker State | *No auth required* |
| **Spark Worker UI** | [http://localhost:8081](http://localhost:8081) | Executor & Task Metrics | *No auth required* |
| **MinIO Web Console** | [http://localhost:9001](http://localhost:9001) | S3 Object Storage Explorer | `minioadmin` / `minioadmin` |
| **MinIO S3 Endpoint**| `http://localhost:9000` | S3 API endpoint for SDKs | `minioadmin` / `minioadmin` |
| **JupyterLab Lab** | [http://localhost:8888](http://localhost:8888) | Interactive Notebooks & PySpark | Token: `admin` |
| **PostgreSQL** | `localhost:5432` | OLTP (`oltp_db`), OLAP (`warehouse_db`) | `postgres` / `postgres` |

---

## Quickstart Guide

### 1. Start the Environment
Run all services with Docker Compose:
```bash
docker compose up -d
```

To verify all containers are up and healthy:
```bash
docker compose ps
```

### 2. Run the Mock OLTP Data Generator
Populate the backend database (`oltp_db`) with transactional orders:
```bash
docker exec -it de_workspace python /workspace/scripts/generate_oltp_data.py
```

### 3. Open Airflow & Trigger the Medallion Pipeline
1. Navigate to [http://localhost:8089](http://localhost:8089) in your browser.
2. Sign in with username `admin` and password `admin`.
3. Locate the DAG named `ecommerce_medallion_pipeline`.
4. Toggle it **ON** and click the **Trigger DAG** button.
5. Watch the DAG execute each phase:
   - `verify_oltp_connection`
   - `extract_oltp_to_bronze` -> Writes raw JSON to MinIO `bronze` bucket.
   - `process_bronze_to_silver` -> Deduplicates and partitions into Parquet in `silver`.
   - `aggregate_silver_to_gold` -> Aggregates daily sales in `gold`.
   - `data_quality_assertion` -> Verifies data integrity.

### 4. Query the Lakehouse with DuckDB
Connect to the workspace and run zero-copy SQL directly against the MinIO S3 lakehouse:
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
print(con.execute('SELECT * FROM read_parquet(\"s3://silver/orders/*/*/*.parquet\") LIMIT 5;').df())
"
```

---

## Project Structure

```
.
├── docker-compose.yml              # Complete multi-service stack definition
├── .env                            # Environment variables and ports
├── .env.example                    # Template environment file
├── lectures.md                     # Comprehensive lecture guide & pedagogy handbook
├── module0.md                      # Module 0: The Mindset Shift (Backend vs. Data Engineering)
├── module1.md                      # Module 1: Advanced Analytical SQL & Dimensional Modeling
├── module2.md                      # Module 2: Object Storage, Data Lakes & Storage Formats
├── module3.md                      # Module 3: Distributed Computing with Apache Spark & PySpark
├── module4.md                      # Module 4: Modern Data Warehousing & Medallion Architecture
├── module5.md                      # Module 5: Workflow Orchestration with Apache Airflow
├── module6.md                      # Module 6: End-to-End Capstone Project Guide
├── README.md                       # This quickstart documentation
├── docker/
│   └── Dockerfile.workspace        # Custom PySpark + DuckDB + Jupyter lab image
├── init-scripts/
│   └── 01-init-dbs.sh              # Automatic Postgres multi-database & schema initialization
├── dags/
│   └── ecommerce_medallion_pipeline.py  # End-to-end Airflow Medallion architecture DAG
├── scripts/
│   ├── generate_oltp_data.py       # Simulates transactional e-commerce traffic
│   ├── pyspark_bronze_to_silver.py # Cleans, deduplicates & partitions Parquet
│   └── pyspark_silver_to_gold.py   # Computes business KPIs & dimensional tables
└── notebooks/
    └── 01_pyspark_and_duckdb_deep_dive.ipynb  # Interactive lab notebook
```

---

## Resource Management Tips
For low-spec host machines (e.g., 2GB-4GB RAM):
- Start only the database and MinIO lake:
  ```bash
  docker compose up -d postgres minio minio-setup
  ```
- Or run individual lab components on demand:
  ```bash
  docker compose up -d workspace
  ```
