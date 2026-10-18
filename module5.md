# Module 5: Workflow Orchestration with Apache Airflow (DAGs, Backfills, Idempotency)

---

## 1. Learning Objectives

By the end of this module, you will be able to:
1. Explain why **Cron** and **Message Queues** fail as enterprise pipeline orchestrators.
2. Construct **Directed Acyclic Graphs (DAGs)** adhering to topological constraints.
3. Enforce the cardinal rule of Data Engineering: **Pipeline Idempotency**.
4. Demystify Airflow scheduling semantics: `data_interval_start`, `data_interval_end`, and **Backfilling**.
5. Use **XComs** safely without bloating or crashing Airflow's metadata database.
6. Build and monitor production DAGs, referencing [`dags/ecommerce_medallion_pipeline.py`](file:///home/ubuntu/agent/dags/ecommerce_medallion_pipeline.py).

---

## 2. Why Not Cron? Why Not Message Queues?

In backend web development, background scheduling is often handled with Linux Cron or message workers (Celery, BullMQ, Sidekiq):
```bash
# Typical backend engineer's attempt at data pipelines:
0 2 * * * /usr/bin/python3 /scripts/extract.py
0 3 * * * /usr/bin/python3 /scripts/transform.py
0 4 * * * /usr/bin/python3 /scripts/load.py
```

### The 4 Structural Failures of Cron in Data Engineering

```
+----------------------------------------------------------------------------------------------------+
|                                    WHY CRON COLLAPSES IN PRODUCTION                                |
+----------------------------------------------------------------------------------------------------+

1. NO DEPENDENCY AWARENESS
   - Cron relies on time guesses (e.g. "extract will finish in 1 hour").
   - If upstream extract takes 65 minutes due to high data volume, transform.py fires while data is
     half-written, corrupting downstream warehouse tables!

2. ZERO BACKFILL CAPABILITY
   - If the database was down for 5 days over a holiday, Cron runs once when restarted.
   - It cannot systematically replay missing days (Day 1 -> Day 2 -> Day 3) in chronological order!

3. NO RETRY POLICIES WITH EXPONENTIAL BACKOFF
   - If S3 has a 503 transient blip, Cron fails immediately. The entire daily pipeline dies until
     an engineer manually logs into SSH to re-run it.

4. ZERO LINEAGE & OBSERVABILITY
   - There is no central dashboard, no SLA alerting, and no visual DAG dependency view.
   - Debugging requires SSHing into instances and grepping through /var/log/syslog.
+----------------------------------------------------------------------------------------------------+
```

### Orchestrator (Airflow) vs. Message Queue (RabbitMQ / Kafka / Celery)

| Dimension | Message Queue / Stream (Kafka, RabbitMQ) | Workflow Orchestrator (Airflow) |
| :--- | :--- | :--- |
| **Trigger Mechanism** | Event-driven (push immediately per record) | Time-interval or dependency-driven (batch) |
| **Scope** | Moving individual messages from A to B | Coordinating complex multi-system workflows (Spark, S3, Postgres) |
| **State Tracking** | Ephemeral (ack/nack) | Persistent, auditable execution state across years of historical runs |
| **Handling Failures** | Dead-letter queues (DLQ) | Task-level retry with exponential backoff and downstream halting |

---

## 3. The Cardinal Rule of Data Engineering: IDEMPOTENCY

> [!IMPORTANT]
> **Mathematical Definition**: An operation $f(x)$ is **idempotent** if applying it multiple times produces the exact same result as applying it once:
> $$f(f(x)) = f(x)$$

In data engineering: **A pipeline is idempotent if running it multiple times for the same time interval produces the exact same state, without creating duplicate rows, losing data, or causing side effects.**

```
+----------------------------------------------------------------------------------------------------+
|                                      IDEMPOTENCY: ANTI-PATTERN vs. BEST PRACTICE                   |
+----------------------------------------------------------------------------------------------------+

ANTI-PATTERN: Non-Idempotent Appending
---------------------------------------
INSERT INTO fact_orders SELECT * FROM staging_orders;
* If the task fails on network timeout after inserting 10,000 rows, Airflow retries the task.
* The retry inserts another 10,000 rows! Your revenue metrics are now doubled and corrupt!

BEST PRACTICE 1: Partition Overwrites (Data Lakehouse)
------------------------------------------------------
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("s3a://silver/orders/")
* Running this 10 times overwrites the exact partition. Zero duplicate records!

BEST PRACTICE 2: Delete-Insert (Data Warehouse)
-----------------------------------------------
BEGIN TRANSACTION;
  DELETE FROM fact_orders WHERE date_id = 20261018;
  INSERT INTO fact_orders SELECT ... WHERE date_id = 20261018;
COMMIT;
* Guaranteed atomic replacement of the partition slice.
+----------------------------------------------------------------------------------------------------+
```

### The "Current Timestamp" Anti-Pattern

Never write SQL or Python scripts that rely on the system clock:
```sql
-- DANGEROUS ANTI-PATTERN:
SELECT * FROM orders WHERE created_at >= NOW() - INTERVAL '1 day';
```
If you backfill this query next month to reprocess October 18th data, `NOW()` will evaluate to November! The backfill will pull the wrong data.

**Always use Airflow's templated execution parameters:**
```sql
-- IDEMPOTENT BEST PRACTICE:
SELECT * FROM orders 
WHERE created_at >= '{{ data_interval_start }}' 
  AND created_at <  '{{ data_interval_end }}';
```

---

## 4. Airflow Architecture & Core Concepts

Apache Airflow is composed of four decoupled components:

```
                                  [ AIRFLOW WEB SERVER ] (:8089)
                                  (Inspect DAGs, Trigger, Monitor Logs)
                                            ^
                                            |
[ AIRFLOW SCHEDULER ] <------------> [ METADATA DATABASE ] (PostgreSQL: airflow)
(Parses DAGs, monitors schedules,     (Stores DAG definitions, run history,
 dispatches tasks to Executor)         task states, and XComs)
         |
         v
[ AIRFLOW EXECUTOR ] (LocalExecutor / Celery / Kubernetes)
         |
         +-------------------+-------------------+
         |                                       |
         v                                       v
[ WORKER PROCESS 1 ]                    [ WORKER PROCESS 2 ]
(Executes Task Instances)               (Executes Task Instances)
```

### Core Terminology

- **DAG (Directed Acyclic Graph)**: A collection of tasks organized with explicit directional dependencies. It cannot contain circular loops (`A -> B -> A` is strictly prohibited).
- **Operator**: A template for a single unit of work:
  - `PythonOperator`: Runs a Python function.
  - `BashOperator`: Executes shell commands.
  - `SparkSubmitOperator`: Submits distributed Spark jobs to a cluster.
- **Sensor**: A special operator that pauses execution and polls until an external event occurs (e.g. `S3KeySensor` waiting for a file to appear in MinIO).
  - *Best Practice*: Always set `mode="reschedule"` instead of `mode="poke"` on long-running sensors so the worker thread is freed while waiting.
- **Task**: An operator instantiated inside a DAG.
- **Task Instance**: A task evaluated for a specific execution date/interval (state: `queued`, `running`, `success`, `failed`, `up_for_retry`).

---

## 5. Airflow Scheduling Semantics: The Execution Date Paradox

The most common point of confusion for backend engineers is Airflow's scheduling interval:

```
Suppose a DAG is configured with:
schedule_interval = '@daily'  (Runs once every 24 hours)
start_date = 2026-10-01 00:00:00

QUESTION: When does the first run for 2026-10-01 actually execute?
ANSWER:   It executes on 2026-10-02 00:00:00!
```

### Why Does Airflow Wait Until the Period Has Passed?
In data engineering, a daily batch job processes data for the entire day. You cannot process all orders for October 1st until October 1st has completely finished (`2026-10-01 23:59:59`). Therefore, the pipeline triggers at the start of October 2nd.

```
+-----------------------------------------------------------------------------------------+
|                              AIRFLOW TIME INTERVAL SEMANTICS                            |
+-----------------------------------------------------------------------------------------+
|                                                                                         |
|  2026-10-01 00:00:00                                             2026-10-02 00:00:00    |
|  |------------------------- DATA INTERVAL -------------------------|                   |
|  ^                                                                 ^                    |
|  data_interval_start                                               data_interval_end    |
|  (logical_date / execution_date)                                   (EXECUTION TRIGGER!) |
|                                                                                         |
+-----------------------------------------------------------------------------------------+
```

### Backfilling: Replaying Historical Pipelines

If you need to process historical data from two months ago, you run Airflow's backfill CLI:
```bash
airflow dags backfill \
    --start-date 2026-08-01 \
    --end-date 2026-08-31 \
    ecommerce_medallion_pipeline
```
Airflow will generate 31 DAG runs, executing each day's interval sequentially with deterministic idempotency.

---

## 6. XComs: Best Practices & The Fatal Anti-Pattern

**XCom (Cross-Communication)** allows tasks to share small metadata values.

### The Fatal Anti-Pattern: Passing DataFrames Through XCom
```python
# DISASTROUS ANTI-PATTERN FOR BACKEND ENGINEERS:
def extract_data(**context):
    df = pd.read_sql("SELECT * FROM orders", conn)
    # Storing 500,000 rows in XCom:
    context['ti'].xcom_push(key='orders_df', value=df.to_json())
```
- **Why it breaks**: Airflow serializes XCom payloads directly into the PostgreSQL metadata database (`airflow` DB). Storing 50MB DataFrames in XCom bloats the metadata DB, saturates RAM, and causes the Airflow Scheduler to crash with heartbeat timeouts.

### The Best Practice: Pass Pointers, Not Payloads
```python
# CORRECT PATTERN:
def extract_data(**context):
    s3_uri = "s3://bronze/orders/orders_batch_20261018.json"
    # Data is written directly to S3; only the lightweight URI is passed:
    context['ti'].xcom_push(key='bronze_s3_path', value=s3_uri)

def transform_data(**context):
    # Downstream task reads the pointer and pulls from S3:
    s3_path = context['ti'].xcom_pull(key='bronze_s3_path', task_ids='extract_data')
    spark.read.json(s3_path)
```

---

## 7. Deep Dive: `ecommerce_medallion_pipeline.py`

Let's inspect our production DAG in [`dags/ecommerce_medallion_pipeline.py`](file:///home/ubuntu/agent/dags/ecommerce_medallion_pipeline.py):

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ecommerce_medallion_pipeline',
    default_args=default_args,
    description='Automated Medallion Data Lakehouse Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'spark', 'lakehouse', 'ecommerce'],
) as dag:

    # 1. Pipeline Sentinel Start
    start_node = EmptyOperator(task_id='pipeline_start')

    # 2. Pre-flight health check
    check_connection = PythonOperator(
        task_id='verify_oltp_connection',
        python_callable=test_oltp_connection,
    )

    # 3. Extract OLTP orders to Bronze S3 bucket
    extract_to_bronze = PythonOperator(
        task_id='extract_oltp_to_bronze',
        python_callable=extract_oltp_to_bronze_task,
        provide_context=True,
    )

    # 4. PySpark cleaning, deduplication, and Parquet partitioning to Silver
    bronze_to_silver = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=transform_bronze_to_silver_task,
        provide_context=True,
    )

    # 5. PySpark aggregation and Gold analytical marts sink
    silver_to_gold = PythonOperator(
        task_id='aggregate_silver_to_gold',
        python_callable=transform_silver_to_gold_task,
        provide_context=True,
    )

    # 6. Data quality integrity assertions
    quality_check = PythonOperator(
        task_id='data_quality_assertion',
        python_callable=data_quality_check_task,
    )

    # 7. Pipeline Sentinel Complete
    end_node = EmptyOperator(task_id='pipeline_complete')

    # Define strict topological execution order:
    start_node >> check_connection >> extract_to_bronze >> bronze_to_silver >> silver_to_gold >> quality_check >> end_node
```

---

## 8. Summary Checklist for Backend Engineers

- [x] Replace brittle Cron jobs with Airflow DAGs for dependency resolution, backfilling, and observability.
- [x] Ensure every task is **idempotent**: running it repeatedly must yield identical results without duplicate rows.
- [x] Never use `NOW()` in data queries; use `data_interval_start` and `data_interval_end`.
- [x] Remember that a `@daily` DAG for date $T$ executes at the end of the interval ($T + 1$).
- [x] Use XCom only for lightweight metadata (file paths, row counts); never pass DataFrames.
- [x] Implement automated **Data Quality Assertions** as an explicit DAG stage before reporting completion.
