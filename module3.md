# Module 3: Distributed Computing with Apache Spark & PySpark

---

## 1. Learning Objectives

By the end of this module, you will be able to:
1. Explain the distributed mechanics of Apache Spark: **Driver**, **Cluster Manager**, **Executors**, and **Tasks**.
2. Demystify **Lazy Evaluation** and the internal phases of the **Catalyst Optimizer** (`explain(True)`).
3. Differentiate between **Narrow transformations** (zero-network pipelining) and **Wide transformations** (expensive network shuffles).
4. Select optimal join strategies: **Broadcast Hash Join (BHJ)** vs. **Sort-Merge Join (SMJ)**.
5. Diagnose and prevent the two most common Spark production failures: **Driver/Executor OOMs** and **Data Skew**.
6. Master the PySpark transformation pipeline implemented in [`scripts/pyspark_bronze_to_silver.py`](file:///home/ubuntu/agent/scripts/pyspark_bronze_to_silver.py).

---

## 2. Distributed Architecture Demystified

When you run a standard Python script using Pandas or SQLAlchemy, it runs as a single OS process bound to a single machine's RAM and CPU core. When data exceeds available RAM, the process crashes with an Out-of-Memory (OOM) error.

Apache Spark is a distributed computing engine that coordinates memory and CPU cores across an entire cluster of virtual machines.

```
+----------------------------------------------------------------------------------------------------+
|                                    SPARK CLUSTER ARCHITECTURE                                      |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  [ SPARK DRIVER PROCESS ]                                                                          |
|  - Runs the Python application code (creates SparkSession)                                         |
|  - Analyzes DataFrame transformations and builds the Catalyst DAG                                  |
|  - Breaks the DAG into Stages (at shuffle boundaries) and Tasks                                    |
|  - Dispatches Tasks to Executors and monitors heartbeats                                           |
|                                                                                                    |
|                                     |                                                              |
|                     (Resource Requests & Allocation)                                       |
|                                     v                                                              |
|  [ CLUSTER MANAGER ] (Spark Standalone :7077 / Kubernetes / Apache YARN)                            |
|                                     |                                                              |
|        +----------------------------+----------------------------+                                 |
|        |                                                         |                                 |
|        v                                                         v                                 |
|  [ SPARK WORKER NODE 1 ]                                  [ SPARK WORKER NODE 2 ]                  |
|  +---------------------------------------+                +---------------------------------------+|
|  | SPARK EXECUTOR (JVM Process)           |                | SPARK EXECUTOR (JVM Process)           ||
|  | +-----------------------------------+ |                | +-----------------------------------+ ||
|  | | Executor JVM Memory               | |                | | Executor JVM Memory               | ||
|  | | [Execution Memory] [Storage Memory]| |                | | [Execution Memory] [Storage Memory]| ||
|  | +-----------------------------------+ |                | +-----------------------------------+ ||
|  |  Task 1 (Part 0)   Task 2 (Part 1)    |                |  Task 3 (Part 2)   Task 4 (Part 3)    ||
|  |  (Thread on Core)  (Thread on Core)   |                |  (Thread on Core)  (Thread on Core)   ||
|  +---------------------------------------+                +---------------------------------------+|
+----------------------------------------------------------------------------------------------------+
```

### The PySpark Architecture: Py4J Bridge vs JVM Execution

Backend engineers often wonder: *"Isn't Python notoriously slow for big data processing?"*

In PySpark, **your Python code does not process the data rows**. 
1. When you write `df.filter(...).groupBy(...)`, the Python process communicates with a local JVM process via the **Py4J bridge** to build the query plan.
2. Once an action (like `.write.parquet()`) is called, the plan is compiled into highly optimized Java bytecode using Spark's **Tungsten engine**.
3. All data parsing, filtering, aggregation, and S3 communication happen directly inside the high-performance JVM executors in C++/Java memory off the Python GIL!

> [!WARNING]
> **The Python UDF Performance Trap**: If you write custom Python functions using `@udf`, Spark is forced to serialize data from the JVM, pipe it over Unix sockets to a Python worker process, run the Python function row-by-row, and serialize it back to the JVM. This causes a **10x to 50x performance degradation**. Always use built-in functions from `pyspark.sql.functions`!

---

## 3. Lazy Evaluation & The Catalyst Optimizer

In PySpark, transformations are strictly **Lazy**. 

```python
# Nothing executes here! Spark merely appends rules to a logical query tree.
orders = spark.read.json("s3a://bronze/orders/")
filtered = orders.filter(col("total_amount") > 100)
projected = filtered.select("order_id", "user_id", "total_amount")
```

Execution is triggered **only** when an **Action** is invoked:
- `.count()`
- `.show()`
- `.collect()`
- `.write.parquet(...)`

### Why Lazy Evaluation Enables High Performance

Because Spark knows the entire processing chain before executing a single byte, the **Catalyst Optimizer** restructures your operations for maximum efficiency:

```
[ User Code (Python/SQL) ]
            |
            v
1. Unresolved Logical Plan  --> Verifies syntax
            |
            v
2. Analyzed Logical Plan    --> Resolves column names and table types via Catalog
            |
            v
3. Optimized Logical Plan   --> Applies rule-based optimizations:
                                - Predicate Pushdown (pushes filter down to S3 Parquet read)
                                - Column Pruning (drops unused columns immediately)
                                - Constant Folding & Boolean Simplification
            |
            v
4. Physical Plans (CBO)     --> Evaluates join strategies (Broadcast vs Sort-Merge)
            |
            v
5. Whole-Stage Code Gen     --> Emits optimized JVM Bytecode (Java 64-bit primitive loops)
```

You can view the exact optimization plan at any time by running:
```python
projected.explain(True)
```

---

## 4. Narrow vs. Wide Transformations & The Shuffle

Understanding transformations is the single most important concept in Spark performance engineering.

```
+----------------------------------------------------------------------------------------------------+
|                                  TRANSFORMATION DEPENDENCY TYPES                                    |
+----------------------------------------------------------------------------------------------------+

1. NARROW TRANSFORMATIONS (Zero Network Transfer - Pipeline Execution)
   Input Partition 0  ------> [Task 1: filter + select] ------> Output Partition 0
   Input Partition 1  ------> [Task 2: filter + select] ------> Output Partition 1
   * Fast, purely in-memory, executed concurrently on local executor threads.
   * Functions: filter(), select(), withColumn(), drop(), cast()

------------------------------------------------------------------------------------------------------

2. WIDE TRANSFORMATIONS (Requires a Distributed Network SHUFFLE)
   Input Partition 0 \       /==== Network Exchange ====>\   Reducer Task 0 (Key A)
   Input Partition 1 ----====                             ==== Reducer Task 1 (Key B)
   Input Partition 2 /       \==== Network Exchange ====>/   Reducer Task 2 (Key C)
   * Forces data exchange across the network to re-group matching keys onto the same worker.
   * Involves: Disk serialization, socket transmission, disk deserialization, garbage collection.
   * Functions: groupBy(), join(), distinct(), repartition(), orderBy()
+----------------------------------------------------------------------------------------------------+
```

### The Shuffle Partitions Bottleneck: `spark.sql.shuffle.partitions`

Whenever a wide transformation occurs, Spark partitions the shuffled data into a fixed number of buckets controlled by `spark.sql.shuffle.partitions`.

- **Spark Default**: `200`
- **The Problem**: If you are aggregating a 10MB table on your local Docker setup, Spark will still create **200 separate tasks**! Scheduling 200 tasks takes 30 seconds for a job that should take 200 milliseconds.
- **The Solution**: On local or small workloads, always configure:
  ```python
  spark.conf.set("spark.sql.shuffle.partitions", "4")
  ```

---

## 5. Join Optimization Strategies

### 1. Shuffle Sort-Merge Join (SMJ)
- **Used when**: Both joined tables are large (e.g. 50M orders joined with 10M users).
- **How it works**: Spark hashes both tables on the join key, shuffles them across executors so matching keys land on the same machine, sorts each partition, and merges them in linear time.
- **Cost**: Expensive network shuffle and disk write.

### 2. Broadcast Hash Join (BHJ)
- **Used when**: One table is small (e.g. 50M orders joined with a 50-row `dim_country` table).
- **How it works**: The Spark Driver collects the small table and broadcasts an identical in-memory copy to **every executor**.
- **Result**: Every executor performs the join locally against its existing partitions. **Zero network shuffle of the 50M order records!**

```python
from pyspark.sql.functions import broadcast

# Forces Spark to broadcast the small dimension table
enriched_orders = fact_orders.join(
    broadcast(dim_products),
    on="product_id",
    how="inner"
)
```

---

## 6. Avoiding Common Production Pitfalls

### Pitfall 1: Calling `.toPandas()` or `.collect()` on the Driver
```python
# FATAL DISASTER IN PRODUCTION:
pandas_df = spark_df.toPandas()
```
- **Why it breaks**: Spark is distributed across 20 worker nodes with 200GB of RAM combined. The Driver node might only have 8GB of RAM. Calling `toPandas()` or `.collect()` forces the workers to stream **all 200GB over the network into the single Driver JVM**. The Driver will crash instantly with `OutOfMemoryError`.
- **Rule**: Use `.take(n)` or `.limit(n).toPandas()` for debugging. Never bring massive datasets into the Driver.

### Pitfall 2: Data Skew (The 99% Complete Stalled Job)
- **Symptom**: You check the Spark Web UI (:8080). 199 tasks finish in 10 seconds, but 1 task stays running for 2 hours.
- **Cause**: Data is skewed. For example, 80% of rows have `user_id = NULL` or a default guest account ID. All those rows are sent to a single executor task!
- **Fix**: Filter out nulls prior to joins, enable **Adaptive Query Execution (AQE)** (`spark.sql.adaptive.enabled=true`), or apply **Key Salting** (appending random numbers `0..k` to the join key to distribute the load across multiple tasks).

---

## 7. Deep Dive: The Bronze to Silver PySpark Job

Let's dissect the production transformation implemented in [`scripts/pyspark_bronze_to_silver.py`](file:///home/ubuntu/agent/scripts/pyspark_bronze_to_silver.py):

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

# 1. Connect Spark to S3/MinIO
spark = SparkSession.builder \
    .appName("BronzeToSilverProcessing") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Ingest raw Bronze JSON
raw_df = spark.read.json("s3a://bronze/orders/")

# 3. Schema Enforcement & Null Filtering (Narrow Transformation)
cleaned_df = raw_df \
    .withColumn("order_id", col("order_id").cast(IntegerType())) \
    .withColumn("user_id", col("user_id").cast(IntegerType())) \
    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
    .withColumn("created_at", to_timestamp(col("created_at"))) \
    .withColumn("updated_at", to_timestamp(col("updated_at"))) \
    .filter(col("order_id").isNotNull() & col("total_amount").isNotNull())

# 4. Distributed Deduplication using Window Function (Wide Transformation / Shuffle)
window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
deduped_df = cleaned_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# 5. Extract Partition Keys & Write to Silver Parquet
silver_df = deduped_df \
    .withColumn("year", year(col("created_at"))) \
    .withColumn("month", month(col("created_at")))

silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3a://silver/orders/")
```

---

## 8. Summary Checklist for Backend Engineers

- [x] Spark Driver coordinates tasks; Executors run tasks inside worker JVMs.
- [x] PySpark delegates all data processing to the JVM; avoid Python UDFs.
- [x] Narrow transformations pipeline in memory; Wide transformations trigger network shuffles.
- [x] Set `spark.sql.shuffle.partitions` appropriately (4 locally, higher on big clusters).
- [x] Use `broadcast()` when joining small lookup tables with large fact tables.
- [x] Never call `.toPandas()` or `.collect()` on large datasets.
