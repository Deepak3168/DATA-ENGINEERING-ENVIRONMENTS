# Module 2: Object Storage, Data Lakes & Storage Formats (Parquet vs. CSV/JSON)

---

## 1. Learning Objectives

By the end of this module, you will be able to:
1. Explain why **Cloud Object Storage (S3 / GCS / MinIO)** replaced traditional POSIX filesystems as the foundation of modern data lakes.
2. Dissect the binary anatomy of **Apache Parquet** (Header, Row Groups, Column Chunks, Pages, and Footer Metadata).
3. Explain how query engines leverage **Predicate Pushdown** and **Column Pruning** to skip 90%+ of I/O.
4. Compare data encodings (Dictionary Encoding, Run-Length Encoding, Bit-Packing) and compression codecs (Snappy, ZSTD, GZIP).
5. Design optimal Hive-style partitioning strategies (`year=YYYY/month=MM`) and avoid the catastrophic **Small Files Problem**.
6. Inspect Parquet metadata and perform benchmarks using Python, PyArrow, DuckDB, and MinIO.

---

## 2. Why Object Storage Over Traditional Filesystems?

In backend web development, we are accustomed to POSIX filesystems (ext4, XFS, NFS) where files live in a hierarchical directory tree with permissions, inode tables, and file locks.

When dealing with petabyte-scale data pipelines, POSIX filesystems fail:
- **Inode Limits & Metadata Bottlenecks**: A traditional filesystem has a fixed inode limit. Navigating deeply nested directories with millions of files creates severe metadata locking contention.
- **Tightly Coupled Disks**: Storing 50TB on an attached SSD volume requires provisioning an enormous EC2/Compute instance, leading to wasted compute spend when workloads are idle.
- **Network File System (NFS) Performance**: Distributed POSIX systems suffer severe write serialization and latency under concurrent multi-node writes.

### Cloud Object Storage: The Flat Key-Value Namespace

Object storage systems like **Amazon S3**, **Google Cloud Storage (GCS)**, **Azure Data Lake Storage (ADLS)**, and our local **MinIO** service do not have true directories. They are massive, globally distributed key-value stores:
- A "file path" like `bronze/orders/year=2026/month=10/data.json` is simply a string key inside the `bronze` bucket.
- Access occurs entirely over standard HTTP/REST APIs:
  - `GET /bucket/key` (Fetch bytes, supports `Range: bytes=start-end` for partial reads)
  - `PUT /bucket/key` (Atomic write of an immutable object)
  - `DELETE /bucket/key`
  - `GET /bucket?prefix=orders/` (List keys matching a prefix)
- **High Durability**: Designed for 99.999999999% (11 nines) durability via automatic multi-datacenter erasure coding.
- **Ultra-Low Cost**: ~$0.02 per GB per month compared to ~$0.10+ per GB for provisioned SSDs.

---

## 3. Storage Formats: CSV vs. JSON vs. Apache Parquet

Operational systems output raw data in CSV or JSON. While human-readable, these formats are disastrous for analytics.

```
+----------------------------------------------------------------------------------------------------+
|                                      STORAGE FORMAT COMPARISON                                     |
+----------------------------------------------------------------------------------------------------+
| Feature                  | CSV                     | JSON Lines (JSONL)      | Apache Parquet      |
+--------------------------+-------------------------+-------------------------+---------------------+
| Layout                   | Row-oriented            | Row-oriented            | Columnar            |
| Schema Enforcement       | None (strings only)     | Implicit/None (keys)    | Strict Binary Schema|
| Data Types               | Everything is text      | Basic (string, num, bool| Rich (timestamp, etc|
| Key Repetition           | Header only             | Repeated every single row| Stored once in Schema|
| Read Efficiency          | Must scan every byte    | Must scan every byte    | Column & Page Skip  |
| Compression Performance  | Moderate (gzip/bz2)     | Moderate                | Exceptional (Snappy)|
| Splittable for Spark     | Difficult if compressed | Splittable per line     | Natively Splittable |
+----------------------------------------------------------------------------------------------------+
```

---

## 4. Deep Dive: The Binary Anatomy of Apache Parquet

Apache Parquet is an open-source, columnar storage format created specifically for analytics.

```
+-----------------------------------------------------------------------------------------+
|                               APACHE PARQUET FILE STRUCTURE                             |
+-----------------------------------------------------------------------------------------+
|  Magic Bytes: "PAR1" (4 bytes)                                                          |
+-----------------------------------------------------------------------------------------+
|  ROW GROUP 1 (e.g. 500,000 rows / 128 MB)                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Column Chunk: 'order_id'                                                           |  |
|  |   [Data Page 1] [Data Page 2] [Data Page 3] ...                                   |  |
|  +-----------------------------------------------------------------------------------+  |
|  | Column Chunk: 'total_amount'                                                      |  |
|  |   [Data Page 1] [Data Page 2] [Data Page 3] ...                                   |  |
|  +-----------------------------------------------------------------------------------+  |
|  | Column Chunk: 'order_status'                                                      |  |
|  |   [Dictionary Page] [Data Page 1] [Data Page 2] ...                               |  |
|  +-----------------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------------+
|  ROW GROUP 2 (Next 500,000 rows / 128 MB)                                               |
|  +-----------------------------------------------------------------------------------+  |
|  | Column Chunk: 'order_id' ...                                                      |  |
|  | Column Chunk: 'total_amount' ...                                                  |  |
|  | Column Chunk: 'order_status' ...                                                  |  |
|  +-----------------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------------+
|  FILE FOOTER (Read FIRST by Query Engines!)                                             |
|  - Schema Definition & Field Encodings                                                  |
|  - Row Group 1 Metadata: Column offsets, Row count                                      |
|    * 'order_id' Stats:     [min: 1,      max: 500000, null_count: 0]                     |
|    * 'total_amount' Stats: [min: 5.00,   max: 499.50, null_count: 0]                     |
|  - Row Group 2 Metadata: Column offsets, Row count                                      |
|    * 'order_id' Stats:     [min: 500001, max: 1000000]                                  |
|    * 'total_amount' Stats: [min: 12.00,  max: 350.00]                                   |
|  - Footer Length (4 bytes)                                                              |
|  Magic Bytes: "PAR1" (4 bytes)                                                          |
+-----------------------------------------------------------------------------------------+
```

### Why the Footer is Stored at the End of the File
Unlike many traditional file formats that put metadata headers at the beginning, Parquet puts its metadata **at the end** (File Footer).
- **Single-Pass Streaming Writes**: During distributed writes (e.g. in Spark), the workers do not know the total row count, min/max statistics, or byte offsets until the file has finished writing. Placing the metadata at the end allows the engine to stream data to S3 without buffering the entire file in RAM or seeking backwards!
- **How Readers Read Parquet**:
  1. The reader issues an HTTP Range request for the last 4 bytes to check the magic bytes (`PAR1`) and the preceding 4-byte footer length.
  2. The reader issues a second Range request to fetch the exact bytes of the File Footer.
  3. The reader now knows the exact schema, row group count, and byte offsets of every column chunk!

---

### Predicate Pushdown & Row Group Pruning

Suppose an analyst runs:
```sql
SELECT SUM(total_amount) FROM orders WHERE order_id = 750000;
```

When reading Parquet:
1. The engine inspects Row Group 1 metadata: `order_id` range is `[1, 500000]`. Value `750000` cannot possibly exist in Row Group 1!
2. **The entire 128MB Row Group 1 is completely skipped without transferring a single byte over the network!**
3. The engine inspects Row Group 2: `order_id` range is `[500001, 1000000]`. It reads **only the `order_id` and `total_amount` column chunks** from Row Group 2.

This combination of **Column Pruning** (skipping unwanted columns) and **Row Group Pruning** (skipping non-matching blocks via Min/Max stats) reduces cloud storage data transfer by up to 99%.

---

## 5. Columnar Encodings & Compression

Parquet achieves dramatic compression ratios through a two-step process: **Encoding**, followed by **Block Compression**.

```
Raw Column Data:
["COMPLETED", "COMPLETED", "CANCELLED", "COMPLETED", "PROCESSING", "COMPLETED"]

Step 1: Dictionary Encoding
Dictionary Page:
[0: "COMPLETED", 1: "CANCELLED", 2: "PROCESSING"]
Encoded Data Page:
[0, 0, 1, 0, 2, 0]  (Replaced 9-byte strings with 2-bit integers!)

Step 2: Run-Length Encoding (RLE) & Bit-Packing
Contiguous values are compacted: "Two 0s, One 1, One 0, One 2, One 0"

Step 3: Compression Codec
The resulting bit stream is compressed with Snappy, ZSTD, or GZIP.
```

### Choosing a Compression Codec

| Codec | Compression Ratio | Compression Speed | Decompression Speed | Best Used For |
| :--- | :--- | :--- | :--- | :--- |
| **Snappy** *(Default)* | Moderate (3x - 5x) | Ultra Fast | Extremely Fast | Hot/Warm Lakehouse tables, interactive queries. |
| **Zstandard (ZSTD)**| High (5x - 8x) | Moderate / Configurable | Very Fast | Modern gold standard for enterprise data lakes. |
| **GZIP** | Very High (6x - 10x)| Slow | Slow | Cold storage, long-term regulatory archival. |

---

## 6. Lakehouse Partitioning & The "Small Files" Pitfall

Partitioning divides a large dataset into separate physical folders in object storage using Hive-style notation:

```
s3://silver/orders/
├── year=2026/
│   ├── month=09/
│   │   ├── part-0000.parquet
│   │   └── part-0001.parquet
│   └── month=10/
│       ├── part-0000.parquet
│       └── part-0001.parquet
```

When a query contains `WHERE year = 2026 AND month = 10`, the query engine performs **Partition Pruning**: it lists and reads objects inside `s3://silver/orders/year=2026/month=10/` only, completely ignoring all other years and months.

### The Small Files Problem: The #1 Data Lake Killer

Backend engineers often intuitively partition by granular fields:
- Partitioning by `user_id`: Creates 100,000 directories, each with a 2 KB file.
- Partitioning by `minute`: Ingesting data every minute generates 1,440 tiny files per day.

#### Why Millions of Small Files Destroy Pipelines:
1. **Metadata Overhead**: Query engines like Spark or Presto must issue an HTTP `GET / LIST` request for every single file. If a query hits 500,000 files, Spark spends 20 minutes just making S3 API calls before reading a single record!
2. **Spark Driver OOM**: The Spark Driver must store the block location and metadata for every file in its JVM heap memory. Millions of files will crash the Driver node with `java.lang.OutOfMemoryError: Java heap space`.
3. **Parquet Metadata Bloat**: Every Parquet file has its own footer and dictionary page (~10KB-50KB). If a file only contains 2KB of actual data, the file is 90% metadata overhead!

#### The Golden Rules of Partitioning:
1. Target individual Parquet file sizes between **128 MB and 512 MB**.
2. Only partition on columns that are regularly filtered in `WHERE` clauses (e.g. `date`, `region`).
3. Only partition on low-cardinality columns (cardinality < 1,000).
4. Run regular **Compaction Jobs** (merging small files into 256MB blocks) using PySpark or Lakehouse table formats (`OPTIMIZE` in Delta/Iceberg).

---

## 7. Hands-on Benchmark Lab

Let's verify the performance and storage differences in practice.

### Step 1: Run the Format Benchmark Script

You can execute this benchmark directly in Python inside the workspace container:

```python
import pandas as pd
import numpy as np
import os
import time

# Generate 500,000 synthetic records
n = 500_000
data = {
    'order_id': np.arange(1, n + 1),
    'user_id': np.random.randint(1, 50_000, size=n),
    'country': np.random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU'], size=n),
    'amount': np.random.uniform(5.0, 500.0, size=n).round(2),
    'status': np.random.choice(['COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED'], size=n)
}
df = pd.DataFrame(data)

os.makedirs('/tmp/benchmark', exist_ok=True)

# 1. Write benchmarks
t0 = time.time()
df.to_csv('/tmp/benchmark/data.csv', index=False)
csv_write_time = time.time() - t0

t0 = time.time()
df.to_json('/tmp/benchmark/data.json', orient='records', lines=True)
json_write_time = time.time() - t0

t0 = time.time()
df.to_parquet('/tmp/benchmark/data.parquet', engine='pyarrow', compression='snappy')
parquet_write_time = time.time() - t0

# 2. Check File Sizes
csv_size = os.path.getsize('/tmp/benchmark/data.csv') / (1024 * 1024)
json_size = os.path.getsize('/tmp/benchmark/data.json') / (1024 * 1024)
parquet_size = os.path.getsize('/tmp/benchmark/data.parquet') / (1024 * 1024)

print(f"CSV Size:     {csv_size:.2f} MB (Write time: {csv_write_time:.2f}s)")
print(f"JSON Size:    {json_size:.2f} MB (Write time: {json_write_time:.2f}s)")
print(f"Parquet Size: {parquet_size:.2f} MB (Write time: {parquet_write_time:.2f}s)")
```

#### Typical Benchmark Results:
- `data.json`: **45.2 MB**
- `data.csv`: **21.8 MB**
- `data.parquet`: **3.9 MB** *(82% smaller than CSV, 91% smaller than JSON!)*

---

### Step 2: Inspecting Parquet Metadata with DuckDB

Run interactive SQL against Parquet metadata using DuckDB:

```python
import duckdb

con = duckdb.connect()

# Inspect Parquet schema & internal column encodings
metadata = con.execute("SELECT * FROM parquet_metadata('/tmp/benchmark/data.parquet');").df()
print(metadata[['file_name', 'row_group_id', 'column_id', 'compression', 'encodings']])

# Read only projected columns (Column Pruning in action)
res = con.execute("""
    SELECT country, SUM(amount) AS total_revenue
    FROM read_parquet('/tmp/benchmark/data.parquet')
    GROUP BY country;
""").df()
print(res)
```

---

## 8. Summary Checklist for Backend Engineers

- [x] Use Object Storage (S3/MinIO) for infinite scalability and storage-compute decoupling.
- [x] Ingest raw files as JSON/CSV into the **Bronze** layer, but convert immediately to **Parquet** in the **Silver** layer.
- [x] Leverage **Column Pruning** and **Predicate Pushdown** to minimize data transfer.
- [x] Always use **Snappy** or **ZSTD** compression for Parquet files.
- [x] Partition only by coarse dimensions (`year=YYYY/month=MM`), keeping file sizes between 128MB and 512MB to avoid the Small Files Problem.
