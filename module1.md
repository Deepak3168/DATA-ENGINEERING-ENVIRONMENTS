# Module 1: Advanced Analytical SQL & Dimensional Modeling

---

## 1. Learning Objectives

By the end of this module, you will be able to:
1. Construct and optimize complex SQL **Window Functions** (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`, and custom window frame clauses).
2. Structure multi-stage data pipelines using **Common Table Expressions (CTEs)** and understand optimization barriers (Materialized vs Inlined CTEs).
3. Architect dimensional models using the **Kimball Methodology** (Star Schema vs. Snowflake Schema).
4. Distinguish between Fact table types (Additive, Semi-Additive, Non-Additive) and choose appropriate granularity.
5. Implement **Slowly Changing Dimensions (SCD Type 1 and Type 2)** to maintain historical accuracy over time.
6. Query and maintain the analytical star schema implemented in [`init-scripts/01-init-dbs.sh`](file:///home/ubuntu/agent/init-scripts/01-init-dbs.sh).

---

## 2. Window Functions: The Data Engineer's Superpower

Backend engineers frequently try to compute running totals, sessionization, or deduplication in application code (Python, Go, Node.js) because standard `GROUP BY` collapses individual rows. 

**Window functions perform calculations across a set of table rows that are related to the current row, without collapsing the rows into a single output row.**

```
+----------------------------------------------------------------------------------------------------+
|                                    ANATOMY OF A WINDOW FUNCTION                                     |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|   FUNCTION(...) OVER (                                                                             |
|       PARTITION BY partition_column_1, partition_column_2   -- 1. Divides data into logical groups |
|       ORDER BY sort_column ASC | DESC                       -- 2. Sorts rows inside each partition |
|       ROWS | RANGE BETWEEN frame_start AND frame_end        -- 3. Defines sliding window boundaries|
|   )                                                                                                |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### 2.1 Ranking Functions Compared

Ranking functions assign an ordinal value to each row within a partition according to a specified ordering.

| Function | Behavior with Duplicate Order Values (e.g. amounts: $100, $100, $50) | Output Values | Primary DE Use Case |
| :--- | :--- | :--- | :--- |
| `ROW_NUMBER()` | Assigns a strictly unique, contiguous integer to each row. | 1, 2, 3 | **Deduplication**: Picking the latest event per entity. |
| `RANK()` | Ties share the same rank; subsequent rank skips numbers. | 1, 1, 3 | Leaderboards, percentile distributions. |
| `DENSE_RANK()`| Ties share the same rank; subsequent rank is contiguous. | 1, 1, 2 | Consecutive tier grading, grouping ranks. |
| `NTILE(n)` | Divides partition into `n` roughly equal buckets. | Bucket 1 to `n` | Decile, quartile, or percentile analysis. |

#### Visual Comparison of Ranking Functions

```
+----------+--------+--------------+--------+--------------+------------+
| Order ID | Amount | ROW_NUMBER() | RANK() | DENSE_RANK() | NTILE(2)   |
+----------+--------+--------------+--------+--------------+------------+
|      101 | $300   |            1 |      1 |            1 |          1 |
|      102 | $200   |            2 |      2 |            2 |          1 |
|      103 | $200   |            3 |      2 |            2 |          2 |
|      104 | $100   |            4 |      4 |            3 |          2 |
+----------+--------+--------------+--------+--------------+------------+
```

---

### 2.2 Critical Data Engineering Patterns with Window Functions

#### Pattern 1: Deduplication & Latest Snapshot (Idempotent State Extraction)
When extracting change logs or event streams (e.g., from Kafka or Change Data Capture (CDC)), an order might appear multiple times as its status changes from `PENDING` -> `PROCESSING` -> `COMPLETED`.

```sql
-- Connect to oltp_db or warehouse_db
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
        ) AS row_num
    FROM orders
)
SELECT 
    order_id,
    user_id,
    total_amount,
    order_status,
    updated_at
FROM ranked_orders
WHERE row_num = 1;
```
*Why this works:* `PARTITION BY order_id` isolates each order into its own processing group. `ORDER BY updated_at DESC` places the most recent modification at row 1. Filtering `row_num = 1` extracts the latest state idempotently.

---

#### Pattern 2: Period-over-Period Delta Analysis with `LAG` and `LEAD`
Calculating Month-over-Month (MoM) revenue growth without self-joining the table:

```sql
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', created_at) AS order_month,
        SUM(total_amount) AS revenue
    FROM orders
    WHERE order_status = 'COMPLETED'
    GROUP BY DATE_TRUNC('month', created_at)
)
SELECT 
    order_month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY order_month) AS prior_month_revenue,
    ROUND(
        ((revenue - LAG(revenue, 1) OVER (ORDER BY order_month)) / 
         NULLIF(LAG(revenue, 1) OVER (ORDER BY order_month), 0)) * 100.0, 
        2
    ) AS mom_growth_pct
FROM monthly_metrics
ORDER BY order_month;
```

---

#### Pattern 3: Sliding Window Aggregations (Moving Averages & Cumulative Sums)
The `ROWS BETWEEN` frame clause allows dynamic time-series smoothing:

```sql
SELECT 
    created_at::DATE AS order_date,
    SUM(total_amount) AS daily_revenue,
    -- 7-Day Moving Average: Current day + prior 6 days
    AVG(SUM(total_amount)) OVER (
        ORDER BY created_at::DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg_revenue,
    -- Running Cumulative Sum of Revenue
    SUM(SUM(total_amount)) OVER (
        ORDER BY created_at::DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM orders
WHERE order_status = 'COMPLETED'
GROUP BY created_at::DATE
ORDER BY order_date;
```

```
Frame Clause Visualization:
[Day 1] (Preceding 6: N/A, Day 1) ------------------------> Avg(Day 1)
[Day 2] (Preceding 6: Day 1, Day 2) ---------------------> Avg(Day 1..2)
...
[Day 7] (Preceding 6: Day 1..6, Day 7) ------------------> Avg(Day 1..7)
[Day 8] (Preceding 6: Day 2..7, Day 8) ------------------> Avg(Day 2..8) [Slides forward]
```

---

## 3. Dimensional Modeling (Kimball Architecture)

Operational databases are normalized (3NF) to minimize write anomalies during `INSERT`, `UPDATE`, and `DELETE`.
In a Data Warehouse, we denormalize data into **Fact** and **Dimension** tables (Star Schema) to optimize for analytical read throughput.

```
                               +-------------------+
                               |     dim_date      |
                               +-------------------+
                               | PK  date_id       |
                               |     full_date     |
                               |     day_of_week   |
                               |     quarter       |
                               |     year          |
                               +-------------------+
                                         |
                                         | 1:N
                                         v
+-------------------+          +-------------------+          +-------------------+
|     dim_user      |          |    fact_orders    |          |    dim_product    |
+-------------------+          +-------------------+          +-------------------+
| PK  user_key      | 1:N      | PK  fact_order_id |      1:N | PK  product_key   |
|     user_id       |<---------| FK  user_key      |--------->|     product_id    |
|     name          |          | FK  date_id       |          |     title         |
|     email         |          | FK  product_key   |          |     category      |
|     country       |          |     order_status  |          |     price         |
|     valid_from    |          |     total_amount  |          +-------------------+
|     valid_to      |          |     item_count    |
|     is_current    |          |     loaded_at     |
+-------------------+          +-------------------+
```

### 3.1 Facts vs. Dimensions

| Dimension / Fact | Description | Characteristics | Examples |
| :--- | :--- | :--- | :--- |
| **Fact Table** | Records business events and measurements. | - Large row count (millions/billions)<br>- Narrow columns (mostly integer FKs and numeric metrics)<br>- Append-mostly | `revenue`, `quantity_ordered`, `discount_amount`, `shipping_duration_seconds` |
| **Dimension Table**| Provides context for filtering, grouping, and labeling. | - Smaller row count<br>- Wide columns with descriptive textual attributes<br>- Updated via SCD | `customer_name`, `billing_city`, `product_category`, `fiscal_quarter` |

### 3.2 Types of Fact Metrics

1. **Additive Facts**: Can be meaningfully summed across all dimensions.
   - *Example:* `total_amount`, `quantity`. (Summing revenue across all dates, all stores, and all products makes business sense).
2. **Semi-Additive Facts**: Can be summed across some dimensions, but not all (especially time).
   - *Example:* Bank account balance, inventory stock count. (Adding today's balance to yesterday's balance is meaningless; instead, we take the average or the end-of-month snapshot).
3. **Non-Additive Facts**: Cannot be summed across any dimension.
   - *Example:* Ratios, unit prices, percentages (`profit_margin = 0.25`). These must be calculated dynamically from base additive components: `SUM(profit) / SUM(revenue)`.

---

### 3.3 Surrogate Keys vs. Natural Keys

Notice that in `warehouse_db`, `dim_user` has:
- `user_id`: The **Natural / Business Key** from the production PostgreSQL database.
- `user_key`: An auto-incrementing integer **Surrogate Key** generated in the Data Warehouse.

```
WHY SURROGATE KEYS ARE MANDATORY IN DATA WAREHOUSING:
1. Performance: Integer joins (4 bytes) are dramatically faster than UUID or string joins.
2. Source System Independence: Merging data from multiple operational databases (e.g. Shopify + Custom ERP)
   might produce colliding user_id = 1. Surrogate keys ensure global uniqueness.
3. Historical Tracking (SCD Type 2): When user #42 changes their country from 'US' to 'DE',
   we must create a second row in dim_user. Both rows share user_id = 42,
   but have different surrogate keys (user_key = 101 and user_key = 205).
   Historical orders reference user_key = 101 (US), while new orders reference user_key = 205 (DE)!
```

---

## 4. Slowly Changing Dimensions (SCD)

Customer profiles, product pricing, and organizational hierarchies change over time. How does a warehouse track these modifications?

```
+----------------------------------------------------------------------------------------------------+
|                                    SCD STRATEGY COMPARISON                                         |
+----------------------------------------------------------------------------------------------------+

1. SCD Type 1: Overwrite in-place
   Before: [user_id: 10, name: "Alice", country: "US"]
   Update: Alice moves to Germany
   After:  [user_id: 10, name: "Alice", country: "DE"]
   -> Historical orders placed while in the US now incorrectly appear as "Germany" sales! History lost.

2. SCD Type 2: Maintain Full History with Effective Dating
   Before:
   [user_key: 1, user_id: 10, country: "US", valid_from: '2024-01-01', valid_to: '2026-06-01', is_current: FALSE]
   
   After update:
   [user_key: 2, user_id: 10, country: "DE", valid_from: '2026-06-01', valid_to: NULL,         is_current: TRUE]
   -> 100% historical accuracy preserved. Point-in-time joins yield exact historical context.
+----------------------------------------------------------------------------------------------------+
```

### Implementing SCD Type 2 in SQL

When building a dimensional pipeline, joining facts to SCD Type 2 dimensions requires a range join:

```sql
SELECT 
    f.order_id,
    f.total_amount,
    d.country AS country_at_time_of_purchase,
    f.created_at
FROM fact_orders f
JOIN dim_user d 
  ON f.user_id = d.user_id
 AND f.created_at >= d.valid_from 
 AND (f.created_at < d.valid_to OR d.valid_to IS NULL);
```

---

## 5. Walkthrough: The Local `warehouse_db` Schema

Review the schema created by [`init-scripts/01-init-dbs.sh`](file:///home/ubuntu/agent/init-scripts/01-init-dbs.sh):

```sql
-- Analytical Star Schema in PostgreSQL (warehouse_db)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,               -- Format: YYYYMMDD (e.g. 20261018)
    full_date DATE NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_user (
    user_key SERIAL PRIMARY KEY,           -- Warehouse surrogate key
    user_id INT NOT NULL,                  -- Operational natural key
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    country VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_orders (
    fact_order_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    user_key INT REFERENCES dim_user(user_key),
    date_id INT REFERENCES dim_date(date_id),
    order_status VARCHAR(30) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    item_count INT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Analytical Query Example: Star Schema Join

```sql
SELECT 
    d.year,
    d.month_name,
    u.country,
    COUNT(f.fact_order_id) AS total_orders,
    SUM(f.total_amount) AS gross_revenue,
    ROUND(AVG(f.total_amount), 2) AS average_order_value
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_user u ON f.user_key = u.user_key
WHERE f.order_status = 'COMPLETED'
GROUP BY d.year, d.month_name, u.country
ORDER BY d.year DESC, gross_revenue DESC;
```

---

## 6. Practical Exercises

1. **Exercise 1 (Deduplication)**: Write a query that finds all orders in `oltp_db` and extracts only the single earliest order ever placed by each user.
2. **Exercise 2 (SCD Type 2 Migration)**: Write an `UPDATE` + `INSERT` transaction that updates user `alice@example.com`'s country from `US` to `CA` while setting `valid_to = NOW()` on the old record and inserting the new record with `is_current = TRUE`.
3. **Exercise 3 (Running Aggregates)**: Write a query using `SUM(...) OVER (...)` that calculates the running percentage of total annual revenue contributed by each successive order date.
