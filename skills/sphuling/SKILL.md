---
name: sphuling
description: "Use Sphuling (pysphuling) to execute ultra-fast, local-first PySpark-compatible DataFrame pipelines and Spark SQL queries powered by Apache DataFusion, Apache Arrow, and DuckDB without JVM overhead."
---

# Sphuling (`pysphuling`) AI Agent Skill

## Overview
Sphuling is a drop-in, zero-JVM replacement for Apache Spark / PySpark designed for local workstations. It provides 100% PySpark DataFrame syntax (`SphulingSession`, `DataFrame`, `Column`, `functions as F`, `types as T`, `Window`) running on Apache DataFusion and DuckDB.

---

## When to Activate This Skill
Use this skill whenever:
- The user requests data transformations, aggregations, or analytics using Apache Spark or PySpark syntax.
- You need to run fast local DataFrame operations without starting a heavy Java/JVM process.
- You need to inspect the schema of large Parquet, CSV, JSON, or SQL datasets without loading the full data.
- The user asks to optimize, test, or execute PySpark code locally with sub-second feedback.

---

## Key Rules for AI Agents

1. **Drop-in PySpark Aliases:**
   You can either use native Sphuling syntax or standard PySpark syntax:
   ```python
   # Direct syntax
   from pysphuling.sql import SphulingSession, functions as F, types as T, Window
   # Or drop-in syntax
   import pysphuling as pyspark
   from pyspark.sql import SparkSession, functions as F, types as T, Window
   ```

2. **Always Use Zero-Load Schema Extraction:**
   Never read an entire dataset just to understand its schema! Use `sphuling.read.schema_of()` or `df.schema`:
   ```python
   # Instant O(1) schema extraction:
   schema = sphuling.read.schema_of("path/to/dataset.parquet")
   print(schema.names)
   ```

3. **Parenthesize Logical Operators:**
   In Python, bitwise `&`, `|`, and `~` have higher precedence than comparison operators. **Always wrap conditions in parentheses**:
   ```python
   # Correct:
   df.filter((F.col("salary") > 50000) & (F.col("dept") == "Engineering"))
   # Wrong (will throw TypeError):
   df.filter(F.col("salary") > 50000 & F.col("dept") == "Engineering")
   ```

4. **Avoid the Legacy RDD API:**
   Sphuling intentionally omits the JVM RDD API (`sc.parallelize`, `df.rdd`). Use vectorized DataFrame operations, `.toPandas()`, or `.toArrow()` instead.

5. **Resource Management:**
   For heavy datasets, specify memory and core bounds to enable automatic NVMe disk spilling:
   ```python
   sphuling = (
       SphulingSession.builder
       .master("local[*]")
       .config("sphuling.executor.cores", "8")
       .config("sphuling.executor.memory", "8g")
       .getOrCreate()
   )
   ```

---

## Standard Code Patterns

### 1. Ingesting & Transforming Data
```python
from pysphuling.sql import SphulingSession, functions as F

sphuling = SphulingSession.builder.appName("AgentTask").getOrCreate()

# Read Parquet
df = sphuling.read.parquet("data/transactions.parquet")

# Filter, compute columns, and group by
result = (
    df.filter(F.col("status") == "COMPLETED")
    .withColumn("tax", F.col("amount") * 0.08)
    .groupBy("category")
    .agg(
        F.count("*").alias("tx_count"),
        F.sum("amount").alias("total_sales"),
        F.avg("tax").alias("avg_tax")
    )
    .orderBy(F.col("total_sales").desc())
)

# Print tabular output
result.show(10)
```

### 2. Analytical Window Functions
```python
from pysphuling.sql.window import Window
import pysphuling.sql.functions as F

w = Window.partitionBy("department").orderBy(F.col("salary").desc())

ranked_df = (
    df.withColumn("rank", F.rank().over(w))
    .withColumn("row_number", F.row_number().over(w))
    .withColumn("dept_running_total", F.sum("salary").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
)
```

### 3. Spark SQL Queries with Temporary Views
```python
df.createOrReplaceTempView("orders")

sql_df = sphuling.sql("""
    SELECT 
        customer_id, 
        COUNT(*) AS order_count,
        SUM(amount) AS total_spend
    FROM orders
    GROUP BY customer_id
    HAVING total_spend > 500
    ORDER BY total_spend DESC
""")
sql_df.show()
```

### 4. Zero-Copy Passing to Pandas or Scikit-Learn
```python
# Convert to Pandas in 0 ms without socket marshaling
pdf = df.filter(F.col("is_active") == True).toPandas()

# Convert directly to Apache Arrow Table
arrow_tbl = df.toArrow()
```

