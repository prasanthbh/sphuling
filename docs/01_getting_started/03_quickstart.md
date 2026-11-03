# Quickstart: 5-Minute Tour

This guide walks you through building an end-to-end data pipeline in Sphuling using familiar PySpark syntax.

---

## Step 1: Create a `SphulingSession`

In PySpark you create a `SparkSession`; in Sphuling you create a `SphulingSession` (or use `SparkSession` as an alias):

```python
from pysphuling.sql import SphulingSession, functions as F, types as T, Window

# Initialize session with 4 executor cores and 4 GB RAM bound
sphuling = (
    SphulingSession.builder
    .appName("QuickstartPipeline")
    .master("local[4]")
    .config("sphuling.executor.cores", "4")
    .config("sphuling.executor.memory", "4g")
    .getOrCreate()
)
```

---

## Step 2: Ingest Sample Data

Create a DataFrame from a Python list of tuples with column names:

```python
employees_data = [
    (1, "Alice", 95000, "Engineering", "San Francisco"),
    (2, "Bob", 80000, "Engineering", "New York"),
    (3, "Charlie", 65000, "Marketing", "New York"),
    (4, "Diana", 110000, "Engineering", "San Francisco"),
    (5, "Evan", 72000, "Marketing", "San Francisco"),
    (6, "Fiona", 125000, "Executive", "New York"),
]

df = sphuling.createDataFrame(
    employees_data,
    schema=["id", "name", "salary", "department", "city"]
)
```

---

## Step 3: Zero-Load Schema Inspection

Inspect the DataFrame schema instantly without executing any underlying computation:

```python
# Print tree schema
df.printSchema()

# Access schema programmatically
print(f"Columns: {df.columns}")
print(f"Total fields: {len(df.schema)}")
```

Output:
```text
root
 |-- id: bigint (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: bigint (nullable = true)
 |-- department: string (nullable = true)
 |-- city: string (nullable = true)
```

---

## Step 4: Lazy Transformations

Every transformation (`select`, `filter`, `withColumn`, `groupBy`) creates an optimized execution graph (DAG). Nothing is executed yet:

```python
# Define analytical window specification
w = Window.partitionBy("department").orderBy(F.col("salary").desc())

# Build pipeline
processed_df = (
    df.filter(F.col("salary") >= 70000)
    .withColumn("bonus", F.col("salary") * 0.15)
    .withColumn("dept_salary_rank", F.rank().over(w))
    .withColumn("is_sf", F.when(F.col("city") == "San Francisco", True).otherwise(False))
)
```

---

## Step 5: Trigger Execution with Actions

Actions compile the logical DAG into DataFusion's physical SIMD plan and stream Arrow RecordBatches across CPU threads:

```python
# Pretty print formatted table
processed_df.show()

# Get total count
print(f"Qualified Employees: {processed_df.count()}")

# Collect rows as Python Row objects
for row in processed_df.collect():
    print(f"Employee: {row.name}, Dept: {row.department}, Rank: {row.dept_salary_rank}")
```

Output:
```text
+----+-------+--------+-------------+---------------+---------+------------------+-------+
| id | name  | salary | department  | city          | bonus   | dept_salary_rank | is_sf |
+----+-------+--------+-------------+---------------+---------+------------------+-------+
| 1  | Alice | 95000  | Engineering | San Francisco | 14250.0 | 2                | true  |
| 2  | Bob   | 80000  | Engineering | New York      | 12000.0 | 3                | false |
| 4  | Diana | 110000 | Engineering | San Francisco | 16500.0 | 1                | true  |
| 5  | Evan  | 72000  | Marketing   | San Francisco | 10800.0 | 1                | true  |
| 6  | Fiona | 125000 | Executive   | New York      | 18750.0 | 1                | false |
+----+-------+--------+-------------+---------------+---------+------------------+-------+
```

---

## Step 6: Spark SQL with Temp Views

Register the DataFrame as an in-memory SQL view and query it with Spark SQL dialect:

```python
# Register temporary view
df.createOrReplaceTempView("employees")

# Query with standard Spark SQL
sql_df = sphuling.sql("""
    SELECT 
        department,
        COUNT(*) AS total_staff,
        AVG(salary) AS avg_compensation,
        MAX(salary) AS highest_earner
    FROM employees
    GROUP BY department
    HAVING total_staff > 1
    ORDER BY avg_compensation DESC
""")

sql_df.show()
```

Output:
```text
+-------------+-------------+------------------+----------------+
| department  | total_staff | avg_compensation | highest_earner |
+-------------+-------------+------------------+----------------+
| Engineering | 3           | 95000.0          | 110000         |
| Marketing   | 2           | 68500.0          | 72000          |
+-------------+-------------+------------------+----------------+
```

---

## Step 7: Multi-Source Export & Partitioned Writes

Export the processed dataset to partitioned Parquet files:

```python
# Write Hive-partitioned Parquet directory
processed_df.write.mode("overwrite").partitionBy("department").parquet("output/employees_partitioned")

# Instant zero-load schema check on the written dataset
schema = sphuling.read.schema_of("output/employees_partitioned")
print(f"Extracted Schema: {schema.names}")

# Read back
reloaded_df = sphuling.read.parquet("output/employees_partitioned")
print(f"Reloaded row count: {reloaded_df.count()}")
```

---

## Step 8: Zero-Copy Pandas & PyArrow Interop

Seamlessly pass data to Pandas or PyArrow without socket serialization:

```python
# Zero-copy conversion to pandas DataFrame
pdf = processed_df.toPandas()
print(type(pdf))  # <class 'pandas.core.frame.DataFrame'>

# Direct extraction of Apache Arrow Table
arrow_tbl = processed_df.toArrow()
print(f"Arrow record batches: {len(arrow_tbl.to_batches())}")
```

