# API Reference: DataFrame Actions

Actions trigger the physical execution pipeline, streaming vectorized Arrow RecordBatches across CPU cores and returning materialized Python results.

---

## 1. Materialization Actions

### `.collect() -> List[Row]`
Executes the physical plan and returns all records as a Python list of `Row` objects:

```python
rows = df.collect()
for r in rows:
    print(f"ID: {r.id}, Name: {r.name}")
    print(r.asDict()) # Access as Python dictionary
```

### `.first() -> Optional[Row]`
Returns the first row as a `Row`, or `None` if the DataFrame is empty:

```python
first_row = df.first()
if first_row:
    print(f"Top row: {first_row.name}")
```

### `.head(n: int = 1) -> Union[Optional[Row], List[Row]]`
Returns the first row if $n=1$, or a list of first $N$ rows if $n > 1$:

```python
single_row = df.head()      # Returns Row
top_5_rows = df.head(5)     # Returns List[Row]
```

### `.take(num: int) -> List[Row]`
Returns the first $N$ rows as a list of `Row`:

```python
samples = df.take(10)
```

### `.count() -> int`
Returns total number of rows. Automatically optimized with metadata statistics when available:

```python
total_rows = df.count()
print(f"Total: {total_rows}")
```

---

## 2. Visualization & Inspection

### `.show(n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None`
Prints formatted tabular output matching PySpark's ASCII table format:

```python
# Default table (top 20 rows, truncated strings)
df.show()

# Show top 5 rows without truncation
df.show(5, truncate=False)

# Vertical presentation (useful for wide schemas with 50+ columns)
df.show(3, vertical=True)
```

Vertical output style:
```text
-RECORD 0-------------------
 id         | 101
 name       | Alice
 department | Engineering
 salary     | 95000
-RECORD 1-------------------
 id         | 102
 name       | Bob
 department | Engineering
 salary     | 80000
```

### `.printSchema() -> None`
Prints the schema tree in $O(1)$ time without scanning rows:

```python
df.printSchema()
```

Output:
```text
root
 |-- id: int (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
```

### `.explain(mode: str = "extended") -> None`
Prints the parsed logical plan, optimized logical plan, and native physical execution plan:

```python
df.explain()
```

Output shows pushdown filters, projection pruning, and thread partitioning details.

---

## 3. Summary & Descriptive Statistics

### `.describe(*cols: str) -> DataFrame`
Computes count, mean, min, and max for numeric and string columns:

```python
df.describe("salary", "bonus").show()
```

Output:
```text
+---------+---------+---------+
| summary | salary  | bonus   |
+---------+---------+---------+
| count   | 1000    | 1000    |
| mean    | 75420.5 | 9050.46 |
| min     | 45000.0 | 4500.0  |
| max     | 135000  | 16200.0 |
+---------+---------+---------+
```

### `.summary(*statistics: str) -> DataFrame`
Configurable statistical summary:

```python
df.summary().show()
```

---

## 4. Zero-Copy In-Memory Interop

### `.toPandas() -> pandas.DataFrame`
Zero-copy converts the underlying Arrow memory buffers into a Pandas DataFrame:

```python
pdf = df.toPandas()
print(type(pdf)) # <class 'pandas.core.frame.DataFrame'>
```

### `.toArrow() -> pyarrow.Table`
Extracts the underlying Apache Arrow `Table`:

```python
arrow_table = df.toArrow()
print(f"Num batches: {len(arrow_table.to_batches())}")
print(f"Total bytes: {arrow_table.nbytes}")
```

---

## 5. Persistence & Export

### `.write -> DataFrameWriter`
Returns the `DataFrameWriter` interface for saving to Parquet, CSV, JSON, or Arrow:

```python
df.write.mode("overwrite").parquet("output/data.parquet")
```

