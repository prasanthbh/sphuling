# Fast Schema Inspection & Extraction Engine

A notorious problem in data engineering is that discovering a dataset's schema often triggers accidental full-table scans. For example, reading a 100 GB CSV or JSON file in PySpark with `inferSchema=True` often reads the **entire dataset** just to determine column types.

Sphuling solves this with its **Fast Schema Inspection Engine** (`SchemaInspector`), which discovers schemas across all supported formats in milliseconds without scanning full datasets.

---

## 1. Schema Discovery Architecture

Sphuling distinguishes between three types of schema acquisition:
1. **Metadata-Based Schema Extraction ($O(1)$):** Reads embedded binary schemas from Parquet and Arrow file footers. Exact, instantaneous, and zero data rows read.
2. **Sample-Based Schema Inference:** For text-based formats (CSV, JSON), reads a bounded initial sample window ($\le 64$ KB or first $N$ rows) to deduce column names and types without scanning gigabytes of data.
3. **Explicit Schema Declaration:** Allows users to pass a known `StructType` or DDL string to bypass inference entirely.

```
                       [ ANY DATA SOURCE ]
                                |
         +----------------------+----------------------+
         |                      |                      |
         v                      v                      v
[ Columnar Metadata ]       [ Text / Delimited ]    [ Database / Catalogs ]
  Parquet, Arrow IPC          CSV, TSV, JSON, NDJSON  SQLite, SQL Tables
         |                      |                      |
  Read File Footer Only    Bounded Byte/Row Sniffer    Read Schema / LIMIT 0
    (A few KB read)         (First 64 KB / N rows)     (Zero data rows read)
         \                      |                      /
          \                     |                     /
           +--------------------+--------------------+
                                |
                                v
                Exact StructType Schema Object
            (Available in < 5 ms, 0 full scans)
```

> [!NOTE]
> For text formats (CSV and JSON), sample-based inference deduces types from the first sample batch. If late-occurring data in an unpartitioned file contains mixed types, declaring an explicit schema (`schema=StructType(...)` or `StructType.fromDDL(...)`) is recommended for production pipelines.

---

## 2. Extraction Mechanics by Source Type

### 1. Parquet, Feather, and Arrow IPC Files ($O(1)$ Complexity)
- **Mechanics:** Columnar formats store the complete schema in a binary footer Thrift structure at the very end of the file.
- **Data Read Overhead:** Sphuling seeks to the last ~4 KB – 64 KB of the file, reads the footer metadata, and exits.
- **Performance:** **`< 2 ms`**, whether the file is 10 MB or 10 TB. **Zero data blocks or row groups are read.**

### 2. CSV and Delimited Files
- **Mechanics:** Reads the first line to extract column header names, and passes a bounded byte range (first 64 KB or first 100 rows) through PyArrow's C++ schema sniffer.
- **Data Read Overhead:** Capped strictly at $\le 64$ KB. Never reads beyond the bounded window.
- **Performance:** **`< 10 ms`**.

### 3. JSON and NDJSON Files
- **Mechanics:** Reads the first 50–100 JSON objects from the start of the stream and infers nested data structures (`StructType`, `ArrayType`, `MapType`).
- **Data Read Overhead:** Capped at the first sample chunk; avoids scanning gigabytes of text.
- **Performance:** **`< 15 ms`**.

### 4. Relational Databases (SQLite, DuckDB, PostgreSQL, MySQL)
- **Mechanics:** Issues an empty prepared statement (`SELECT * FROM table WHERE 1=0` or `LIMIT 0`) or inspects the information schema.
- **Data Read Overhead:** **Zero data rows** are fetched or transferred over the network.
- **Performance:** **`< 5 ms`**.

### 5. Delta Lake Tables
- **Mechanics:** Inspects the latest commit JSON inside the `_delta_log/` directory, parsing the serialized `schemaString` attribute.
- **Data Read Overhead:** Reads a single transaction metadata file. **Zero underlying Parquet data files are touched.**
- **Performance:** **`< 8 ms`**.

### 6. In-Memory Objects (Pandas DataFrames, PyArrow Tables)
- **Mechanics:** Directly inspects memory pointers (`__arrow_c_schema__` or `.dtypes`).
- **Data Read Overhead:** **Zero memory copying** or serialization.
- **Performance:** **`< 1 ms`**.

---

## 3. How to Use Zero-Load Schema Extraction

### Method 1: Instant DataFrame Properties (No Action Needed)
When you load a DataFrame in Sphuling, `df.schema` is populated instantly:

```python
from pysphuling.sql import SphulingSession

sphuling = SphulingSession.builder.getOrCreate()

# Load reference to massive dataset
df = sphuling.read.parquet("s3_or_local/terabyte_scale_dataset/")

# Access schema in O(1) time
schema = df.schema          # Returns StructType immediately
print(f"Columns: {schema.names}")
print(f"Salary Type: {schema['salary'].dataType}")

# Print formatted tree view
df.printSchema()
```

### Method 2: Dedicated Universal Schema Inspector
You can extract the schema of any source directly **without even creating a DataFrame**:

```python
# Extract schema from Parquet
pq_schema = sphuling.read.schema_of("data/large_dataset.parquet")

# Extract schema from CSV with custom delimiter
csv_schema = sphuling.read.schema_of("data/transactions.csv", header=True, sep="|")

# Extract schema from JSON
json_schema = sphuling.read.schema_of("data/events.json", sample_rows=50)

# Extract schema from SQLite database
db_schema = sphuling.read.schema_of("sqlite:///warehouse.db#customers")

# Inspect an in-memory Pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
mem_schema = sphuling.read.schema_of(pdf)
```

