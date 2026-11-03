# API Reference: I/O Connectors (`read` & `write`)

Sphuling provides high-speed reader and writer interfaces supporting Parquet, Apache Arrow / Feather, CSV, JSON, and in-memory tables.

---

## 1. Reading Data (`DataFrameReader` / `sphuling.read`)

```python
from pysphuling.sql import SphulingSession

sphuling = SphulingSession.builder.getOrCreate()
```

### 1.1 Zero-Load Schema Introspection (`.schema_of`)
Discovers exact schemas from footers and sample sniffers without scanning full datasets:

```python
# Parquet footer read (< 2 ms)
schema = sphuling.read.schema_of("data/large_dataset.parquet")

# CSV sniffer
schema = sphuling.read.schema_of("data/transactions.csv", header=True, sep=",")

# JSON chunk sample
schema = sphuling.read.schema_of("data/events.json", sample_rows=100)
```

### 1.2 Parquet Files (`.parquet`)
Reads columnar Parquet files with native filter and projection pushdown:

```python
# Single file, glob, or directory
df = sphuling.read.parquet("data/sales.parquet")
df = sphuling.read.parquet("data/partitions/*.parquet")
df = sphuling.read.parquet("data/partitioned_directory/")
```

### 1.3 Arrow & Feather Files (`.arrow` / `.feather`)
Loads binary Arrow IPC / Feather files at zero-copy speeds:

```python
df = sphuling.read.feather("data/features.feather")
# or
df = sphuling.read.arrow("data/stream.arrow")
```

### 1.4 CSV Files (`.csv`)
Reads delimited text files with schema inference or an explicit schema:

```python
# Automatic header and type inference
df = sphuling.read.csv("data/users.csv", header=True, inferSchema=True, sep=",")

# With explicit schema (Fastest, zero type inference needed)
from pysphuling.sql import types as T
schema = T.StructType.fromDDL("id INT, username STRING, score DOUBLE")
df = sphuling.read.schema(schema).csv("data/users.csv", header=True)
```

### 1.5 JSON Files (`.json`)
Reads newline-delimited JSON (NDJSON) or JSON records:

```python
df = sphuling.read.json("data/events.json")
```

### 1.6 Generic Format Loader (`.format().load()`)
Standard PySpark generic format syntax:

```python
df = (
    sphuling.read
    .format("parquet")
    .option("compression", "snappy")
    .load("data/sales.parquet")
)
```

---

## 2. Writing Data (`DataFrameWriter` / `df.write`)

Access the writer through `df.write`:

```python
writer = df.write
```

### 2.1 Save Modes (`.mode()`)
Controls behavior when the target file or directory already exists:
- `"error"` / `"errorifexists"` (Default): Raises `FileExistsError` if target exists.
- `"overwrite"`: Overwrites existing data at target path.
- `"ignore"`: Silently ignores the write if target path exists.
- `"append"`: Appends new files to the directory.

```python
df.write.mode("overwrite").parquet("output/clean_data.parquet")
```

### 2.2 Partitioned Writes (`.partitionBy()`)
Writes Hive-partitioned directory hierarchies (`/department=Engineering/city=SF/`):

```python
(
    df.write
    .mode("overwrite")
    .partitionBy("department", "city")
    .parquet("output/partitioned_employees")
)
```

Directory structure created on disk:
```text
output/partitioned_employees/
├── department=Engineering/
│   ├── city=San Francisco/
│   │   └── part-0.parquet
│   └── city=New York/
│       └── part-0.parquet
└── department=Marketing/
    └── city=New York/
        └── part-0.parquet
```

### 2.3 Parquet Export (`.parquet`)
Saves data in Parquet format with configurable compression (`snappy`, `zstd`, `gzip`):

```python
df.write.parquet("output/data.parquet", compression="zstd")
```

### 2.4 Feather / Arrow Export (`.feather` / `.arrow`)
Saves data in Apache Arrow IPC format for instantaneous zero-copy reload:

```python
df.write.arrow("output/cache.feather")
```

### 2.5 CSV Export (`.csv`)
Saves data as comma-separated values:

```python
df.write.csv("output/export.csv", header=True, sep=",")
```

### 2.6 JSON Export (`.json`)
Saves records as newline-delimited JSON:

```python
df.write.json("output/records.json")
```

### 2.7 In-Memory Table Persistence (`.saveAsTable`)
Registers the output as a named table in the session catalog:

```python
df.write.saveAsTable("processed_transactions")
```

