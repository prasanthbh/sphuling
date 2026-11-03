# API Reference: `SphulingSession`

The `SphulingSession` (and drop-in alias `SparkSession`) is the entry point for programming with the DataFrame and SQL APIs in Sphuling.

```python
from pysphuling.sql import SphulingSession
# or
from pysphuling.sql import SparkSession
```

---

## 1. Builder Methods (`SphulingSession.builder`)

### `.appName(name: str) -> Builder`
Sets a name for the application, displayed in logs and monitoring.

```python
spark = SphulingSession.builder.appName("SalesAnalysis").getOrCreate()
```

### `.master(master: str) -> Builder`
Sets the local execution parallelism:
- `"local"`: 1 worker thread.
- `"local[N]"`: N parallel worker threads.
- `"local[*]"`: Uses all available physical CPU cores.

```python
spark = SphulingSession.builder.master("local[8]").getOrCreate()
```

### `.config(key: str, value: Any) -> Builder`
Sets configuration properties for cores, memory limits, batch size, and disk spilling.

```python
spark = (
    SphulingSession.builder
    .config("sphuling.executor.cores", "8")
    .config("sphuling.executor.memory", "16g")
    .config("sphuling.scratch.spill.dir", "./.sphuling/spill")
    .config("sphuling.batch.size", "8192")
    .getOrCreate()
)
```

### `.enableHiveSupport() -> Builder`
Enables persistent catalog support backed by DuckDB metadata.

```python
spark = SphulingSession.builder.enableHiveSupport().getOrCreate()
```

### `.getOrCreate() -> SphulingSession`
Gets an existing `SphulingSession` or initializes a new one.

```python
spark = SphulingSession.builder.getOrCreate()
```

---

## 2. Session Methods

### `.sql(sqlQuery: str) -> DataFrame`
Executes a SQL query string (supporting Spark SQL dialect) and returns a `DataFrame`.

```python
df = spark.sql("SELECT 1 AS id, 'Alice' AS name, 50000 * 1.1 AS new_salary")
df.show()
```

### `.createDataFrame(data: Any, schema=None) -> DataFrame`
Creates a DataFrame from various data structures:

#### From Pandas DataFrame:
```python
import pandas as pd
pdf = pd.DataFrame({"id": [1, 2], "val": [10.5, 20.0]})
df = spark.createDataFrame(pdf)
```

#### From List of Tuples with Column Names:
```python
data = [(1, "HR"), (2, "Engineering")]
df = spark.createDataFrame(data, schema=["id", "department"])
```

#### From List of Dicts:
```python
data = [{"user": "Alice", "score": 95}, {"user": "Bob", "score": 88}]
df = spark.createDataFrame(data)
```

#### From PyArrow Table:
```python
import pyarrow as pa
table = pa.Table.from_pydict({"x": [1, 2, 3]})
df = spark.createDataFrame(table)
```

### `.range(start: int, end: int = None, step: int = 1, numPartitions: int = None) -> DataFrame`
Generates a DataFrame with a single column `id` containing an integer sequence:

```python
# Numbers 0 to 999,999 partitioned across 4 threads
df = spark.range(0, 1_000_000, step=1, numPartitions=4)
print(df.count()) # 1000000
```

### `.table(tableName: str) -> DataFrame`
Returns an existing in-memory table or view as a DataFrame:

```python
df.createOrReplaceTempView("customers")
reloaded = spark.table("customers")
```

### `.read -> DataFrameReader`
Returns a `DataFrameReader` for loading files (Parquet, CSV, JSON, Arrow):

```python
df = spark.read.parquet("data/*.parquet")
```

### `.catalog -> Catalog`
Returns the metadata catalog interface for querying tables and views:

```python
tables = spark.catalog.listTables()
print([t.name for t in tables])
```

### `.stop() -> None`
Stops the session, closes worker threadpools, and cleans up temporary spill scratch files:

```python
spark.stop()
```

