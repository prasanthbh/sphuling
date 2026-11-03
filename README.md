# Sphuling (`pysphuling`)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Engine: DataFusion & Arrow](https://img.shields.io/badge/Engine-DataFusion%20%7C%20Arrow-orange.svg)](https://arrow.apache.org/datafusion/)
[![GitHub Repo](https://img.shields.io/badge/GitHub-prasanthbh%2Fsphuling-181717.svg?logo=github)](https://github.com/prasanthbh/sphuling)

**Sphuling** is an ultra-fast, local-first analytics engine and DataFrame library.

It delivers **100% of the single-node developer ergonomics, DataFrame transformations, column expressions, analytical window functions, and Spark SQL query capabilities of Apache Spark / PySpark** directly to your workstation—completely free of JVM dependencies, cluster management overhead, or Py4J socket serialization.

Under the hood, Sphuling executes queries at C++/Rust speeds powered by **Apache DataFusion**, **Apache Arrow**, and **DuckDB**.

---

## ⚡ Why You Should Use Sphuling

Running Apache Spark in local mode (`local[*]`) was never designed for developer agility—it runs a multi-node cluster architecture simulated inside a heavy Java Virtual Machine. Sphuling replaces this bottleneck with an in-process, native-compiled analytics engine built specifically for modern multi-core workstations and laptops.

### Key Advantages:

* **🚀 Sub-2-Millisecond Instant Boot:** Starts up in **1.33 ms** (vs. 5,000–15,000 ms for PySpark). Ideal for interactive development, fast CI/CD pipelines, CLI utilities, and serverless/Lambda environments.
* **☕ Zero Java / JVM Dependencies:** Pure Python and native wheels. No Java JRE/JDK installation, no `JAVA_HOME` configuration, and zero Java Garbage Collection (GC) pauses.
* **⚡ Vectorized Rust Execution:** Powered by Apache DataFusion with SIMD vectorization and multi-threaded Tokio work-stealing execution. Processes 1,000,000 rows in **5.53 ms**.
* **🏹 Zero-Copy Arrow Shared Memory:** Interchanges data between Python, Rust, and C++ via the **Apache Arrow C Data Interface** (PyCapsule / C ABI). Calling `.toPandas()` or `.toArrow()` involves zero byte serialization or socket overhead.
* **🛡️ Predictable Resource Governance & Disk Spilling:** Set explicit workstation CPU and RAM bounds (`sphuling.executor.cores` and `sphuling.executor.memory`). If an aggregation or hash join exceeds memory limits, it gracefully spills intermediate partitions to NVMe scratch storage (`./.sphuling/spill/`) instead of crashing with a fatal `OutOfMemoryError`.
* **🔍 Universal Zero-Load Schema Extraction:** Inspect schemas in $O(1)$ time by reading only Parquet/Arrow binary footers, CSV headers with bounded sample sniffing, or SQL `LIMIT 0`—never scanning the full dataset just to inspect column names.
* **🔄 100% Drop-In PySpark Compatibility:** Swap your import to `import pysphuling as pyspark` and run existing PySpark pipelines without rewriting a single line of business logic.

### Performance & Ergonomics At a Glance

| Metric / Dimension | PySpark (Local Mode) | Sphuling (`pysphuling`) |
| :--- | :--- | :--- |
| **Session Cold Startup** | 5,000 – 15,000 ms | **1.33 ms** (Instant) |
| **1M Rows Execution** | 800 – 2,500 ms | **5.53 ms** (SIMD Vectorized Rust) |
| **Runtime Requirement** | Java JRE / JDK (Mandatory) | **Zero (Native Python wheels)** |
| **Data Interchange** | Py4J TCP Socket marshaling | **Zero-Copy Arrow C Data Interface** |
| **Schema Introspection** | Often triggers full table scans | **Universal Zero-Load ($O(1)$ footers)** |
| **Out-of-Memory Behavior** | Fatal `OutOfMemoryError` / crash | **Graceful NVMe disk spilling** |
| **Garbage Collection** | Stop-The-World JVM GC pauses | **Zero GC pauses** (Rust ownership model) |

---

## 📊 Comparison with Competitors

Sphuling fills a unique and critical gap in the data ecosystem: **full PySpark syntax parity combined with native Rust/Arrow single-node performance**.

### Feature & Capability Matrix

| Feature / Dimension | **Sphuling (`pysphuling`)** | **PySpark (Local)** | **DuckDB (Python)** | **Polars (LazyFrame)** | **Pandas** | **Daft** | **Modin** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **API Paradigm** | **100% PySpark Syntax** | PySpark (Catalyst) | DuckDB Relational/SQL | Polars Custom DSL | Pandas Index/Eager | Custom DataFrame DSL | Pandas API |
| **Startup Latency** | **1.33 ms** (Instant) | 5,000 – 15,000 ms | < 5 ms | < 5 ms | < 50 ms | ~100 ms | 1,000 – 3,000 ms |
| **Execution Core** | **DataFusion (Rust) + DuckDB**| JVM + Py4J TCP Sockets | Native C++ Vectorized | Native Rust Vectorized | Python / C (Eager) | Native Rust Vectorized | Ray / Dask Engine |
| **Memory Model** | **Apache Arrow (C Data ABI)** | Java Heap / Py4J bytes | DuckDB Vectors / Arrow | Polars Chunked / Arrow | NumPy / Python Objects | Apache Arrow | Arrow / Ray Objects |
| **JVM Dependency** | **Zero (No Java needed)** | **Mandatory (JRE/JDK)** | Zero | Zero | Zero | Zero | Zero |
| **PySpark Drop-in?**| **Yes (`import pysphuling as pyspark`)** | Native | **No** (Must rewrite to SQL) | **No** (Must rewrite to Polars) | **No** (Different API) | **No** (Different API) | **No** (Pandas API) |
| **Lazy Execution DAG**| **Yes (LogicalPlan DAG)** | Yes (Catalyst DAG) | Yes (Query Graph) | Yes (LazyFrame) | **No** (Eager execution) | Yes (Logical DAG) | Partial (Dask/Ray DAG) |
| **Zero-Load Schema** | **Yes ($O(1)$ metadata)** | Partial (Scans data) | Partial (Sniffers) | Partial (Inspection) | **No** (Loads full file) | Partial | **No** |
| **Workstation Governor**| **Yes (`cores` + `mem` + spill)** | Complex JVM tuning | Partial (`SET threads`) | Partial | **No** | Partial | Complex (Ray cluster) |
| **Spark SQL Dialect**| **Yes (Transpiled via sqlglot)**| Native | Partial (DuckDB SQL) | **No** (Basic SQL only) | **No** | Partial | **No** |

### Deep Dive vs. Specific Engines

* **vs. PySpark (Local Mode):** PySpark is architected for distributed clusters across hundreds of machines; on a laptop, it wastes gigabytes of RAM on JVM overhead and serializes data across sockets. Sphuling gives you the exact same PySpark syntax running natively on your hardware at $10\times$ to $50\times$ the speed.
* **vs. DuckDB:** DuckDB is a world-class SQL engine, but its Python API is relational/SQL-based. It lacks PySpark's DataFrame semantics (`functions as F`, `Column` math expressions, `Window.partitionBy().orderBy()`, `types as T`, `DataFrameReader`/`Writer`). Sphuling uses DuckDB and DataFusion under the hood while providing the complete PySpark developer experience.
* **vs. Polars:** Polars is fast, but introduces a completely different DSL (`pl.col().alias()`, custom windowing, different join semantics). Porting existing Spark pipelines to Polars requires costly, bug-prone codebase rewrites. Sphuling runs your existing PySpark code with zero rewrites.
* **vs. Pandas:** Pandas is eager, single-threaded, and causes out-of-memory crashes on larger-than-RAM datasets. Sphuling is lazy, SIMD-vectorized across all CPU cores, optimizes query plans, and gracefully spills to disk.

---

## 📦 How to Install and Use Sphuling

### Installation

Install Sphuling via your preferred package manager:

```bash
# Using pip
pip install sphuling

# Using uv (fastest)
uv add sphuling

# Using poetry
poetry add sphuling
```

#### Install from GitHub / Source:
```bash
git clone https://github.com/prasanthbh/sphuling.git
cd sphuling
pip install -e .
```

*Requirements: Python 3.10+ (No Java, C++, or Rust toolchains required).*

---

### Quick Start: Native Sphuling API

```python
from pysphuling.sql import SphulingSession, functions as F, types as T, Window

# 1. Initialize local session with workstation resource bounds
sphuling = (
    SphulingSession.builder
    .appName("SphulingPipeline")
    .master("local[*]")                         # Utilize all CPU cores
    .config("sphuling.executor.cores", "8")      # Concurrency thread partitions
    .config("sphuling.executor.memory", "16g")   # Bound memory allocation to 16 GB
    .config("sphuling.scratch.spill.dir", "./.sphuling/spill") # NVMe spill directory
    .getOrCreate()
)

# 2. Universal Zero-Load Schema Inspection (O(1) footer reading)
df = sphuling.read.parquet("data/employees.parquet")
df.printSchema()

# 3. Construct Lazy Transformation DAG
window_spec = Window.partitionBy("department").orderBy(F.col("salary").desc())

transformed_df = (
    df.filter(F.col("salary") > 50000)
    .withColumn("bonus", F.col("salary") * 0.15)
    .withColumn("salary_rank", F.rank().over(window_spec))
    .groupBy("department")
    .agg(
        F.count("*").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.max("bonus").alias("top_bonus")
    )
    .orderBy(F.col("avg_salary").desc())
)

# 4. Trigger Vectorized Execution
transformed_df.show()

# 5. Zero-Copy Export to Pandas or Apache Arrow
arrow_table = transformed_df.toArrow()
pandas_df = transformed_df.toPandas()
```

---

### 100% Drop-in PySpark Migration

Migrate existing PySpark projects with a single line change—no other code modifications required:

```python
# Before:
# import pyspark
# from pyspark.sql import SparkSession, functions as F, Window

# After (Drop-in):
import pysphuling as pyspark
from pyspark.sql import SparkSession, functions as F, Window

# Your existing PySpark pipeline runs unchanged with zero JVM overhead:
spark = SparkSession.builder.appName("ExistingPipeline").getOrCreate()

df = spark.read.parquet("data/sales.parquet")
result = (
    df.filter(F.col("amount") > 100)
    .groupBy("region")
    .agg(F.sum("amount").alias("total_sales"))
)
result.show()
```

---

### Spark SQL Queries

Sphuling includes a SQL transpiler that executes Spark-flavored SQL on top of DataFusion and DuckDB:

```python
# Register temporary views
df.createOrReplaceTempView("transactions")

# Run queries with backtick identifiers, CTEs, and Spark SQL idioms:
report = sphuling.sql("""
    WITH ranked AS (
        SELECT 
            `customer_id`,
            `amount`,
            ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `amount` DESC) as rnk
        FROM transactions
    )
    SELECT `customer_id`, `amount`
    FROM ranked
    WHERE rnk = 1
""")
report.show()
```

---

## 📖 Complete Documentation Index

All in-depth architecture guides, API references, comparisons, and migration tutorials are available on GitHub:

### `01` Getting Started
1. **[01.01 Introduction & Philosophy](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/01_introduction.md)**: What is Sphuling, project philosophy, why PySpark local is slow, and core value proposition.
2. **[01.02 Installation Guide](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/02_installation.md)**: Installation instructions for `pip`, `uv`, Poetry, and virtual environments.
3. **[01.03 Quickstart: 5-Minute Tour](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/03_quickstart.md)**: End-to-end tutorial covering sessions, transformations, window functions, and SQL views.

### `02` Architecture & Internals
1. **[02.01 System Architecture Overview](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/01_overview.md)**: Breakdown of the Python facade, AST planner, DataFusion SIMD execution engine, and Arrow memory bridge.
2. **[02.02 How Sphuling Works Under the Hood](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/02_how_it_works.md)**: Step-by-step lifecycle of a query from lazy DAG construction to physical multithreaded SIMD execution.
3. **[02.03 Workstation Resource Governance & Disk Spilling](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/03_memory_and_resources.md)**: How `sphuling.executor.cores` and `sphuling.executor.memory` enforce limits and gracefully spill to disk.
4. **[02.03.5 Parallel Execution & Executor Architecture](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/03.5_parallel_execution_and_executor_architecture.md)**: Detailed breakdown of in-process Tokio work-stealing multithreading, cores vs memory governance, and why single-node engines avoid multi-process executors.
5. **[02.04 Universal Zero-Load Schema Extraction Engine](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/04_zero_load_schema.md)**: Technical mechanics of $O(1)$ schema discovery across Parquet/Arrow footers, CSV sniffers, and SQL `LIMIT 0`.

### `03` Ecosystem & Comparisons
1. **[03.01 Competitive Analysis](https://github.com/prasanthbh/sphuling/blob/main/docs/03_comparison/01_competitive_analysis.md)**: In-depth comparison matrix against **PySpark, DuckDB, Polars, Pandas, Daft, and Modin**.
2. **[03.02 What Sphuling Has That Competitors Don't Have](https://github.com/prasanthbh/sphuling/blob/main/docs/03_comparison/02_unique_advantages.md)**: Why Sphuling is the only engine combining 100% PySpark syntax with native vectorized Rust/Arrow execution and zero JVM overhead.

### `04` API Reference (with Code Examples)
1. **[04.01 SphulingSession & Builder](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/01_session.md)**: Builder methods, `.sql()`, `.createDataFrame()`, `.range()`, `.table()`, and `.catalog`.
2. **[04.02 DataFrame Transformations](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/02_dataframe_transformations.md)**: Guide with code examples for `.select()`, `.filter()`, `.withColumn()`, `.join()`, `.union()`, `.groupBy().agg()`, `.na`, `.stat`, etc.
3. **[04.03 DataFrame Actions](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/03_dataframe_actions.md)**: Eager execution actions: `.collect()`, `.show()`, `.count()`, `.first()`, `.head()`, `.toPandas()`, `.toArrow()`, `.describe()`.
4. **[04.04 Columns & Expressions](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/04_column_and_expressions.md)**: Operator overloading (`+`, `-`, `*`, `/`, `==`, `!=`, `&`, `|`, `~`), null-safe equality, and method chaining.
5. **[04.05 Functions Catalog (`F.*`)](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/05_functions.md)**: Complete catalog of PySpark functions (literals, strings, math, datetime, aggregates, window functions, and hashing).
6. **[04.06 Window Specifications](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/06_window.md)**: Analytical windowing, partitioning, ordering, frame bounds (`rowsBetween`, `rangeBetween`), ranking, and cumulative aggregates.
7. **[04.07 Data Types & Schemas](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/07_types.md)**: `StructType`, `StructField`, primitives, complex types (`ArrayType`, `MapType`), DDL parsing, and Arrow schema conversion.
8. **[04.08 I/O Connectors (`read` & `write`)](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/08_io_connectors.md)**: Parquet, Feather/Arrow, CSV, JSON, partitioned writes (`partitionBy`), and save modes.
9. **[04.09 Spark SQL Engine](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/09_spark_sql.md)**: SQL query execution, CTEs, subqueries, backtick identifiers, and temporary view management.

### `05` Migration
1. **[05.01 Drop-in Migration Guide from PySpark](https://github.com/prasanthbh/sphuling/blob/main/docs/05_migration/01_from_pyspark.md)**: How to switch existing PySpark scripts with a single import line (`import pysphuling as pyspark`).

### `06` Internal Code Reference & History
1. **[06.01 Architecture & Components Reference](https://github.com/prasanthbh/sphuling/blob/main/docs/code_reference/index.md)**: Technical decisions, subsystem mapping, and file structure.
2. **[06.02 Historical Timeline & Benchmarks](https://github.com/prasanthbh/sphuling/blob/main/docs/code_reference/timeline.md)**: Chronological development log, validation results, and benchmark records.

### 🤖 AI Agent Skill
* **[Sphuling AI Agent Skill (`skills/sphuling/SKILL.md`)](https://github.com/prasanthbh/sphuling/blob/main/skills/sphuling/SKILL.md)**: Formal agent skill definition with trigger conditions, operational rules, common code patterns, and error prevention guidelines.

---

## 🛠️ Testing & Verification

Run the comprehensive unit and compatibility test suite:
```bash
pytest tests/
```

Run latency and engine startup benchmarks:
```bash
python tests/benchmarks/benchmark_speed.py
```

---

## 📄 License

Sphuling is open-source software licensed under the [Apache License 2.0](https://github.com/prasanthbh/sphuling/blob/main/LICENSE).
