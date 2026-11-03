# Sphuling (`pysphuling`)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Engine: Apache DataFusion](https://img.shields.io/badge/Engine-Apache%20DataFusion-orange.svg)](https://arrow.apache.org/datafusion/)
[![GitHub Repo](https://img.shields.io/badge/GitHub-prasanthbh%2Fsphuling-181717.svg?logo=github)](https://github.com/prasanthbh/sphuling)

**Sphuling** (Bengali: স্ফুলিঙ্গ / Sanskrit: स्फुलिंग, meaning *"Spark"*) is an ultra-fast, local-first analytics engine and DataFrame library.

It delivers **high-fidelity single-node PySpark compatibility for core DataFrame and Spark SQL workloads** directly to your workstation—completely free of JVM dependencies, cluster management overhead, or Py4J socket serialization.

Under the hood, Sphuling executes queries at compiled Rust speeds powered by **Apache DataFusion** and **Apache Arrow**.

---

## ⚡ Why You Should Use Sphuling

Running Apache Spark in local mode (`local[*]`) was never designed for developer agility—it runs a cluster architecture simulated inside a heavy Java Virtual Machine. Sphuling replaces this bottleneck with an in-process, native-compiled analytics engine built specifically for modern multi-core workstations, laptops, and CI/CD runners.

### Core Value Propositions:

* **🚀 Sub-2-Millisecond Instant Boot:** Starts up in **1.33 ms** (vs. 5,000–15,000 ms for PySpark). Ideal for interactive development, fast CI/CD unit testing, CLI utilities, and serverless/Lambda environments.
* **☕ Zero Java / JVM Dependencies:** Pure Python API with native compiled wheels. No Java JRE/JDK installation, no `JAVA_HOME` configuration, and zero Java Garbage Collection (GC) pauses.
* **⚡ Vectorized Native Rust Execution:** Powered by Apache DataFusion with SIMD vectorization and multi-threaded Tokio work-stealing execution. In-memory synthetic 1M-row filter and aggregation queries complete in **5.53 ms** on an 8-core CPU.
* **🏹 Zero-Copy Arrow Shared Memory:** Interchanges data between Python, Rust, and C++ via the **Apache Arrow C Data Interface** (PyCapsule / C ABI). Calling `.toArrow()` involves zero memory copying; `.toPandas()` uses zero-copy memory buffers where ArrowDtype is supported.
* **🛡️ Predictable Resource Governance & Disk Spilling:** Set explicit workstation CPU and RAM bounds (`sphuling.executor.cores` and `sphuling.executor.memory`). DataFusion's `GreedyMemoryPool` manages memory budgets for heavy operators (external sort, hash join, hash aggregation) and spills intermediate partitions to NVMe scratch storage (`./.sphuling/spill/`).
* **🔍 Metadata-Driven & Fast Schema Inspection:** Inspect schemas in $O(1)$ time by reading binary footers for Parquet and Arrow IPC files, or using bounded sample sniffing for CSV/JSON—avoiding scanning full datasets just to discover column names.
* **🔄 High-Fidelity PySpark API Parity:** Write familiar PySpark code (`from pysphuling.sql import SparkSession, functions as F, Window, types as T`) with full support for analytical windowing, data cleaning (`df.na`), summary statistics (`df.stat`), and Spark SQL expressions.

---

## 📊 Comprehensive Competitor Comparison

Sphuling fills a unique and critical gap in the data ecosystem: **bringing familiar PySpark DataFrame ergonomics to a lightweight, single-node native execution engine**.

### Feature & Capability Matrix

| Feature / Dimension | **Sphuling (`pysphuling`)** | **PySpark (Local)** | **DuckDB (Native Python)** | **DuckDB Spark (`experimental.spark`)** | **Polars (LazyFrame)** | **Pandas** | **Daft** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **API Paradigm** | **PySpark-Compatible** | PySpark (Catalyst) | DuckDB Relational/SQL | Partial PySpark Emulation | Polars Custom DSL | Pandas Index/Eager | Custom DataFrame DSL |
| **Execution Engine** | **Apache DataFusion (Rust)**| JVM + Local Threads | Native C++ Vectorized | DuckDB C++ Engine | Native Rust Vectorized | Python / C (Eager) | Native Rust Vectorized |
| **Memory Model** | **Apache Arrow (C Data ABI)**| Java Heap / Py4J Socket | DuckDB Vectors / Arrow | DuckDB Vectors / Arrow | Polars Chunked / Arrow | NumPy / Python Objects | Apache Arrow |
| **JVM Dependency** | **Zero (Native Python wheel)**| **Mandatory (JRE/JDK)** | Zero | Zero | Zero | Zero | Zero |
| **PySpark Syntax Drop-in?**| **High Parity (Core APIs)** | Native | **No** (Must rewrite to SQL) | **Partial / Incomplete** | **No** (Must rewrite to Polars)| **No** (Different API) | **No** (Different API) |
| **Analytical Windowing (`Window`)**| **Supported (`Window.partitionBy`)**| Native | SQL Window functions | ❌ **Missing (`ImportError`)** | Polars `over()` DSL | ❌ Rolling / GroupBy only | Partial |
| **Data Cleaning (`df.na`)**| **Supported (`drop`, `fill`, `replace`)**| Native | SQL `COALESCE` / `CASE` | ❌ **Missing** | Polars `fill_null()` DSL | Pandas `.fillna()` / `.dropna()`| Partial |
| **Summary & Stats (`df.stat`)**| **Supported (`corr`, `crosstab`)**| Native | Custom SQL queries | ❌ **Missing** | Polars custom expressions | Pandas `.corr()` / `.describe()`| Partial |
| **Spark SQL Dialect**| **Transpiled via sqlglot** | Native | DuckDB SQL Dialect | DuckDB SQL Dialect | Basic SQL only | ❌ | Partial |
| **Configuration Engine** | **Active (`SphulingConf`)** | Native (`SparkConf`) | `SET key = val` | ❌ **Raises `NotImplementedError`**| Environment variables | ❌ | Config object |
| **Resource Bounds & Disk Spill** | **Configurable (`cores` + `memory`)**| Complex JVM heap tuning | `SET max_memory`, auto-spill | `SET max_memory`, auto-spill | Streaming out-of-core | ❌ Crashes on RAM limits | Spill support |

---

### Deep-Dive Comparisons

#### 1. Sphuling vs. Apache Spark (Local Mode)
* **PySpark Local Overhead:** Apache Spark was designed for multi-node clusters. When running locally, it spins up a full JVM, consumes 1–2 GB of baseline RAM, requires setting up Java environments, and incurs 5–15 second cold start delays.
* **The Sphuling Difference:** Sphuling runs in-process in native Rust. Queries execute without JVM boot delays or Py4J gateway handshakes.
* **When to choose PySpark:** Multi-node clusters spanning terabytes to petabytes of data, Structured Streaming, or integration with Hive/Hadoop clusters.
* **When to choose Sphuling:** Local ETL development, single-node pipelines, CI/CD testing, and serverless functions.

#### 2. Sphuling vs. DuckDB (Native SQL / Relational API)
* **DuckDB's Strengths:** DuckDB is an outstanding in-process analytical SQL database with vectorized C++ execution and out-of-core query processing.
* **The Gap for Spark Engineers:** DuckDB is fundamentally a SQL engine. Its Python API is relational and SQL-centric. It lacks PySpark DataFrame semantics: no `functions as F`, no `Column` expression arithmetic (`F.col("a") + F.col("b")`), no `Window.partitionBy().orderBy()`, no `types as T`, and no `DataFrameReader`/`Writer`.
* **Verdict:** Use DuckDB directly if your pipelines are written in pure SQL. Use Sphuling if you want PySpark DataFrame code, modular transformation functions, and familiar PySpark idioms running on top of modern native execution.

#### 3. Sphuling vs. DuckDB Spark (`duckdb.experimental.spark`)
While DuckDB includes an experimental PySpark module (`duckdb.experimental.spark`), it is an incomplete prototype with major limitations:
* **No `Window` Module:** `from duckdb.experimental.spark.sql import Window` raises an `ImportError`. `Column.over()` is not implemented. Standard PySpark windowing, ranking, and rolling aggregations cannot be run.
* **Missing DataFrame Utilities:** DuckDB Spark has no `df.na` (`drop`, `fill`, `replace`), no `df.stat` (`corr`, `cov`, `crosstab`), no `df.selectExpr()`, no `df.describe()`, no `df.repartition()`, and no `StructType.fromDDL()`.
* **Stubbed Configuration:** Classes like `SparkConf` and `RuntimeConfig` (`spark.conf`) raise `NotImplementedError` or `ContributionsAcceptedError`.
* **Sphuling Advantage:** Sphuling is **100% committed to PySpark developer ergonomics**, providing complete implementations of `Window`, `Column.over()`, `df.na`, `df.stat`, `df.selectExpr`, and active configuration parsing.

#### 4. Sphuling vs. Polars
* **Polars Strengths:** Polars is a fast, mature DataFrame library with an expressive custom Python DSL and streaming engine.
* **The Cost of Polars for Spark Teams:** Polars uses an entirely different syntax: `pl.col().alias()`, different method names, distinct join rules, and no Spark SQL dialect support. Migrating an existing PySpark codebase to Polars requires months of engineering rewrites.
* **Verdict:** Choose Polars for greenfield projects where you prefer the Polars DSL. Choose Sphuling when you have existing PySpark code, require Spark API familiarity, or want Spark SQL compatibility without Java.

#### 5. Sphuling vs. Pandas
* **Pandas Limitations:** Pandas primarily relies on eager execution (no logical query planning, no automatic filter pushdown, no projection pruning). On multi-gigabyte datasets, Pandas often creates intermediate memory copies, leading to out-of-memory crashes.
* **The Sphuling Difference:** Sphuling builds a lazy query plan, executes multithreaded SIMD operations across all CPU cores in compiled Rust, bounds execution memory with disk spilling, and provides sub-millisecond zero-copy `.toArrow()` / `.toPandas()` export when needed.

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

# 2. Fast Metadata-Driven Schema Inspection (O(1) footer reading)
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

# 4. Trigger Vectorized Native Execution
transformed_df.show()

# 5. Export to Pandas or Apache Arrow
arrow_table = transformed_df.toArrow()
pandas_df = transformed_df.toPandas()
```

---

### Drop-in PySpark Migration

Migrate existing PySpark code for supported DataFrame transformations:

```python
# Swap your import to pysphuling:
import pysphuling as pyspark
from pysphuling.sql import SparkSession, functions as F, Window

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

Sphuling includes a SQL transpiler that executes Spark-flavored SQL on top of DataFusion:

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

All architecture guides, API references, comparisons, and migration tutorials are available on GitHub:

### `01` Getting Started
1. **[01.01 Introduction & Philosophy](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/01_introduction.md)**: What is Sphuling, project philosophy, why PySpark local is slow, and core value proposition.
2. **[01.02 Installation Guide](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/02_installation.md)**: Installation instructions for `pip`, `uv`, Poetry, and virtual environments.
3. **[01.03 Quickstart: 5-Minute Tour](https://github.com/prasanthbh/sphuling/blob/main/docs/01_getting_started/03_quickstart.md)**: End-to-end tutorial covering sessions, transformations, window functions, and SQL views.

### `02` Architecture & Internals
1. **[02.01 System Architecture Overview](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/01_overview.md)**: Breakdown of the Python facade, AST planner, DataFusion SIMD execution engine, and Arrow memory bridge.
2. **[02.02 How Sphuling Works Under the Hood](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/02_how_it_works.md)**: Step-by-step lifecycle of a query from lazy DAG construction to physical multithreaded SIMD execution.
3. **[02.03 Workstation Resource Governance & Disk Spilling](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/03_memory_and_resources.md)**: How `sphuling.executor.cores` and `sphuling.executor.memory` enforce limits and gracefully spill to disk.
4. **[02.03.5 Parallel Execution & Concurrency Architecture](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/03.5_parallel_execution_and_executor_architecture.md)**: Detailed breakdown of in-process Tokio work-stealing multithreading, cores vs memory governance, and why single-node engines avoid multi-process executors.
5. **[02.04 Universal Zero-Load Schema Extraction Engine](https://github.com/prasanthbh/sphuling/blob/main/docs/02_architecture/04_zero_load_schema.md)**: Technical mechanics of schema discovery across Parquet/Arrow footers, CSV sniffers, and SQL `LIMIT 0`.

### `03` Ecosystem & Comparisons
1. **[03.01 Competitive Analysis](https://github.com/prasanthbh/sphuling/blob/main/docs/03_comparison/01_competitive_analysis.md)**: In-depth comparison matrix against **PySpark, DuckDB, DuckDB Spark, Polars, Pandas, and Daft**.
2. **[03.02 What Sphuling Has That Competitors Don't Have](https://github.com/prasanthbh/sphuling/blob/main/docs/03_comparison/02_unique_advantages.md)**: Why Sphuling is the only engine combining PySpark syntax with native vectorized Rust/Arrow execution and zero JVM overhead.

### `04` API Reference & Compatibility
1. **[04.01 SphulingSession & Builder](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/01_session.md)**: Builder methods, `.sql()`, `.createDataFrame()`, `.range()`, `.table()`, and `.catalog`.
2. **[04.02 DataFrame Transformations](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/02_dataframe_transformations.md)**: Guide with code examples for `.select()`, `.filter()`, `.withColumn()`, `.join()`, `.union()`, `.groupBy().agg()`, `.na`, `.stat`, etc.
3. **[04.03 DataFrame Actions](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/03_dataframe_actions.md)**: Eager execution actions: `.collect()`, `.show()`, `.count()`, `.first()`, `.head()`, `.toPandas()`, `.toArrow()`, `.describe()`.
4. **[04.04 Columns & Expressions](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/04_column_and_expressions.md)**: Operator overloading (`+`, `-`, `*`, `/`, `==`, `!=`, `&`, `|`, `~`), null-safe equality, and method chaining.
5. **[04.05 Functions Catalog (`F.*`)](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/05_functions.md)**: Complete catalog of PySpark functions (literals, strings, math, datetime, aggregates, window functions, and hashing).
6. **[04.06 Window Specifications](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/06_window.md)**: Analytical windowing, partitioning, ordering, frame bounds (`rowsBetween`, `rangeBetween`), ranking, and cumulative aggregates.
7. **[04.07 Data Types & Schemas](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/07_types.md)**: `StructType`, `StructField`, primitives, complex types (`ArrayType`, `MapType`), DDL parsing, and Arrow schema conversion.
8. **[04.08 I/O Connectors (`read` & `write`)](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/08_io_connectors.md)**: Parquet, Feather/Arrow, CSV, JSON, partitioned writes (`partitionBy`), and save modes.
9. **[04.09 Spark SQL Engine](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/09_spark_sql.md)**: SQL query execution, CTEs, subqueries, backtick identifiers, and temporary view management.
10. **[04.10 PySpark API Compatibility Matrix](https://github.com/prasanthbh/sphuling/blob/main/docs/04_api_reference/10_compatibility_matrix.md)**: Transparent support status and semantic parity matrix across all PySpark APIs.

### `05` Migration
1. **[05.01 Drop-in Migration Guide from PySpark](https://github.com/prasanthbh/sphuling/blob/main/docs/05_migration/01_from_pyspark.md)**: How to switch existing PySpark scripts with a single import line (`import pysphuling as pyspark`).

### `06` Publishing & Distribution
1. **[06.01 Publishing to PyPI Guide](https://github.com/prasanthbh/sphuling/blob/main/docs/06_publishing/01_pypi_submission.md)**: Step-by-step walkthrough to build distribution wheels, test on TestPyPI, publish to PyPI, and enable automated GitHub Actions releases.

### `07` Internal Code Reference & History
1. **[07.01 Architecture & Components Reference](https://github.com/prasanthbh/sphuling/blob/main/docs/code_reference/index.md)**: Technical decisions, subsystem mapping, and file structure.
2. **[07.02 Historical Timeline & Benchmarks](https://github.com/prasanthbh/sphuling/blob/main/docs/code_reference/timeline.md)**: Chronological development log, validation results, and benchmark records.

### 🤖 AI Agent Skill
* **[Sphuling AI Agent Skill (`skills/sphuling/SKILL.md`)](https://github.com/prasanthbh/sphuling/blob/main/skills/sphuling/SKILL.md)**: Formal agent skill definition with trigger conditions, operational rules, common code patterns, and error prevention guidelines.

---

## 🛠️ Testing & Verification

Run the comprehensive unit and compatibility test suite:
```bash
pytest tests/
```

Run latency and synthetic benchmark workloads:
```bash
python tests/benchmarks/benchmark_speed.py
```

---

## 📄 License

Sphuling is open-source software licensed under the [Apache License 2.0](https://github.com/prasanthbh/sphuling/blob/main/LICENSE).
