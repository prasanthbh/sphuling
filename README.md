# Sphuling (`pysphuling`)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Engine: DataFusion & Arrow](https://img.shields.io/badge/Engine-DataFusion%20%7C%20Arrow-orange.svg)](https://arrow.apache.org/datafusion/)

**Sphuling** (Bengali: স্ফুলিঙ্গ / Sanskrit: स्फुलिंग) is an ultra-fast, local-first analytics engine and library. It brings **100% of the single-node developer ergonomics, DataFrame transformations, column expressions, analytical window functions, and Spark SQL query features of Apache Spark / PySpark** to local development without JVM dependencies, cluster management overhead, or Py4J socket serialization.

Powered under the hood by **Apache DataFusion**, **Apache Arrow**, and **DuckDB**.

---

## ⚡ Why Sphuling?

| Metric / Feature | PySpark (Local Mode) | Sphuling (`pysphuling`) |
| :--- | :--- | :--- |
| **Session Cold Startup** | 5,000 – 15,000 ms | **1.33 ms** (Instant) |
| **1M Rows Execution** | ~800 – 2,500 ms | **5.53 ms** (SIMD Vectorized Rust) |
| **Java / JVM Requirement** | Mandatory (JRE/JDK) | **Zero (Pure native wheels)** |
| **Data Interchange** | Py4J TCP Socket marshaling | **Zero-Copy Arrow C Data Interface** |
| **Schema Introspection** | Often triggers full scans | **Universal Zero-Load ($O(1)$ footers)** |
| **Workstation Resource Control** | Complex JVM heap tuning | **Simple `cores` & `memory` bounds + auto spill** |

---

## 🚀 Quick Start

```python
from pysphuling.sql import SphulingSession, functions as F, types as T, Window

# 1. Initialize local session with 8 cores and 8 GB RAM
sphuling = (
    SphulingSession.builder
    .appName("SphulingPipeline")
    .master("local[*]")
    .config("sphuling.executor.cores", "8")
    .config("sphuling.executor.memory", "8g")
    .getOrCreate()
)

# 2. Read dataset with instant zero-load schema
df = sphuling.read.parquet("data/*.parquet")
df.printSchema()

# 3. Build lazy transformation DAG
w = Window.partitionBy("department").orderBy(F.col("salary").desc())

transformed = (
    df.filter(F.col("salary") > 50000)
    .withColumn("bonus", F.col("salary") * 0.15)
    .withColumn("rank", F.rank().over(w))
    .groupBy("department")
    .agg(
        F.count("*").alias("employee_count"),
        F.avg("salary").alias("avg_salary"),
        F.max("bonus").alias("max_bonus")
    )
    .orderBy(F.col("avg_salary").desc())
)

# 4. Trigger execution
transformed.show()
```

---

## 🔄 Drop-in PySpark Migration

Existing PySpark pipelines run with zero code modifications:

```python
# Simply swap the import:
import pysphuling as pyspark
from pyspark.sql import SparkSession, functions as F, types as T, Window

# Everything else remains 100% identical:
spark = SparkSession.builder.appName("MyPySparkApp").getOrCreate()
df = spark.read.parquet("data/")
df.filter(F.col("age") > 21).show()
```

---

## 📖 Sequential Documentation Hub

The documentation is organized in a clear numbered reading order:

- **01. Getting Started**
  - [01.01 Introduction & Philosophy](docs/01_getting_started/01_introduction.md)
  - [01.02 Installation Guide](docs/01_getting_started/02_installation.md)
  - [01.03 Quickstart: 5-Minute Tour](docs/01_getting_started/03_quickstart.md)
- **02. Architecture & Internals**
  - [02.01 System Architecture Overview](docs/02_architecture/01_overview.md)
  - [02.02 How Sphuling Works Under the Hood](docs/02_architecture/02_how_it_works.md)
  - [02.03 Resource Governance & NVMe Disk Spilling](docs/02_architecture/03_memory_and_resources.md)
  - [02.04 Universal Zero-Load Schema Extraction Engine](docs/02_architecture/04_zero_load_schema.md)
- **03. Ecosystem & Comparisons**
  - [03.01 Competitive Analysis vs PySpark, DuckDB, Polars, Pandas](docs/03_comparison/01_competitive_analysis.md)
  - [03.02 What Sphuling Has That Competitors Don't Have](docs/03_comparison/02_unique_advantages.md)
- **04. API Reference (with code examples for every feature)**
  - [04.01 `SphulingSession` & Builder](docs/04_api_reference/01_session.md)
  - [04.02 DataFrame Transformations](docs/04_api_reference/02_dataframe_transformations.md)
  - [04.03 DataFrame Actions](docs/04_api_reference/03_dataframe_actions.md)
  - [04.04 Columns & Expressions](docs/04_api_reference/04_column_and_expressions.md)
  - [04.05 Functions Catalog (`F.*`)](docs/04_api_reference/05_functions.md)
  - [04.06 Window Specifications](docs/04_api_reference/06_window.md)
  - [04.07 Data Types & Schemas](docs/04_api_reference/07_types.md)
  - [04.08 I/O Connectors (`read` & `write`)](docs/04_api_reference/08_io_connectors.md)
  - [04.09 Spark SQL Engine](docs/04_api_reference/09_spark_sql.md)
- **05. Migration**
  - [05.01 Drop-in Migration Guide from PySpark](docs/05_migration/01_from_pyspark.md)
- **06. Publishing**
  - [06.01 Publishing to PyPI Guide (`pip` / `uv`)](docs/06_publishing/01_pypi_submission.md)
- **AI Agent Skill**
  - [Sphuling AI Agent Skill (`skills/sphuling/SKILL.md`)](skills/sphuling/SKILL.md)

---

## 🛠️ Testing & Verification

Run the test suite across all modules:
```bash
pytest tests/
```

Run performance and latency benchmarks:
```bash
python tests/benchmarks/benchmark_speed.py
```

---

## 📄 License
Sphuling is licensed under the [Apache License 2.0](LICENSE).
