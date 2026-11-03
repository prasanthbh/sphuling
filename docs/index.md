# Sphuling Documentation Hub

Welcome to the documentation for **Sphuling (`pysphuling`)**—the high-performance, local-first analytics engine that brings the full API and developer ergonomics of Apache Spark / PySpark to single-node workstations, powered by Apache DataFusion, Apache Arrow, and DuckDB.

---

## 🧭 Recommended Sequential Reading Path

For developers and teams learning or evaluating Sphuling, we recommend reading through the documentation in the numbered sequence below:

```text
[ 01. Getting Started ] ──> [ 02. Architecture ] ──> [ 03. Comparison ]
                                                              │
                                                              v
[ 06. Publishing ] <── [ 05. Migration ] <── [ 04. API Reference ]
```

---

## 📑 Numbered Table of Contents

### `01` Getting Started
Start here to understand the core motivation, install Sphuling, and run your first pipeline:
1. **[`01.01` Introduction & Philosophy](./01_getting_started/01_introduction.md)**: What is Sphuling, the origin of its name (স্ফুলিঙ্গ / स्फुलिंग), why PySpark local is slow, and the core value proposition.
2. **[`01.02` Installation Guide](./01_getting_started/02_installation.md)**: Installing via `pip`, `uv`, Poetry, and local virtual environments (with zero C++/Rust compiler requirements).
3. **[`01.03` Quickstart: 5-Minute Tour](./01_getting_started/03_quickstart.md)**: Fast end-to-end tutorial covering sessions, transformations, window rankings, and SQL views.

---

### `02` Architecture & Internals
Learn how Sphuling executes queries at C++/Rust speeds directly in Python:
1. **[`02.01` System Architecture Overview](./02_architecture/01_overview.md)**: Detailed breakdown of the Python facade, logical AST planner, DataFusion SIMD execution engine, and Arrow C Data memory bridge.
2. **[`02.02` How Sphuling Works Under the Hood](./02_architecture/02_how_it_works.md)**: Step-by-step lifecycle of a query from lazy DAG construction to physical multithreaded SIMD execution.
3. **[`02.03` Workstation Resource Governance & Disk Spilling](./02_architecture/03_memory_and_resources.md)**: How `sphuling.executor.cores` and `sphuling.executor.memory` enforce limits and gracefully spill to disk instead of crashing.
4. **[`02.03.5` Parallel Execution & Executor Architecture](./02_architecture/03.5_parallel_execution_and_executor_architecture.md)**: In-depth breakdown of in-process Tokio multithreading, CPU thread concurrency vs executors, and performance benchmarks.
5. **[`02.04` Universal Zero-Load Schema Extraction](./02_architecture/04_zero_load_schema.md)**: Technical mechanics of $O(1)$ schema discovery across Parquet/Arrow footers, CSV sniffers, and SQL `LIMIT 0`.

---

### `03` Ecosystem & Comparisons
Understand how Sphuling compares to existing analytics engines:
1. **[`03.01` Competitive Analysis](./03_comparison/01_competitive_analysis.md)**: In-depth comparison matrix against **PySpark, DuckDB, Polars, Pandas, Daft, and Modin**.
2. **[`03.02` What Sphuling Has That Competitors Don't Have](./03_comparison/02_unique_advantages.md)**: Why Sphuling is the only engine combining 100% PySpark syntax with native vectorized Rust/Arrow execution and zero JVM overhead.

---

### `04` Complete API Reference (with Code Examples)
Detailed reference with copy-pasteable examples for every single feature:
1. **[`04.01` `SphulingSession` & Builder](./04_api_reference/01_session.md)**: All builder methods, `.sql()`, `.createDataFrame()`, `.range()`, `.table()`, and `.catalog`.
2. **[`04.02` DataFrame Transformations](./04_api_reference/02_dataframe_transformations.md)**: Complete guide with code examples for `.select()`, `.filter()`, `.withColumn()`, `.join()` (inner, left, semi, anti), `.union()`, `.groupBy().agg()`, `.na`, `.stat`, etc.
3. **[`04.03` DataFrame Actions](./04_api_reference/03_dataframe_actions.md)**: `.collect()`, `.show()`, `.count()`, `.first()`, `.head()`, `.toPandas()`, `.toArrow()`, `.describe()`.
4. **[`04.04` Columns & Expressions](./04_api_reference/04_column_and_expressions.md)**: Operator overloading (`+`, `-`, `*`, `/`, `==`, `!=`, `&`, `|`, `~`), null-safe equality (`eqNullSafe`), and method chaining.
5. **[`04.05` Functions Catalog (`pysphuling.sql.functions` as `F`)](./04_api_reference/05_functions.md)**: Complete catalog of PySpark functions (literals, strings, math, datetime, aggregates, window functions, and hashing).
6. **[`04.06` Window Specifications](./04_api_reference/06_window.md)**: Analytical windowing, partitioning, ordering, frame bounds (`rowsBetween`, `rangeBetween`), ranking, and cumulative sums.
7. **[`04.07` Data Types & Schemas](./04_api_reference/07_types.md)**: `StructType`, `StructField`, primitives, complex types, Spark DDL parsing (`StructType.fromDDL`), and Arrow schema conversion.
8. **[`04.08` I/O Connectors (`read` & `write`)](./04_api_reference/08_io_connectors.md)**: Parquet, Feather/Arrow, CSV, JSON, partitioned writes (`partitionBy`), and save modes.
9. **[`04.09` Spark SQL Engine](./04_api_reference/09_spark_sql.md)**: SQL query execution, CTEs, subqueries, backtick identifiers, and temporary view management.
10. **[`04.10` PySpark API Compatibility Matrix](./04_api_reference/10_compatibility_matrix.md)**: Comprehensive support status, limitations, and semantic parity matrix for all PySpark APIs.

---

### `05` Migration
1. **[`05.01` Drop-in Migration Guide from PySpark](./05_migration/01_from_pyspark.md)**: How to switch existing PySpark scripts with a single import line (`import pysphuling as pyspark`).

---

### `06` Publishing & Distribution
1. **[`06.01` Publishing to PyPI Guide](./06_publishing/01_pypi_submission.md)**: Step-by-step walkthrough to build distribution wheels, test on TestPyPI, publish to PyPI, and enable automated GitHub Actions releases so anyone can `pip install sphuling` or `uv add sphuling`.

---

### `07` Internal Code Reference & History
Internal reference maintained for developers and continuous maintenance:
1. **[`07.01` Architecture & Components Reference](./code_reference/index.md)**: Technical decisions, subsystem mapping, and file structure.
2. **[`07.02` Historical Timeline & Benchmarks](./code_reference/timeline.md)**: Chronological development log, validation results, and benchmark records.

---

### 🤖 AI Agent Skill
For autonomous AI coding agents and LLM tool calling:
- **[`skills/sphuling/SKILL.md`](../skills/sphuling/SKILL.md)**: Formal agent skill definition with trigger conditions, operational rules, common code patterns, and error prevention guidelines.
