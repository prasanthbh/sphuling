# Timeline of Development & Discoveries

## 2026-11-04: Full Documentation & README Remediation Following Technical Reviews
- **Claim Calibration & Realism**:
  - Toned down "100% PySpark Compatibility" to "High-fidelity single-node PySpark compatibility for core DataFrame and SQL workloads".
  - Created a dedicated, transparent compatibility matrix: `docs/04_api_reference/10_compatibility_matrix.md`, covering transformations, actions, `Window`, functions, data types, and deliberate exclusions (RDD, streaming).
  - Clarified execution core: established Apache DataFusion as the active physical execution engine, clarifying memory pool behavior and operator-specific disk spilling.
  - Corrected zero-copy claims: explicitly distinguished Arrow C Data Interface (zero-copy) from Pandas conversion (minimal copy / ArrowDtype).
  - Refined schema discovery terminology: separated metadata footer inspection ($O(1)$) for Parquet/Arrow from sample-based inference ($\le 64$ KB) for CSV/JSON.
- **Deep-Dive Competitor Expansions**:
  - Added dedicated comparative analysis for **Sphuling vs. DuckDB (Native SQL/Relational)** and **Sphuling vs. DuckDB Spark (`duckdb.experimental.spark`)**.
  - Documented DuckDB Spark's missing capabilities (no `Window` class, no `Column.over()`, no `df.na`, no `df.stat`, stubbed `SparkConf`).
- **Benchmark Precision**:
  - Updated `tests/benchmarks/benchmark_speed.py` to output platform and CPU details, clarifying in-memory synthetic workloads vs. disk I/O.
- **Validation**:
  - Executed benchmark: cold startup in 1.19 ms, in-memory 1M-row filter+agg in 6.40 ms.
  - Full test suite verified (34/34 passing).

## 2026-11-03: README.md Complete Overhaul & GitHub Documentation Links
- **README Structure Revamp**:
  - Structured the README with the 5 requested sections: Introduction, Why You Should Use It, Competitor Comparisons (matrix & deep-dives), Installation & Usage (native API, drop-in PySpark migration, Spark SQL), and Full Documentation Index.
  - Linked all documentation chapters and agent skills directly to GitHub repository URLs (`https://github.com/prasanthbh/sphuling/blob/main/docs/...`).
- **Validation**:
  - Verified formatting, links, and markdown structure.

## 2026-11-03: Relative Path Standardization Across All Documentation
- **Path Normalization**:
  - Audited all markdown documentation and source code to eliminate absolute paths and `file:///` hardcoded local URIs.
  - Converted all links in `docs/index.md` to clean relative paths (`./01_getting_started/...`, `../skills/sphuling/...`).
  - Added `02.03.5 Parallel Execution & Executor Architecture` link into `docs/index.md`.
  - Updated `docs/02_architecture/03.5_parallel_execution_and_executor_architecture.md` with relative links to `../../src/pysphuling/...`.
  - Updated `docs/code_reference/index.md` location links and scratch directories to relative paths (`../../src/pysphuling/...`, `./.sphuling/spill/`).
- **Validation**:
  - Full test suite verified (34/34 passing).

## 2026-11-03: Architectural Evaluation: Parallel Execution & Concurrency Model
- **Investigation & Findings**:
  - Investigated parallel execution mechanics in Sphuling (`pysphuling.core.engine.SphulingEngine` and `pysphuling.core.resources.parse_cores`).
  - Confirmed that Sphuling relies on Apache DataFusion's in-process morsel-driven parallelism via a native Tokio multi-threaded work-stealing runtime, mapping cores to `datafusion.execution.target_partitions`.
  - Clarified `master("local[N]")` vs `sphuling.executor.cores`: both configure physical CPU thread concurrency; `sphuling.executor.cores` acts as the explicit granular override over the high-level PySpark master URL.
  - Evaluated in-process multi-threading vs. multi-process executors: on a single machine, multi-process executors incur high IPC serialization, memory fragmentation, and startup latency (~1.3ms vs ~500ms+), whereas in-process native threads achieve 10x-50x higher throughput via zero-copy Arrow memory and GIL release in Rust.
  - Decision: Maintain in-process multi-threading as the primary execution engine; synthesize `spark.executor.instances * spark.executor.cores` for PySpark compatibility.

- **Documentation Restructuring**:
  - Renamed all documentation directories and files with chronological and sequential two-digit prefixes:
    - `01_getting_started/`: `01_introduction.md`, `02_installation.md`, `03_quickstart.md`.
    - `02_architecture/`: `01_overview.md`, `02_how_it_works.md`, `03_memory_and_resources.md`, `04_zero_load_schema.md`.
    - `03_comparison/`: `01_competitive_analysis.md`, `02_unique_advantages.md`.
    - `04_api_reference/`: `01_session.md`, `02_dataframe_transformations.md`, `03_dataframe_actions.md`, `04_column_and_expressions.md`, `05_functions.md`, `06_window.md`, `07_types.md`, `08_io_connectors.md`, `09_spark_sql.md`.
    - `05_migration/`: `01_from_pyspark.md`.
    - `06_publishing/`: `01_pypi_submission.md`.
    - `code_reference/`: Preserved for internal system reference.
  - Refreshed `docs/index.md` with explicit sequential reading flow: `01 -> 02 -> 03 -> 04 -> 05 -> 06`.
  - Updated all markdown links across `docs/index.md` and `README.md`.
- **Validation**:
  - Full test suite verified (34/34 passing).
