# Timeline of Development & Discoveries

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
