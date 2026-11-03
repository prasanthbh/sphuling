# System Architecture Overview

Sphuling is architected from the ground up as an **embedded, in-process, vectorized analytics engine**. It completely eliminates the traditional client-server JVM architecture used by Apache Spark, replacing it with direct native memory bridges.

---

## 1. High-Level Architectural Diagram

```
       +-----------------------------------------------------------------+
       |                  User Application / Pipeline                    |
       |  from pysphuling.sql import SphulingSession, functions as F     |
       +-----------------------------------------------------------------+
                                       |
                                       v
       +-----------------------------------------------------------------+
       |                  PySphuling API Compatibility Facade            |
       |  SphulingSession | DataFrame | Column | Window | Types | F.*   |
       +-----------------------------------------------------------------+
                                       |
                                       v
       +-----------------------------------------------------------------+
       |               Sphuling Logical Planner & AST Translator          |
       |  - Lazy Evaluation DAG Construction                             |
       |  - Spark SQL Dialect -> ANSI AST (sqlglot normalization)        |
       |  - Column Expression Translation to DataFusion Expr             |
       |  - Spark-Specific Behavioral Rewrites (Null-Safe, Cast Rules)   |
       +-----------------------------------------------------------------+
                                       |
                                       v
       +-----------------------------------------------------------------+
       |              Execution Engine Core (Apache DataFusion)          |
       |  - Rule-Based Optimizer (Filter/Projection/Limit Pushdown)      |
       |  - Cost-Based Physical Planner (Hash Joins, Sorting)            |
       |  - Multithreaded Vectorized Pipeline Execution (SIMD Rust)      |
       +-----------------------------------------------------------------+
                                       |
                                       v
       +-----------------------------------------------------------------+
       |                   Apache Arrow C Data Interface                 |
       |  - Zero-Copy RecordBatches via PyCapsule / C ABI                |
       |  - Seamless PyArrow and Minimal-Overhead Pandas Interop         |
       +-----------------------------------------------------------------+
                                       |
                                       v
       +-----------------------------------------------------------------+
       |              Storage, Connectors & Disk Spill Subsystem         |
       |  Parquet | Feather/Arrow IPC | CSV | JSON | NVMe Spill          |
       +-----------------------------------------------------------------+
```

---

## 2. Architectural Subsystems

### Subsystem 1: PySphuling API Facade
Provides PySpark API parity for single-node development:
- **`SphulingSession` (aliased as `SparkSession`):** Entry point managing execution contexts, configuration, and catalogs.
- **`DataFrame`:** Lazy abstraction exposing over 40 standard transformations (`select`, `filter`, `withColumn`, `join`, `groupBy`, `union`) and actions (`collect`, `show`, `count`, `toPandas`, `toArrow`).
- **`Column`:** Wraps DataFusion expressions with full Python arithmetic, logical, and comparison operator overloading (`+`, `-`, `*`, `/`, `==`, `!=`, `&`, `|`, `~`).
- **`functions` (aliased as `F`):** Standard PySpark SQL function catalog (strings, math, dates, windowing, aggregates, conditionals).
- **`types` (aliased as `T`):** PySpark data type hierarchy mapped to Apache Arrow physical layouts.

### Subsystem 2: Logical Planner & AST Translator
When a user writes PySpark code, Sphuling converts the Python expression graph into an optimized execution tree:
- **Expression Translation:** Translates `pysphuling.sql.Column` expressions and chained methods into native DataFusion `Expr` AST nodes.
- **Dialect Normalization:** PySpark SQL statements passed to `sphuling.sql(...)` are parsed with `sqlglot`, translating Spark-specific keywords (e.g. backticks, date functions, lateral views) into ANSI-compliant SQL.
- **Schema Reflection:** Maintains instantaneous schema knowledge through PyArrow type signatures without materializing data.

### Subsystem 3: Native Vectorized Engine (Apache DataFusion)
Rather than executing queries across distributed network worker daemons or JVM threads, Sphuling delegates execution to Apache DataFusion:
- **Rust SIMD Columnar Execution:** Processes data in columnar chunks (`RecordBatch`), leveraging CPU SIMD instructions (AVX-512, AVX2, NEON) for memory bandwidth maximization.
- **Pushdown Optimizations:** Pushes projections (column pruning) and predicates (filters) directly into Parquet row-group statistics, skipping unneeded byte reads entirely.
- **Parallel Work Stealing:** Uses a work-stealing threadpool scaled to `sphuling.executor.cores` to execute parallel tasks across physical CPU cores without Python GIL contention.

### Subsystem 4: In-Process Zero-Copy Memory Bridge
- PySpark uses sockets (`Py4J`) to exchange data between Python and the JVM for collections and UDFs.
- Sphuling utilizes the **Apache Arrow C Data Interface** (PyCapsule / C ABI). Python and Rust access the exact same contiguous physical memory buffers via memory pointers.
- Calling `.toArrow()` involves **zero memory copying**; exporting to Pandas achieves minimal overhead (zero-copy when ArrowDtype-compatible).

### Subsystem 5: Resource Governor & NVMe Disk Spilling
- Bounded memory pool allocator (`sphuling.executor.memory`) budgets RAM for active query execution.
- When aggregations, sorts, or large hash joins demand more memory than allocated, the engine automatically spills intermediate sorted partitions to local scratch NVMe storage (`./.sphuling/spill/`), preventing memory exhaustion.
