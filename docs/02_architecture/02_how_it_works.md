# How Sphuling Works Under the Hood

This document walks through the exact operational lifecycle of a query in Sphuling, from the moment a user calls a transformation method to the physical streaming of SIMD vectors across CPU cores.

---

## 1. The Query Execution Lifecycle

```
[ Python User Code ]
       |
       v
1. AST Construction (Lazy)
   - User chains .filter(), .withColumn(), .groupBy()
   - Sphuling builds a LogicalPlan DAG
   - Time elapsed: ~1–2 ms (Zero data read)
       |
       v
2. Logical Optimization Passes
   - Projection Pushdown (Column Pruning)
   - Predicate Pushdown (Filter Pushdown)
   - Common Subexpression Elimination (CSE)
   - Limit Pushdown & Type Coercion
       |
       v
3. Action Triggered (.collect(), .show(), .toPandas())
   - Action signals the execution barrier
       |
       v
4. Physical Planning & Partitioning
   - LogicalPlan mapped to Physical ExecutionPlan
   - Repartitioning into N streams (sphuling.executor.cores)
   - Memory pool allocator verified
       |
       v
5. Multithreaded Native Execution
   - Parallel Rust threads scan Parquet/Arrow/CSV chunks
   - Vectorized SIMD expressions evaluated
   - Memory bounds monitored; disk spill triggered if needed
       |
       v
6. Zero-Copy Materialization
   - Output streams collected into Arrow RecordBatches
   - Transferred via Arrow C Data Interface into Python Row/Pandas/Table
```

---

## 2. Detailed Lifecycle Stages

### Stage 1: Lazy DAG Construction
When you call:
```python
df = sphuling.read.parquet("sales.parquet")
filtered = df.filter(F.col("amount") > 1000)
projected = filtered.select("customer_id", "amount", F.col("amount") * 0.1)
```

No data is loaded from disk. Instead:
1. `sphuling.read.parquet()` creates a `TableScan` logical plan node, querying only the Parquet footer metadata to know the column names and data types.
2. `.filter()` wraps the scan node with a `Filter` logical plan node containing the binary expression `amount > 1000`.
3. `.select()` wraps the filter node with a `Projection` logical plan node.
4. Each method returns a new `DataFrame` instance wrapping the modified logical plan tree in less than 2 milliseconds.

---

### Stage 2: Logical Plan Optimization
Before execution starts, Sphuling submits the logical DAG to DataFusion's optimizer pipeline, which runs several optimization passes:

#### 1. Projection Pushdown (Column Pruning)
If the original file has 50 columns but your query only selects 3 (`customer_id`, `amount`, and `discount`), the optimizer modifies the leaf `TableScan` node so the storage reader reads **only the byte chunks for those 3 columns**. The remaining 47 columns are completely ignored on disk.

#### 2. Predicate Pushdown (Filter Pushdown)
If the filter is `amount > 1000`, the filter expression is pushed directly into the Parquet reader. Parquet files store minimum and maximum statistics for every row group (e.g. 100,000 rows per group). If a row group's maximum `amount` is `850`, DataFusion skips reading and decompressing that entire row group!

#### 3. Common Subexpression Elimination (CSE)
If an expression like `F.col("salary") * 0.1` appears multiple times in different parts of a select statement, the optimizer computes it once, stores it in an internal register, and reuses the vector.

#### 4. Join Optimization
When joining two DataFrames:
- If one DataFrame is small, Sphuling automatically selects a **Broadcast Hash Join** (in-memory hash table built on the small side, probed in parallel by worker threads).
- If both are large, Sphuling repartitions both sides by join key hash and executes a **Partitioned Hash Join**.

---

### Stage 3: Physical Planning & Partitioning
When an action (such as `.collect()` or `.toPandas()`) is invoked:
1. The optimized logical plan is translated into a physical `ExecutionPlan`.
2. The physical plan determines execution concurrency:
   - DataFusion checks `sphuling.executor.cores` (e.g., 8 cores).
   - The dataset is split into 8 parallel stream partitions.
3. Memory boundaries are initialized via DataFusion's `GreedyMemoryPool`.

---

### Stage 4: Multithreaded Vectorized Physical Execution
1. **Threadpool Execution:** A threadpool of native Rust worker threads starts pulling batches of data from storage.
2. **Batch Processing:** Data is handled in columnar chunks (`batch_size=8192` rows by default). Each batch is represented as an Apache Arrow `RecordBatch`.
3. **SIMD Vectorization:** Operations like addition, string matching, or filter masks run using CPU SIMD instructions (processing 4 to 16 rows per CPU clock cycle).
4. **GIL-Free Execution:** Because the engine runs in native Rust and C++, all query processing occurs outside Python's Global Interpreter Lock (GIL). All physical CPU cores run at 100% capacity without Python thread contention.

---

### Stage 5: In-Process Zero-Copy Result Delivery
When processing completes:
1. Intermediate RecordBatches are concatenated into an Apache Arrow `Table`.
2. If `.toPandas()` was called, PyArrow passes the raw memory pointers directly into NumPy / Pandas memory blocks via the Arrow C Data Interface without serialization.
3. If `.collect()` was called, Sphuling maps the Arrow column arrays into a list of Python `Row` objects.

