# Workstation Resource Governance & Memory Spilling

Unlike Apache Spark, which requires configuring arcane JVM heap parameters, Sphuling provides simple, predictable workstation resource controls.

---

## 1. Resource Configuration Properties

You configure CPU cores and memory allocation during `SphulingSession` initialization:

```python
from pysphuling.sql import SphulingSession

sphuling = (
    SphulingSession.builder
    .appName("ResourceGovernedPipeline")
    .master("local[8]")                         # Use 8 physical cores
    .config("sphuling.executor.cores", "8")      # Explicit partition concurrency
    .config("sphuling.executor.memory", "16g")   # Bound memory allocation to 16 GB
    .config("sphuling.batch.size", "8192")       # 8,192 rows per vectorized batch
    .config("sphuling.scratch.spill.dir", "./.sphuling/spill") # Disk spill directory
    .getOrCreate()
)
```

### Property Reference

| Property | Default | Description |
| :--- | :--- | :--- |
| `sphuling.master` | `local[*]` | Master string: `local` (1 core), `local[N]` (N cores), `local[*]` (all physical CPU cores). |
| `sphuling.executor.cores` | All cores | Direct number of parallel thread partitions created in DataFusion. |
| `sphuling.executor.memory` | `4g` | Maximum RAM threshold before disk spilling is triggered. Supports `512m`, `8g`, `16gb`, etc. |
| `sphuling.batch.size` | `8192` | Number of rows processed together in a single SIMD vectorized RecordBatch. |
| `sphuling.scratch.spill.dir` | `./.sphuling/spill` | Scratch directory path for intermediate partition spilling on NVMe/SSD. |

*(Note: Legacy `spark.*` configuration keys such as `spark.executor.cores` and `spark.executor.memory` are automatically recognized and translated).*

---

## 2. Memory Pool Mechanics

Sphuling interfaces with Apache DataFusion's native memory management layer:

```
[ High-Memory Query: Massive GroupBy / Hash Join / Sort ]
                           |
                           v
           [ DataFusion Memory Pool Allocator ]
           Allocated RAM: 14 GB / Limit: 16 GB
                           |
                           +---> [ Incoming 3 GB Batch ]
                           |
                           v
              Does it exceed 16 GB Limit?
                           |
            +--------------+--------------+
            |                             |
         [ NO ]                        [ YES ]
            |                             |
      Keep in RAM                   Trigger Spill
            |                             |
      Fast Vectorized             Spill intermediate sorted
        Processing                partitions to NVMe SSD:
                                  ./.sphuling/spill/*.spill
                                          |
                                  Free RAM & continue
                                          |
                                  Merge spilled runs on read
```

### The `GreedyMemoryPool`
- Tracks every buffer allocated by aggregations, sort operations, and hash tables.
- Guarantees that query execution stays strictly within the byte limit derived from `sphuling.executor.memory`.
- If memory demands spike, it triggers the registered disk manager instead of throwing an unrecoverable system `OutOfMemoryError`.

---

## 3. NVMe Disk Spilling Subsystem

When queries exceed physical memory (e.g., sorting or aggregating a 50 GB dataset on an 8 GB workstation):

1. **Partition Serialization:** DataFusion flushes active in-memory hash tables or sort runs into temporary binary IPC files inside `sphuling.scratch.spill.dir`.
2. **Buffer Recycling:** Physical memory is immediately reclaimed for the next incoming partition batch.
3. **External Sort / Merge Aggregation:** When all partitions have been scanned, the engine reads back the spilled runs via a multi-way external merge algorithm.
4. **Automatic Cleanup:** When the `SphulingSession` stops or the action finishes, temporary scratch files are purged automatically.

---

## 4. Comparison: PySpark JVM vs. Sphuling Resource Model

| Feature | Legacy PySpark (Local) | Sphuling (`pysphuling`) |
| :--- | :--- | :--- |
| **Configuration** | Complex: `-Xmx`, `spark.driver.memory`, `spark.memory.fraction`, `spark.memory.storageFraction` | Simple: `sphuling.executor.memory="8g"` |
| **OOM Behavior** | Fatal `java.lang.OutOfMemoryError`, crash JVM, kill driver | Graceful disk spill to NVMe SSD |
| **Garbage Collection** | Java GC pauses (Stop-The-World latency) | **Zero GC pauses** (Rust deterministic ownership) |
| **Physical Concurrency** | Restricted by GIL (Python) + JVM threads | **100% native CPU core utilization** without GIL |

