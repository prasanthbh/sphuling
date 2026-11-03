# Drop-in Migration Guide: Moving from PySpark to Sphuling

Migrating an existing PySpark pipeline to Sphuling requires **zero architectural rewrites**.

---

## 1. Zero-Friction Drop-in Option (Single Line Change)

Because Sphuling registers dynamic module aliases, you can convert your script with a single import swap:

### Before (PySpark):
```python
import pyspark
from pyspark.sql import SparkSession, functions as F, types as T, Window

spark = SparkSession.builder.appName("MyPipeline").getOrCreate()
df = spark.read.parquet("input_data/")
result = df.filter(F.col("salary") > 50000).groupBy("dept").agg(F.avg("salary"))
result.show()
```

### After (Sphuling Drop-in):
```python
# Swap imports to pysphuling:
import pysphuling as pyspark
from pysphuling.sql import SparkSession, functions as F, types as T, Window

# Your pipeline runs with zero JVM overhead:
spark = SparkSession.builder.appName("MyPipeline").getOrCreate()
df = spark.read.parquet("input_data/")
result = df.filter(F.col("salary") > 50000).groupBy("dept").agg(F.avg("salary"))
result.show()
```

*(See the full [PySpark Compatibility Matrix](../04_api_reference/10_compatibility_matrix.md) for details on supported methods, functions, and semantic differences).*

---

## 2. Direct Sphuling Syntax (Recommended for New Code)

For new pipelines, use Sphuling's native class names:

```python
from pysphuling.sql import SphulingSession, functions as F, types as T, Window

sphuling = (
    SphulingSession.builder
    .appName("MyPipeline")
    .master("local[8]")
    .config("sphuling.executor.cores", "8")
    .config("sphuling.executor.memory", "16g")
    .getOrCreate()
)

df = sphuling.read.parquet("input_data/")
result = df.filter(F.col("salary") > 50000).groupBy("dept").agg(F.avg("salary"))
result.show()
```

---

## 3. Configuration Mapping Reference

| PySpark Legacy Configuration | Sphuling Equivalent | Sphuling Behavior |
| :--- | :--- | :--- |
| `spark.master = "local[4]"` | `sphuling.master = "local[4]"` | Sets 4 native Rust execution partitions |
| `spark.executor.cores = "8"` | `sphuling.executor.cores = "8"` | Slices data across 8 parallel CPU threads |
| `spark.executor.memory = "16g"` | `sphuling.executor.memory = "16g"` | Sets native memory pool to 16 GB + spills to disk |
| `spark.sql.shuffle.partitions` | `sphuling.sql.shuffle.partitions` | Configures join/aggregate partition count |
| `spark.driver.memory` | Not needed (No JVM) | Uses workstation memory pool directly |
| `spark.driver.maxResultSize` | Not needed (No JVM) | Bounded by workstation RAM |
| `spark.memory.fraction` | Not needed (No JVM) | Automatically managed by DataFusion pool |

*(Note: Sphuling automatically accepts `spark.*` configuration keys and translates them behind the scenes).*

---

## 4. Behavior Differences & Exclusions

1. **No RDD API:**
   - In PySpark: `df.rdd.map(lambda row: ...)` passes Python objects through Java serialization.
   - In Sphuling: RDD is excluded. Use vectorized transformations (`withColumn`, `select`), `.toPandas()`, or `.toArrow()` to operate directly on Arrow memory.
2. **Instant Startup Time:**
   - Instead of waiting 10–15 seconds for the JVM, your script executes within **1.33 milliseconds**.
3. **Zero Java Dependency:**
   - You can uninstall Java from your machine or Docker container, reducing container image size by over 500 MB.

