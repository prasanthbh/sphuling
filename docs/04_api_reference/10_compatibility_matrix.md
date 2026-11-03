# PySpark API Compatibility & Limitations Matrix

This document provides a transparent and detailed breakdown of PySpark API compatibility in **Sphuling (`pysphuling`)**. It highlights which DataFrame methods, functions, data types, and execution semantics are currently supported, partially supported, or out-of-scope.

---

## 1. Executive Compatibility Summary

| PySpark Module / Layer | Support Level | Implementation & Semantic Status |
| :--- | :---: | :--- |
| **`pyspark.sql.SparkSession`** | **High** | Session creation, builder options (`appName`, `master`, `config`), `.sql()`, `.table()`, `.range()`, `.createDataFrame()`. |
| **`pyspark.sql.DataFrame`** | **High** | Core transformations (`select`, `filter`/`where`, `withColumn`, `drop`, `distinct`, `join`, `union`, `groupBy`), actions (`collect`, `show`, `count`, `head`, `toPandas`, `toArrow`). |
| **`pyspark.sql.Window`** | **High** | `Window.partitionBy()`, `.orderBy()`, `.rowsBetween()`, `.rangeBetween()`, `Column.over()`. |
| **`pyspark.sql.functions` (`F`)** | **Core Library (~60+)** | Literals, math, string manipulation, date/time, aggregation, and ranking window functions. |
| **`pyspark.sql.types` (`T`)** | **High** | Primitives, `StructType`, `StructField`, `ArrayType`, `MapType`, Spark DDL string parser (`StructType.fromDDL`). |
| **Spark SQL Dialect** | **Transpiled** | ANSI and Spark SQL transpiled via `sqlglot` to DataFusion SQL; common queries, CTEs, subqueries supported. |
| **Data Cleaning (`df.na`)** | **High** | `df.na.drop()`, `df.na.fill()`, `df.na.replace()`. |
| **Summary & Stats (`df.stat`)** | **High** | `df.stat.corr()`, `df.stat.cov()`, `df.stat.crosstab()`, `df.describe()`, `df.summary()`. |
| **Python UDFs (`@udf`)** | **Planned / Under Design** | Standard Python row-level functions executing under the Python GIL. |
| **Low-Level RDDs** | **Out-of-Scope** | By design: Sphuling is focused exclusively on relational DataFrame and SQL execution. |
| **Structured Streaming** | **Out-of-Scope** | Sphuling is optimized for single-node batch analytics and local ETL workflows. |

---

## 2. Detailed DataFrame API Coverage

### A. Transformations

| Method | Status | Tested | Semantic Notes |
| :--- | :---: | :---: | :--- |
| `select(*cols)` | Supported | Yes | Column expressions and string names |
| `selectExpr(*exprs)` | Supported | Yes | SQL expressions evaluated lazily |
| `filter(condition)` / `where(condition)` | Supported | Yes | Boolean Column expressions |
| `withColumn(colName, col)` | Supported | Yes | Adds or replaces a column |
| `withColumns(colMap)` | Supported | Yes | Batch addition/replacement of columns |
| `withColumnRenamed(existing, new)` | Supported | Yes | Renames an existing column |
| `withColumnsRenamed(colMap)` | Supported | Yes | Batch column renaming |
| `drop(*cols)` | Supported | Yes | Drops specified columns |
| `distinct()` | Supported | Yes | Deduplicates rows across all columns |
| `dropDuplicates(subset)` | Supported | Yes | Deduplicates on full row or subset |
| `limit(n)` | Supported | Yes | Slices first $n$ rows |
| `tail(n)` | Supported | Yes | Returns the last $n$ rows as a list |
| `sort(*cols)` / `orderBy(*cols)` | Supported | Yes | Ascending / descending ordering with null handling |
| `join(other, on, how)` | Supported | Yes | `inner`, `left`, `right`, `full`, `semi`, `anti` |
| `crossJoin(other)` | Supported | Yes | Cartesian product join |
| `union(other)` / `unionAll(other)` | Supported | Yes | Positional union |
| `unionByName(other)` | Supported | Yes | Column-name-aligned union |
| `intersect(other)` / `intersectAll(other)`| Supported | Yes | Set intersection |
| `subtract(other)` / `exceptAll(other)` | Supported | Yes | Set difference |
| `groupBy(*cols)` | Supported | Yes | Returns `GroupedData` for aggregation |
| `transform(func, *args, **kwargs)` | Supported | Yes | Chains custom DataFrame transformation functions |
| `repartition(numPartitions, *cols)` | Supported | Yes | Maps to DataFusion physical repartitioning |
| `coalesce(numPartitions)` | Supported | Yes | Reduces partition stream concurrency |
| `sample(withReplacement, fraction, seed)`| Supported | Yes | Row sampling |
| `randomSplit(weights, seed)` | Supported | Yes | Splits DataFrame into multiple subsets |

---

### B. Actions & Output

| Action | Status | Tested | Semantic Notes |
| :--- | :---: | :---: | :--- |
| `collect()` | Supported | Yes | Materializes all records as `list[Row]` |
| `show(n, truncate, vertical)` | Supported | Yes | Formatted PySpark-style ASCII table output |
| `count()` | Supported | Yes | Eager row count |
| `first()` / `head(n)` / `take(n)` | Supported | Yes | Returns the first row or list of rows |
| `describe(*cols)` | Supported | Yes | Computes `count`, `mean`, `stddev`, `min`, `max` |
| `summary(*stats)` | Supported | Yes | Extended percentile and summary statistics |
| `toPandas()` | Supported | Yes | Exports to `pandas.DataFrame` (zero-copy when ArrowDtype compatible) |
| `toArrow()` | Supported | Yes | Zero-copy export to `pyarrow.Table` via Arrow C Data Interface |
| `explain(mode)` | Supported | Yes | Prints DataFusion logical and physical query plans |
| `cache()` / `persist()` | Supported | Yes | In-memory plan caching in DataFusion |

---

### C. Data Cleaning (`df.na`) & Statistics (`df.stat`)

| Method | Status | Tested | Semantic Notes |
| :--- | :---: | :---: | :--- |
| `df.na.drop(how, thresh, subset)` | Supported | Yes | Drops rows containing nulls (`any` or `all`) |
| `df.na.fill(value, subset)` | Supported | Yes | Replaces null values with specified scalar |
| `df.na.replace(to_replace, value, subset)` | Supported | Yes | Value substitution |
| `df.stat.corr(col1, col2, method)` | Supported | Yes | Pearson correlation coefficient |
| `df.stat.cov(col1, col2)` | Supported | Yes | Sample covariance |
| `df.stat.crosstab(col1, col2)` | Supported | Yes | Contingency table (cross-tabulation) |
| `df.stat.freqItems(cols, support)` | Supported | Yes | Frequent itemset calculation |

---

## 3. PySpark Functions (`pysphuling.sql.functions as F`)

Sphuling supports 60+ core standard library functions:

| Category | Supported Functions |
| :--- | :--- |
| **Literals & Identifiers** | `col()`, `column()`, `lit()` |
| **Arithmetic & Math** | `abs()`, `sqrt()`, `exp()`, `ln()`, `log()`, `log2()`, `log10()`, `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`, `ceil()`, `floor()`, `round()`, `signum()`, `pow()` |
| **String Manipulation** | `lower()`, `upper()`, `trim()`, `ltrim()`, `rtrim()`, `concat()`, `concat_ws()`, `substring()`, `length()`, `reverse()`, `initcap()`, `ascii()`, `soundex()`, `levenshtein()` |
| **Conditionals & Nulls** | `when().otherwise()`, `coalesce()`, `isnull()`, `isnan()`, `nanvl()` |
| **Aggregations** | `count()`, `countDistinct()`, `sum()`, `sumDistinct()`, `avg()`, `mean()`, `min()`, `max()`, `stddev()`, `stddev_samp()`, `stddev_pop()`, `variance()`, `var_samp()`, `var_pop()` |
| **Analytical Windowing** | `row_number()`, `rank()`, `dense_rank()`, `percent_rank()`, `cume_dist()`, `lag()`, `lead()`, `first()`, `last()` |
| **Hashing & Encoding** | `md5()`, `sha1()`, `sha2()` |
| **Pending / Under Development** | Complex array higher-order functions (`transform`, `aggregate`, `filter`), map creation functions (`create_map`), specialized geospatial/hyperloglog functions |

---

## 4. Analytical Windowing (`Window`)

| Method / Operator | Status | Tested | Semantic Notes |
| :--- | :---: | :---: | :--- |
| `Window.partitionBy(*cols)` | Supported | Yes | Groups rows into analytical partitions |
| `Window.orderBy(*cols)` | Supported | Yes | Orders partitions ascending or descending |
| `Window.rowsBetween(start, end)` | Supported | Yes | Physical row offsets (e.g., `Window.unboundedPreceding`) |
| `Window.rangeBetween(start, end)` | Supported | Yes | Value-based logical frame boundaries |
| `col.over(window_spec)` | Supported | Yes | Evaluates aggregate or ranking over window specification |

---

## 5. Architectural Semantic Differences from Apache Spark

While Sphuling mirrors PySpark's syntax and API signatures, data engineers should keep these execution differences in mind:

1. **In-Process Single-Node vs. Cluster Scheduling:**
   - Apache Spark schedules task stages over cluster managers (YARN, Mesos, Kubernetes) and uses distributed shuffle exchanges.
   - Sphuling executes entirely in-process using compiled Rust threads (Tokio runtime) and shared Arrow memory. There are no network shuffles, port allocations, or worker daemon heartbeats.
2. **Deterministic Rust Errors vs. Java Stack Traces:**
   - Instead of multi-page JVM `Py4JJavaError` stack traces, Sphuling returns concise, readable Python/Rust error messages pointing directly to column or type mismatches.
3. **Execution-Memory Pool vs. Entire Process Ceiling:**
   - `sphuling.executor.memory` bounds the memory available to active DataFusion execution operators (hash tables, sorts, aggregate buffers).
   - In-memory Arrow tables held in user Python variables (`table = df.toArrow()`) reside in Python heap space and are not evicted by the query engine's disk manager.
4. **ANSI SQL vs. Legacy Spark SQL Edge-Cases:**
   - Sphuling uses `sqlglot` to transpile Spark SQL into DataFusion ANSI SQL.
   - Standard queries, CTEs, window clauses, and expressions transpile seamlessly. However, certain legacy Spark SQL quirks (e.g., lenient implicit string-to-timestamp conversions or non-standard null ordering) may behave strictly according to ANSI standards in DataFusion.

