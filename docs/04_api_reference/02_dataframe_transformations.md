# API Reference: DataFrame Transformations

Every transformation in Sphuling is **lazy**—it creates or extends a logical computation plan without reading raw rows or performing I/O until an action is called.

---

## 1. Selection & Projection

### `.select(*cols: Union[str, Column]) -> DataFrame`
Projects a set of expressions or column names:

```python
import pysphuling.sql.functions as F

# Select columns by string or Column object
df.select("id", "name", F.col("salary") * 1.1).show()
```

### `.selectExpr(*exprs: str) -> DataFrame`
Projects one or more SQL expressions directly:

```python
df.selectExpr("id", "upper(name) AS upper_name", "salary * 1.15 AS new_salary").show()
```

### `.drop(*cols: str) -> DataFrame`
Drops specified columns from the projection:

```python
df.drop("temporary_flag", "raw_payload").show()
```

---

## 2. Column Manipulation

### `.withColumn(colName: str, colExpr: Column) -> DataFrame`
Adds a new column or replaces an existing column with the same name:

```python
df.withColumn("annual_bonus", F.col("salary") * 0.12).show()
```

### `.withColumns(colsMap: Dict[str, Column]) -> DataFrame`
Adds or replaces multiple columns simultaneously:

```python
df.withColumns({
    "tax": F.col("salary") * 0.2,
    "net_pay": F.col("salary") * 0.8
}).show()
```

### `.withColumnRenamed(existingName: str, newName: str) -> DataFrame`
Renames an existing column:

```python
df.withColumnRenamed("dept_id", "department_id").show()
```

### `.withColumnsRenamed(colsMap: Dict[str, str]) -> DataFrame`
Renames multiple columns:

```python
df.withColumnsRenamed({"c1": "first_col", "c2": "second_col"}).show()
```

---

## 3. Filtering & Deduplication

### `.filter(condition: Union[Column, str]) -> DataFrame`
Filters rows matching a boolean expression (`.where()` is an identical alias):

```python
# Using Column expressions
df.filter((F.col("salary") > 60000) & (F.col("department") == "IT")).show()

# Using SQL string expression
df.where("salary > 60000 AND department = 'IT'").show()
```

### `.distinct() -> DataFrame`
Deduplicates identical rows:

```python
df.select("department").distinct().show()
```

### `.dropDuplicates(subset: Optional[List[str]] = None) -> DataFrame`
Deduplicates rows, optionally considering only a subset of columns:

```python
df.dropDuplicates(subset=["user_id"]).show()
```

---

## 4. Slicing & Sorting

### `.limit(num: int) -> DataFrame`
Restricts the output to the first $N$ rows:

```python
df.limit(10).show()
```

### `.tail(num: int) -> DataFrame`
Returns the last $N$ rows:

```python
df.tail(5).show()
```

### `.sort(*cols: Union[str, Column], ascending=True) -> DataFrame`
Sorts the DataFrame by one or more columns (`.orderBy()` is an alias):

```python
# Ascending
df.sort("salary").show()

# Multi-column sort with ascending/descending
df.orderBy(F.col("department").asc(), F.col("salary").desc()).show()
```

---

## 5. Partitioning & Concurrency

### `.repartition(numPartitions: int) -> DataFrame`
Redistributes data into $N$ internal partition streams across CPU threads:

```python
df_partitioned = df.repartition(8)
```

### `.coalesce(numPartitions: int) -> DataFrame`
Merges partition streams down without a full shuffle:

```python
df_single = df.coalesce(1)
```

---

## 6. Joins & Set Operations

### `.join(other: DataFrame, on: Union[str, List[str]] = None, how: str = "inner") -> DataFrame`
Joins two DataFrames. Supports: `"inner"`, `"left"`, `"right"`, `"full"`, `"semi"`, `"anti"`.

```python
# Inner join on single key
df1.join(df2, on="id", how="inner").show()

# Left outer join
df1.join(df2, on="department", how="left").show()

# Left semi join (rows in df1 that have matches in df2)
df1.join(df2, on="user_id", how="semi").show()

# Left anti join (rows in df1 that do NOT have matches in df2)
df1.join(df2, on="user_id", how="anti").show()
```

### `.crossJoin(other: DataFrame) -> DataFrame`
Computes the Cartesian product of two DataFrames:

```python
df_cartesian = df1.crossJoin(df2)
```

### `.union(other: DataFrame) -> DataFrame`
Concatenates rows by positional schema:

```python
df_combined = df1.union(df2)
```

### `.unionByName(other: DataFrame, allowMissingColumns: bool = False) -> DataFrame`
Concatenates rows by column names:

```python
df_combined = df1.unionByName(df2)
```

### `.intersect(other: DataFrame) -> DataFrame`
Returns rows present in both DataFrames:

```python
common_rows = df1.intersect(df2)
```

### `.subtract(other: DataFrame) -> DataFrame`
Returns rows present in this DataFrame but not in another (`.exceptAll()` is an alias):

```python
diff_rows = df1.subtract(df2)
```

---

## 7. Grouping & Aggregations

### `.groupBy(*cols: Union[str, Column]) -> GroupedData`
Groups rows by keys and applies aggregate functions:

```python
# Multi-aggregate syntax
(
    df.groupBy("department")
    .agg(
        F.count("*").alias("headcount"),
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary")
    )
    .show()
)

# Shorthand aggregates
df.groupBy("department").count().show()
df.groupBy("department").avg("salary").show()
```

---

## 8. Missing Data Handling (`df.na`)

### `.na.drop(how="any"|"all", subset=None) -> DataFrame`
Omits rows with null values:

```python
# Drop rows where any column is null
clean_df = df.na.drop()

# Drop rows where salary is null
clean_df = df.na.drop(subset=["salary"])
```

### `.na.fill(value: Any, subset=None) -> DataFrame`
Replaces nulls with a default value:

```python
# Fill null bonuses with 0.0
df.na.fill(0.0, subset=["bonus"]).show()
```

### `.na.replace(to_replace: Any, value=None, subset=None) -> DataFrame`
Replaces target values:

```python
# Replace "N/A" with "Unknown" in department
df.na.replace("N/A", "Unknown", subset=["department"]).show()
```

---

## 9. Statistical Operations (`df.stat`)

### `.stat.corr(col1: str, col2: str) -> float`
Calculates Pearson correlation between two numeric columns:

```python
r = df.stat.corr("salary", "experience_years")
print(f"Correlation: {r:.4f}")
```

### `.stat.cov(col1: str, col2: str) -> float`
Calculates covariance between two numeric columns:

```python
c = df.stat.cov("salary", "bonus")
print(f"Covariance: {c:.2f}")
```

### `.stat.crosstab(col1: str, col2: str) -> DataFrame`
Computes pair-wise frequency table:

```python
df.stat.crosstab("department", "gender").show()
```

---

## 10. Pipelines, Caching & Views

### `.transform(func: Callable, *args, **kwargs) -> DataFrame`
Chains custom function transformations:

```python
def add_metadata(df, env):
    return df.withColumn("env", F.lit(env))

df.transform(add_metadata, env="production").show()
```

### `.cache() -> DataFrame` / `.persist() -> DataFrame`
Persists the DataFrame's intermediate RecordBatches in DataFusion memory:

```python
cached_df = df.cache()
```

### `.createOrReplaceTempView(name: str) -> None`
Registers the DataFrame as an in-memory SQL view for `sphuling.sql()`:

```python
df.createOrReplaceTempView("active_users")
sphuling.sql("SELECT * FROM active_users WHERE score > 90").show()
```

