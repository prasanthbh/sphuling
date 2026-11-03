# API Reference: Columns & Expressions

The `Column` class in Sphuling wraps an Apache DataFusion `Expr`, providing full Python operator overloading and expression manipulation matching PySpark.

---

## 1. Creating Column Objects

Columns can be referenced in three ways:

```python
import pysphuling.sql.functions as F

# 1. Using F.col() (Recommended)
c1 = F.col("salary")

# 2. DataFrame dictionary indexing
c2 = df["salary"]

# 3. DataFrame attribute access
c3 = df.salary
```

---

## 2. Operator Overloading

### Arithmetic Operators
Support column-to-column and column-to-scalar operations:
- `+` (addition): `F.col("salary") + F.col("bonus")`
- `-` (subtraction): `F.col("price") - 10`
- `*` (multiplication): `F.col("qty") * F.col("unit_price")`
- `/` (division): `F.col("revenue") / F.col("units")`
- `%` (modulo): `F.col("id") % 10`
- `-` (unary negation): `-F.col("amount")`

```python
df.withColumn("total_comp", F.col("salary") + F.col("bonus")).show()
```

### Comparison Operators
Generate boolean filter expressions:
- `==` (equality): `F.col("dept") == "Engineering"`
- `!=` (inequality): `F.col("status") != "CLOSED"`
- `<` (less than): `F.col("age") < 30`
- `<=` (less than or equal): `F.col("score") <= 100`
- `>` (greater than): `F.col("salary") > 80000`
- `>=` (greater than or equal): `F.col("rating") >= 4.5`

```python
df.filter(F.col("salary") >= 75000).show()
```

### Logical Operators
Chains boolean expressions:
- `&` (logical AND): `(F.col("age") > 25) & (F.col("salary") > 60000)`
- `|` (logical OR): `(F.col("city") == "NY") | (F.col("city") == "SF")`
- `~` (logical NOT): `~(F.col("is_active"))`

> **Note:** Always wrap individual conditions in parentheses `(...)` due to Python operator precedence!

```python
df.filter((F.col("age") > 21) & (F.col("is_registered") == True)).show()
```

### Null-Safe Equality (`eqNullSafe` / `<=>`)
Returns `True` if both sides match, or if **both sides are null**:

```python
df.filter(F.col("referrer_id").eqNullSafe(None)).show()
```

---

## 3. Column Methods

### `.alias(alias_name: str) -> Column`
Renames the output expression column:

```python
df.select((F.col("salary") * 1.1).alias("adjusted_salary")).show()
```

### `.cast(dataType: Union[DataType, str]) -> Column`
Casts column to another data type (supports strings like `"int"`, `"double"`, `"string"` or `T.IntegerType()`):

```python
from pysphuling.sql import types as T

df.select(
    F.col("raw_id").cast("int").alias("id_int"),
    F.col("timestamp_str").cast(T.TimestampType()).alias("ts")
).show()
```

### Sorting Specifications
- `.asc()`: Ascending order (nulls last)
- `.desc()`: Descending order (nulls first)
- `.asc_nulls_first()`: Ascending with nulls placed first
- `.asc_nulls_last()`: Ascending with nulls placed last
- `.desc_nulls_first()`: Descending with nulls placed first
- `.desc_nulls_last()`: Descending with nulls placed last

```python
df.orderBy(F.col("department").asc(), F.col("salary").desc_nulls_last()).show()
```

### Null & NaN Predicates
- `.isNull()`: Returns `True` if value is null
- `.isNotNull()`: Returns `True` if value is not null
- `.isnan()`: Returns `True` if floating-point value is NaN

```python
df.filter(F.col("bonus").isNull()).show()
```

### Set Membership & Range
- `.isin(*values)`: Tests if value is present in list
- `.between(lower, upper)`: Inclusive range test

```python
# Set inclusion
df.filter(F.col("department").isin("Engineering", "Marketing", "Sales")).show()

# Range test
df.filter(F.col("salary").between(60000, 95000)).show()
```

### String Search & Slicing
- `.startswith(prefix: str)`: String starts with prefix
- `.endswith(suffix: str)`: String ends with suffix
- `.contains(substr: str)`: Substring match
- `.substr(startPos: int, length: int)`: 1-based substring extraction

```python
df.filter(F.col("email").endswith("@company.com")).show()
df.select(F.col("phone").substr(1, 3).alias("area_code")).show()
```

### Complex Types Extraction
- `.getItem(key_or_idx)`: Array element access or Map lookup
- `.getField(fieldName)`: Struct field access

```python
# Array element access
df.select(F.col("scores").getItem(0).alias("first_score")).show()

# Struct field access
df.select(F.col("address").getField("city").alias("city")).show()
```

### Analytical Window Evaluation
- `.over(windowSpec: WindowSpec) -> Column`: Evaluates the column expression across an analytical window partition:

```python
from pysphuling.sql.window import Window

w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
df.withColumn("rank", F.rank().over(w)).show()
```

