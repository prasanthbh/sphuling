# API Reference: `pysphuling.sql.functions` (`F.*`)

Sphuling provides a complete PySpark-compatible standard functions catalog under `pysphuling.sql.functions` (typically imported as `import pysphuling.sql.functions as F`).

```python
import pysphuling.sql.functions as F
```

---

## 1. Literals & Conditionals

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.col(name: str)` | References a column by name | `F.col("salary")` |
| `F.lit(value: Any)` | Creates a constant literal column | `F.lit(100)`, `F.lit("active")` |
| `F.when(cond, val).otherwise(val)` | SQL `CASE WHEN ... THEN ... ELSE` | `F.when(F.col("age") >= 18, "Adult").otherwise("Minor")` |
| `F.coalesce(*cols)` | Returns the first non-null expression | `F.coalesce(F.col("mobile"), F.col("phone"), F.lit("None"))` |
| `F.greatest(*cols)` | Returns the maximum value of expressions | `F.greatest(F.col("q1"), F.col("q2"), F.col("q3"))` |
| `F.least(*cols)` | Returns the minimum value of expressions | `F.least(F.col("cost_a"), F.col("cost_b"))` |
| `F.nanvl(col1, col2)` | Returns `col1` if not NaN, or `col2` if NaN | `F.nanvl(F.col("ratio"), F.lit(0.0))` |

```python
# Chained conditional grading example
grade_expr = (
    F.when(F.col("score") >= 90, "A")
    .when(F.col("score") >= 80, "B")
    .when(F.col("score") >= 70, "C")
    .otherwise("F")
)
df.withColumn("grade", grade_expr).show()
```

---

## 2. String Manipulation Functions

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.concat(*cols)` | Concatenates string columns | `F.concat(F.col("first"), F.lit(" "), F.col("last"))` |
| `F.concat_ws(sep, *cols)` | Concatenates with separator | `F.concat_ws("-", F.col("year"), F.col("month"))` |
| `F.lower(col)` | Converts to lowercase | `F.lower(F.col("email"))` |
| `F.upper(col)` | Converts to uppercase | `F.upper(F.col("state"))` |
| `F.trim(col)` | Trims leading and trailing spaces | `F.trim(F.col("raw_text"))` |
| `F.ltrim(col)` | Trims leading spaces | `F.ltrim(F.col("text"))` |
| `F.rtrim(col)` | Trims trailing spaces | `F.rtrim(F.col("text"))` |
| `F.initcap(col)` | Capitalizes first letter of each word | `F.initcap(F.col("title"))` |
| `F.length(col)` | Returns character length | `F.length(F.col("description"))` |
| `F.substring(col, pos, len)` | 1-based substring | `F.substring(F.col("phone"), 1, 3)` |
| `F.split(col, pattern)` | Splits string around regex pattern | `F.split(F.col("tags"), ",")` |
| `F.regexp_replace(col, pat, rep)` | Replaces regex matches | `F.regexp_replace(F.col("text"), r"\s+", " ")` |
| `F.repeat(col, n)` | Repeats string $N$ times | `F.repeat(F.col("char"), 5)` |
| `F.reverse(col)` | Reverses characters in string | `F.reverse(F.col("code"))` |
| `F.ascii(col)` | ASCII numeric code of first char | `F.ascii(F.col("letter"))` |

---

## 3. Math & Numeric Functions

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.abs(col)` | Absolute value | `F.abs(F.col("diff"))` |
| `F.sqrt(col)` | Square root | `F.sqrt(F.col("variance"))` |
| `F.cbrt(col)` | Cube root | `F.cbrt(F.col("volume"))` |
| `F.exp(col)` | Exponential ($e^x$) | `F.exp(F.col("growth_rate"))` |
| `F.log(col)` | Natural logarithm | `F.log(F.col("p"))` |
| `F.log10(col)` | Base 10 logarithm | `F.log10(F.col("intensity"))` |
| `F.log2(col)` | Base 2 logarithm | `F.log2(F.col("bits"))` |
| `F.pow(col1, col2)` | Exponential power ($x^y$) | `F.pow(F.col("base"), F.lit(2))` |
| `F.round(col, scale=0)` | Rounds to decimal places | `F.round(F.col("tax"), 2)` |
| `F.floor(col)` | Largest integer $\le x$ | `F.floor(F.col("val"))` |
| `F.ceil(col)` | Smallest integer $\ge x$ | `F.ceil(F.col("val"))` |
| `F.sin`, `F.cos`, `F.tan` | Trigonometric functions | `F.sin(F.col("angle"))` |
| `F.asin`, `F.acos`, `F.atan` | Inverse trigonometric | `F.atan(F.col("slope"))` |

---

## 4. Datetime & Timestamp Functions

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.current_date()` | Current system date | `F.current_date()` |
| `F.current_timestamp()` / `now()` | Current UTC timestamp | `F.current_timestamp()` |
| `F.date_add(col, days)` | Adds $N$ days to a date | `F.date_add(F.col("order_date"), 7)` |
| `F.date_sub(col, days)` | Subtracts $N$ days from date | `F.date_sub(F.col("due_date"), 3)` |
| `F.year(col)` | Extracts year integer | `F.year(F.col("created_at"))` |
| `F.month(col)` | Extracts month (1–12) | `F.month(F.col("created_at"))` |
| `F.dayofmonth(col)` | Extracts day of month (1–31) | `F.dayofmonth(F.col("created_at"))` |
| `F.hour(col)` | Extracts hour (0–23) | `F.hour(F.col("timestamp"))` |
| `F.minute(col)` | Extracts minute (0–59) | `F.minute(F.col("timestamp"))` |
| `F.second(col)` | Extracts second (0–59) | `F.second(F.col("timestamp"))` |
| `F.to_date(col, fmt=None)` | Parses string to DateType | `F.to_date(F.col("dt_str"), "%Y-%m-%d")` |
| `F.to_timestamp(col, fmt=None)` | Parses string to TimestampType | `F.to_timestamp(F.col("ts_str"))` |

---

## 5. Aggregation Functions

Used inside `df.groupBy(...).agg(...)` or over a `Window`:

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.count(col)` | Counts items (supports `"*"`) | `F.count("*")`, `F.count("user_id")` |
| `F.countDistinct(col, *cols)` | Exact count of distinct items | `F.countDistinct("country")` |
| `F.approx_count_distinct(col)` | HyperLogLog approximate count | `F.approx_count_distinct("ip_address")` |
| `F.sum(col)` | Sum of values | `F.sum("salary")` |
| `F.avg(col)` / `F.mean(col)` | Mean of values | `F.avg("salary")` |
| `F.min(col)` | Minimum value | `F.min("created_at")` |
| `F.max(col)` | Maximum value | `F.max("score")` |
| `F.stddev(col)` | Sample standard deviation | `F.stddev("latency")` |
| `F.variance(col)` | Sample variance | `F.variance("price")` |
| `F.first(col)` | First value in group | `F.first("event_name")` |
| `F.last(col)` | Last value in group | `F.last("event_name")` |

---

## 6. Analytical Window Functions

Must be combined with `.over(windowSpec)`:

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.row_number()` | Sequential 1-based row index in partition | `F.row_number().over(w)` |
| `F.rank()` | Rank with gaps for ties | `F.rank().over(w)` |
| `F.dense_rank()` | Rank without gaps for ties | `F.dense_rank().over(w)` |
| `F.percent_rank()` | Relative rank between 0.0 and 1.0 | `F.percent_rank().over(w)` |
| `F.lead(col, offset=1, default=None)` | Accesses value $N$ rows ahead | `F.lead("salary", 1).over(w)` |
| `F.lag(col, offset=1, default=None)` | Accesses value $N$ rows behind | `F.lag("salary", 1).over(w)` |

---

## 7. Complex Types & Hashing

| Function | Description | Example |
| :--- | :--- | :--- |
| `F.array(*cols)` | Constructs an array column | `F.array("val1", "val2")` |
| `F.array_contains(col, val)` | Tests if array contains value | `F.array_contains("tags", "python")` |
| `F.array_distinct(col)` | Removes duplicates from array | `F.array_distinct("categories")` |
| `F.struct(*cols)` | Constructs a nested Struct column | `F.struct("street", "city", "zip")` |
| `F.md5(col)` | 32-character hex MD5 digest | `F.md5("user_id")` |
| `F.sha2(col, numBits)` | SHA-256 or SHA-512 hex digest | `F.sha2("password", 256)` |
| `F.monotonically_increasing_id()` | Generates monotonically increasing ID | `F.monotonically_increasing_id()` |

