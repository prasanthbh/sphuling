# API Reference: Window Specifications

The `Window` and `WindowSpec` classes define analytical partition boundaries, sorting orders, and frame windows for computing rankings, cumulative running sums, and moving averages.

```python
from pysphuling.sql.window import Window
import pysphuling.sql.functions as F
```

---

## 1. Frame Boundary Constants

Sphuling defines standard Spark window boundary constants:

- `Window.unboundedPreceding`: Represents the first row in the partition ($-\infty$).
- `Window.unboundedFollowing`: Represents the last row in the partition ($+\infty$).
- `Window.currentRow`: Represents the current row being evaluated ($0$).

---

## 2. Window Construction Methods

### `Window.partitionBy(*cols) -> WindowSpec`
Divides rows into independent groups:

```python
w = Window.partitionBy("department")
```

### `Window.orderBy(*cols) -> WindowSpec`
Orders rows within each partition:

```python
w = Window.partitionBy("department").orderBy(F.col("salary").desc())
```

### `.rowsBetween(start: int, end: int) -> WindowSpec`
Defines physical row-based frame boundaries relative to the current row:

```python
# Cumulative frame from partition start to current row
w_cumulative = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 3-row moving window (1 row before, current row, 1 row ahead)
w_moving = w.rowsBetween(-1, 1)
```

### `.rangeBetween(start: int, end: int) -> WindowSpec`
Defines value-based frame boundaries relative to the current row value:

```python
w_range = w.rangeBetween(Window.unboundedPreceding, Window.currentRow)
```

---

## 3. Practical Window Examples

### Example 1: Ranking Employees by Salary Within Department
```python
w = Window.partitionBy("department").orderBy(F.col("salary").desc())

(
    df.withColumn("rank", F.rank().over(w))
    .withColumn("dense_rank", F.dense_rank().over(w))
    .withColumn("row_num", F.row_number().over(w))
    .show()
)
```

### Example 2: Running Cumulative Total (Running Sum)
```python
w_running = (
    Window.partitionBy("account_id")
    .orderBy("transaction_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df.withColumn("running_balance", F.sum("amount").over(w_running)).show()
```

### Example 3: Moving Average (3-Day Rolling Window)
```python
w_rolling = (
    Window.partitionBy("ticker")
    .orderBy("trade_date")
    .rowsBetween(-2, Window.currentRow) # Current day + previous 2 days
)

df.withColumn("rolling_3d_avg", F.avg("close_price").over(w_rolling)).show()
```

### Example 4: Period-Over-Period Growth (Lead & Lag)
```python
w_chronological = Window.partitionBy("store_id").orderBy("month")

(
    df.withColumn("prev_month_sales", F.lag("sales", 1).over(w_chronological))
    .withColumn("growth_rate", (F.col("sales") - F.col("prev_month_sales")) / F.col("prev_month_sales"))
    .show()
)
```

