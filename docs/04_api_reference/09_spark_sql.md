# API Reference: Spark SQL Engine

Sphuling includes a full SQL execution engine supporting the **Spark SQL dialect**. It integrates an automatic SQL transpilation layer (powered by `sqlglot`) that normalizes Spark-specific SQL constructs into ANSI-compliant execution trees executed by Apache DataFusion and DuckDB.

---

## 1. Executing SQL Queries (`sphuling.sql`)

```python
from pysphuling.sql import SphulingSession

sphuling = SphulingSession.builder.getOrCreate()

# Direct scalar expressions
df = sphuling.sql("SELECT 100 AS id, UPPER('sphuling') AS engine, CURRENT_TIMESTAMP() AS execution_time")
df.show()
```

---

## 2. In-Memory Views (`createOrReplaceTempView`)

Any DataFrame can be registered as an in-memory SQL table/view and queried with `sphuling.sql()`:

```python
# 1. Ingest DataFrame
data = [(1, "Electronics", 1200), (2, "Clothing", 450), (3, "Electronics", 800)]
df = sphuling.createDataFrame(data, schema=["order_id", "category", "amount"])

# 2. Register temporary view
df.createOrReplaceTempView("orders")

# 3. Query with SQL
result_df = sphuling.sql("""
    SELECT 
        category,
        COUNT(*) AS total_orders,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_order_value
    FROM orders
    GROUP BY category
    ORDER BY total_revenue DESC
""")

result_df.show()
```

---

## 3. Spark SQL Dialect Features Supported

### 3.1 Common Table Expressions (CTEs)
```python
query = """
    WITH regional_sales AS (
        SELECT region, SUM(amount) AS revenue
        FROM sales
        GROUP BY region
    )
    SELECT region, revenue
    FROM regional_sales
    WHERE revenue > 100000
    ORDER BY revenue DESC
"""
sphuling.sql(query).show()
```

### 3.2 Subqueries & IN / EXISTS Predicates
```python
query = """
    SELECT * 
    FROM employees
    WHERE department_id IN (
        SELECT id FROM departments WHERE is_active = true
    )
"""
sphuling.sql(query).show()
```

### 3.3 Backtick-Quoted Identifiers
Spark allows non-standard characters in column names via backtick quotes:

```python
sphuling.sql("SELECT `first-name`, `zip code` FROM users WHERE `user-id` > 10").show()
```

### 3.4 Nested Struct Navigation (Dot Syntax)
```python
sphuling.sql("SELECT user.address.city, user.contact.email FROM profiles").show()
```

### 3.5 Analytical Windows in SQL
```python
query = """
    SELECT 
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
        SUM(salary) OVER (PARTITION BY department) as dept_total
    FROM employees
"""
sphuling.sql(query).show()
```

---

## 4. Metadata Catalog Operations

Query registered views and tables via the catalog API:

```python
# List tables
for table in sphuling.catalog.listTables():
    print(f"Table: {table.name}, Type: {table.tableType}")

# Check table existence
if sphuling.catalog.tableExists("orders"):
    print("Table 'orders' is registered and ready for queries!")

# Drop temporary view
sphuling.catalog.dropTempView("orders")
```

