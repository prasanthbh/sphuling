"""PySpark drop-in API parity test suite."""

import pytest


def test_pyspark_drop_in_imports():
    import pysphuling as pyspark
    from pyspark.sql import SparkSession, functions as F, types as T, Window

    assert SparkSession is not None
    assert F is not None
    assert T is not None
    assert Window is not None


def test_pyspark_end_to_end_pipeline():
    import pysphuling as pyspark
    from pyspark.sql import SparkSession, functions as F, Window

    # 1. Initialize session using traditional PySpark syntax
    spark = (
        SparkSession.builder
        .appName("PySparkParityApp")
        .master("local[2]")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )

    # 2. Ingest test dataset
    raw_data = [
        (101, "Finance", 95000, "NY"),
        (102, "Finance", 85000, "NY"),
        (103, "Finance", 72000, "SF"),
        (104, "Engineering", 120000, "SF"),
        (105, "Engineering", 110000, "NY"),
        (106, "Engineering", 90000, "SF"),
    ]
    df = spark.createDataFrame(raw_data, schema=["emp_id", "dept", "salary", "location"])

    # 3. Check schema properties without execution
    assert "salary" in df.columns
    assert len(df.schema) == 4

    # 4. Window ranking expression
    w = Window.partitionBy("dept").orderBy(F.col("salary").desc())

    # 5. Complex PySpark DataFrame pipeline
    processed = (
        df.filter(F.col("salary") > 80000)
        .withColumn("rank_in_dept", F.rank().over(w))
        .withColumn("tax_estimate", F.col("salary") * 0.25)
        .withColumn("is_sf", F.when(F.col("location") == "SF", True).otherwise(False))
    )

    # 6. Action: collect results
    rows = processed.collect()
    assert len(rows) == 5

    # 7. Aggregation pipeline
    summary = (
        processed.groupBy("dept")
        .agg(
            F.count("*").alias("high_earners"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("top_salary")
        )
        .orderBy(F.col("dept").asc())
    )

    summary_rows = summary.collect()
    assert len(summary_rows) == 2

    eng = [r for r in summary_rows if r.dept == "Engineering"][0]
    assert eng.high_earners == 3
    assert eng.top_salary == 120000

