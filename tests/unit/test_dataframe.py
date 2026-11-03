import pytest
import pandas as pd
from pysphuling.sql import SphulingSession, functions as F


@pytest.fixture(scope="module")
def sphuling():
    return (
        SphulingSession.builder
        .appName("DataFrameTestSession")
        .master("local[2]")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(sphuling):
    data = [
        (1, "Alice", 75000, "Engineering"),
        (2, "Bob", 60000, "Engineering"),
        (3, "Carol", 80000, "Marketing"),
        (4, "Dave", 55000, "Marketing"),
        (5, "Eve", 90000, "Executive"),
    ]
    return sphuling.createDataFrame(data, schema=["id", "name", "salary", "department"])


def test_select_and_filter(sample_df):
    filtered = sample_df.filter(F.col("salary") >= 75000).select("name", "salary")
    assert filtered.count() == 3
    assert filtered.columns == ["name", "salary"]
    names = [r.name for r in filtered.collect()]
    assert set(names) == {"Alice", "Carol", "Eve"}


def test_with_column_and_rename(sample_df):
    transformed = (
        sample_df.withColumn("bonus", F.col("salary") * 0.1)
        .withColumnRenamed("department", "dept")
    )
    assert "bonus" in transformed.columns
    assert "dept" in transformed.columns
    assert "department" not in transformed.columns

    row = transformed.filter(F.col("id") == 1).first()
    assert row.bonus == 7500.0
    assert row.dept == "Engineering"


def test_group_by_agg(sample_df):
    agg_df = (
        sample_df.groupBy("department")
        .agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary")
        )
        .orderBy(F.col("department").asc())
    )
    assert agg_df.count() == 3
    rows = agg_df.collect()
    eng = [r for r in rows if r.department == "Engineering"][0]
    assert eng.count == 2
    assert eng.avg_salary == 67500.0
    assert eng.max_salary == 75000


def test_join(sphuling, sample_df):
    bonus_data = [
        ("Engineering", 5000),
        ("Marketing", 3000),
    ]
    bonus_df = sphuling.createDataFrame(bonus_data, schema=["department", "dept_bonus"])

    joined = sample_df.join(bonus_df, on="department", how="inner")
    assert joined.count() == 4
    assert "dept_bonus" in joined.columns


def test_union(sphuling, sample_df):
    new_data = [(6, "Frank", 62000, "Engineering")]
    new_df = sphuling.createDataFrame(new_data, schema=sample_df.columns)

    unioned = sample_df.union(new_df)
    assert unioned.count() == 6


def test_actions_to_pandas_and_arrow(sample_df):
    pdf = sample_df.toPandas()
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == 5

    arrow_table = sample_df.toArrow()
    assert arrow_table.num_rows == 5


def test_temp_view_query(sphuling, sample_df):
    sample_df.createOrReplaceTempView("employees")
    result = sphuling.sql("SELECT department, count(*) as cnt FROM employees GROUP BY department HAVING cnt > 1")
    assert result.count() == 2

