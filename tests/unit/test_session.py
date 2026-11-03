import pytest
import pandas as pd
from pysphuling.sql import SphulingSession, SparkSession
from pysphuling.sql.types import IntegerType, StringType


@pytest.fixture
def sphuling():
    session = (
        SphulingSession.builder
        .appName("TestSession")
        .master("local[2]")
        .config("sphuling.executor.cores", "2")
        .config("sphuling.executor.memory", "1g")
        .getOrCreate()
    )
    yield session
    # Don't stop to reuse or stop cleanly


def test_session_creation(sphuling):
    assert sphuling is not None
    assert sphuling.conf.get("sphuling.app.name") == "TestSession"


def test_spark_session_alias():
    spark = SparkSession.builder.appName("SparkAlias").getOrCreate()
    assert spark is not None
    assert isinstance(spark, SphulingSession)


def test_create_dataframe_from_pandas(sphuling):
    pdf = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
    df = sphuling.createDataFrame(pdf)

    assert df.count() == 3
    assert df.columns == ["id", "name"]
    rows = df.collect()
    assert len(rows) == 3
    assert rows[0].name == "alice"
    assert rows[1].id == 2


def test_create_dataframe_from_tuples(sphuling):
    data = [(10, "apple"), (20, "banana")]
    df = sphuling.createDataFrame(data, schema=["item_id", "item_name"])

    assert df.count() == 2
    assert df.columns == ["item_id", "item_name"]
    first_row = df.first()
    assert first_row.item_id == 10
    assert first_row.item_name == "apple"


def test_create_dataframe_from_dicts(sphuling):
    data = [{"x": 1.5, "y": "test1"}, {"x": 2.5, "y": "test2"}]
    df = sphuling.createDataFrame(data)
    assert df.count() == 2
    assert "x" in df.columns
    assert "y" in df.columns


def test_range(sphuling):
    df = sphuling.range(5)
    assert df.count() == 5
    assert df.columns == ["id"]
    rows = df.collect()
    assert [r.id for r in rows] == [0, 1, 2, 3, 4]


def test_sql_execution(sphuling):
    df = sphuling.sql("SELECT 42 as answer, 'hello sphuling' as greeting")
    assert df.count() == 1
    assert df.columns == ["answer", "greeting"]
    row = df.first()
    assert row.answer == 42
    assert row.greeting == "hello sphuling"

