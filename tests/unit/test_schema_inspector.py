import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pysphuling.schema.inspector import SchemaInspector
from pysphuling.sql.types import IntegerType, LongType, StringType, DoubleType


def test_schema_inspector_parquet():
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = os.path.join(tmpdir, "test.parquet")
        table = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["alice", "bob", "carol"]), pa.array([10.5, 20.0, 30.5])],
            names=["id", "name", "score"]
        )
        pq.write_table(table, pq_path)

        schema = SchemaInspector.inspect(pq_path, format="parquet")
        assert len(schema) == 3
        assert schema.names == ["id", "name", "score"]
        assert schema["id"].dataType == LongType() or schema["id"].dataType == IntegerType()
        assert schema["name"].dataType == StringType()
        assert schema["score"].dataType == DoubleType()


def test_schema_inspector_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "test.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("user_id,username,active\n1,john,true\n2,mary,false\n")

        schema = SchemaInspector.inspect(csv_path, format="csv")
        assert len(schema) == 3
        assert "user_id" in schema.names
        assert "username" in schema.names
        assert "active" in schema.names


def test_schema_inspector_in_memory():
    import pandas as pd
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    schema = SchemaInspector.inspect(df)
    assert schema.names == ["a", "b"]
    assert schema["b"].dataType == StringType()
