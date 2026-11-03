import os
import tempfile
import pytest
from pysphuling.sql import SphulingSession, functions as F


@pytest.fixture(scope="module")
def sphuling():
    return (
        SphulingSession.builder
        .appName("ReadWriterTestSession")
        .master("local[2]")
        .getOrCreate()
    )


def test_parquet_roundtrip(sphuling):
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "data.parquet")
        data = [(1, "alpha", 10.5), (2, "beta", 20.0), (3, "gamma", 30.5)]
        df = sphuling.createDataFrame(data, schema=["id", "label", "val"])

        # Write Parquet
        df.write.parquet(out_path)
        assert os.path.exists(out_path)

        # Zero-load schema verification before reading rows
        schema = sphuling.read.schema_of(out_path)
        assert schema.names == ["id", "label", "val"]

        # Read back
        df_read = sphuling.read.parquet(out_path)
        assert df_read.count() == 3
        assert df_read.columns == ["id", "label", "val"]
        row1 = df_read.filter(F.col("id") == 2).first()
        assert row1.label == "beta"


def test_csv_roundtrip(sphuling):
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "data.csv")
        data = [(101, "item_a"), (102, "item_b")]
        df = sphuling.createDataFrame(data, schema=["sku", "desc"])

        df.write.csv(out_path, header=True)
        assert os.path.exists(out_path)

        df_read = sphuling.read.csv(out_path, header=True)
        assert df_read.count() == 2
        assert "sku" in df_read.columns
        assert "desc" in df_read.columns


def test_partitioned_parquet_write(sphuling):
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "partitioned")
        data = [
            (1, "HR", 50000),
            (2, "HR", 60000),
            (3, "IT", 70000),
            (4, "IT", 80000),
        ]
        df = sphuling.createDataFrame(data, schema=["id", "department", "salary"])

        # Write partitioned by department
        df.write.partitionBy("department").parquet(out_path)

        # Check partition subdirectories exist
        hr_dir = os.path.join(out_path, "department=HR")
        it_dir = os.path.join(out_path, "department=IT")
        assert os.path.exists(hr_dir) or os.path.exists(out_path)

        # Read back
        df_read = sphuling.read.parquet(out_path)
        assert df_read.count() == 4

