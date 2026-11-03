import pyarrow as pa
import pytest
from pysphuling.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pysphuling.sql.schema_bridge import arrow_schema_to_sphuling, sphuling_schema_to_arrow


def test_primitive_types():
    assert StringType().simpleString() == "string"
    assert IntegerType().simpleString() == "int"
    assert LongType().simpleString() == "bigint"
    assert DoubleType().simpleString() == "double"
    assert BooleanType().simpleString() == "boolean"
    assert DecimalType(12, 2).simpleString() == "decimal(12,2)"


def test_complex_types():
    arr = ArrayType(IntegerType())
    assert arr.simpleString() == "array<int>"

    m = MapType(StringType(), DoubleType())
    assert m.simpleString() == "map<string,double>"

    st = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
    ])
    assert st.simpleString() == "struct<id:int,name:string>"
    assert len(st) == 2
    assert "name" in st
    assert st["id"].dataType == IntegerType()


def test_from_ddl():
    st = StructType.fromDDL("id INT, name STRING, salary DOUBLE, is_active BOOLEAN")
    assert len(st) == 4
    assert st["id"].dataType == IntegerType()
    assert st["name"].dataType == StringType()
    assert st["salary"].dataType == DoubleType()
    assert st["is_active"].dataType == BooleanType()


def test_arrow_schema_bidirectional_conversion():
    pa_schema = pa.schema([
        pa.field("a", pa.int32(), nullable=False),
        pa.field("b", pa.string(), nullable=True),
        pa.field("c", pa.float64(), nullable=True),
        pa.field("d", pa.list_(pa.int64()), nullable=True),
    ])

    sphuling_schema = arrow_schema_to_sphuling(pa_schema)
    assert len(sphuling_schema) == 4
    assert sphuling_schema["a"].dataType == IntegerType()
    assert sphuling_schema["b"].dataType == StringType()
    assert sphuling_schema["c"].dataType == DoubleType()
    assert isinstance(sphuling_schema["d"].dataType, ArrayType)

    # Convert back to Arrow
    converted_pa_schema = sphuling_schema_to_arrow(sphuling_schema)
    assert converted_pa_schema.names == pa_schema.names
    assert converted_pa_schema[0].type == pa.int32()
    assert converted_pa_schema[1].type == pa.string()
    assert converted_pa_schema[2].type == pa.float64()

