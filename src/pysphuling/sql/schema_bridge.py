"""Bidirectional translation between Apache Arrow schemas/types and PySphuling types."""

from __future__ import annotations
import pyarrow as pa
from pysphuling.sql.types import (
    DataType,
    NullType,
    StringType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    ArrayType,
    MapType,
    StructField,
    StructType,
)


def arrow_type_to_sphuling(arrow_type: pa.DataType) -> DataType:
    """Converts a PyArrow DataType to the corresponding PySphuling DataType."""
    if pa.types.is_null(arrow_type):
        return NullType()
    elif pa.types.is_boolean(arrow_type):
        return BooleanType()
    elif pa.types.is_int8(arrow_type):
        return ByteType()
    elif pa.types.is_int16(arrow_type):
        return ShortType()
    elif pa.types.is_int32(arrow_type):
        return IntegerType()
    elif pa.types.is_int64(arrow_type):
        return LongType()
    elif pa.types.is_uint8(arrow_type):
        return ShortType()
    elif pa.types.is_uint16(arrow_type):
        return IntegerType()
    elif pa.types.is_uint32(arrow_type) or pa.types.is_uint64(arrow_type):
        return LongType()
    elif pa.types.is_float32(arrow_type):
        return FloatType()
    elif pa.types.is_float64(arrow_type):
        return DoubleType()
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) or pa.types.is_string_view(arrow_type):
        return StringType()
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()
    elif pa.types.is_date(arrow_type):
        return DateType()
    elif pa.types.is_timestamp(arrow_type):
        return TimestampType()
    elif pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return ArrayType(arrow_type_to_sphuling(arrow_type.value_type))
    elif pa.types.is_map(arrow_type):
        return MapType(
            arrow_type_to_sphuling(arrow_type.key_type),
            arrow_type_to_sphuling(arrow_type.item_type),
        )
    elif pa.types.is_struct(arrow_type):
        fields = [
            StructField(
                name=arrow_type[i].name,
                dataType=arrow_type_to_sphuling(arrow_type[i].type),
                nullable=arrow_type[i].nullable,
            )
            for i in range(arrow_type.num_fields)
        ]
        return StructType(fields)
    return StringType()


def sphuling_type_to_arrow(data_type: DataType) -> pa.DataType:
    """Converts a PySphuling DataType to the corresponding PyArrow DataType."""
    if isinstance(data_type, NullType):
        return pa.null()
    elif isinstance(data_type, BooleanType):
        return pa.bool_()
    elif isinstance(data_type, ByteType):
        return pa.int8()
    elif isinstance(data_type, ShortType):
        return pa.int16()
    elif isinstance(data_type, IntegerType):
        return pa.int32()
    elif isinstance(data_type, LongType):
        return pa.int64()
    elif isinstance(data_type, FloatType):
        return pa.float32()
    elif isinstance(data_type, DoubleType):
        return pa.float64()
    elif isinstance(data_type, StringType):
        return pa.string()
    elif isinstance(data_type, BinaryType):
        return pa.binary()
    elif isinstance(data_type, DateType):
        return pa.date32()
    elif isinstance(data_type, TimestampType):
        return pa.timestamp("us")
    elif isinstance(data_type, DecimalType):
        return pa.decimal128(data_type.precision, data_type.scale)
    elif isinstance(data_type, ArrayType):
        return pa.list_(sphuling_type_to_arrow(data_type.elementType))
    elif isinstance(data_type, MapType):
        return pa.map_(
            sphuling_type_to_arrow(data_type.keyType),
            sphuling_type_to_arrow(data_type.valueType),
        )
    elif isinstance(data_type, StructType):
        fields = [
            pa.field(
                f.name,
                sphuling_type_to_arrow(f.dataType),
                nullable=f.nullable,
            )
            for f in data_type.fields
        ]
        return pa.struct(fields)
    return pa.string()


def arrow_schema_to_sphuling(arrow_schema: pa.Schema) -> StructType:
    """Converts a PyArrow Schema to a PySphuling StructType."""
    fields = [
        StructField(
            name=field.name,
            dataType=arrow_type_to_sphuling(field.type),
            nullable=field.nullable,
        )
        for field in arrow_schema
    ]
    return StructType(fields)


def sphuling_schema_to_arrow(struct_type: StructType) -> pa.Schema:
    """Converts a PySphuling StructType to a PyArrow Schema."""
    fields = [
        pa.field(
            f.name,
            sphuling_type_to_arrow(f.dataType),
            nullable=f.nullable,
        )
        for f in struct_type.fields
    ]
    return pa.schema(fields)

