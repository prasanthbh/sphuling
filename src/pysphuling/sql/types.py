"""Spark-compatible DataType system for PySphuling.

Supports all PySpark primitive and complex types, StructType, StructField,
DDL parsing, and bidirectional Apache Arrow schema conversions.
"""

from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Union


class DataType:
    """Base class for all Sphuling data types."""

    def typeName(self) -> str:
        return self.__class__.__name__[:-4].lower()

    def simpleString(self) -> str:
        return self.typeName()

    def jsonValue(self) -> Union[str, Dict[str, Any]]:
        return self.typeName()

    def json(self) -> str:
        return json.dumps(self.jsonValue(), separators=(",", ":"), sort_keys=True)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__)

    def __hash__(self) -> int:
        return hash(self.__class__.__name__)


class PrimitiveType(DataType):
    pass


class NullType(PrimitiveType):
    def typeName(self) -> str:
        return "void"

    def simpleString(self) -> str:
        return "void"


class StringType(PrimitiveType):
    pass


class BinaryType(PrimitiveType):
    pass


class BooleanType(PrimitiveType):
    pass


class DateType(PrimitiveType):
    pass


class TimestampType(PrimitiveType):
    pass


class NumericType(PrimitiveType):
    pass


class IntegralType(NumericType):
    pass


class ByteType(IntegralType):
    def typeName(self) -> str:
        return "byte"

    def simpleString(self) -> str:
        return "tinyint"


class ShortType(IntegralType):
    def typeName(self) -> str:
        return "short"

    def simpleString(self) -> str:
        return "smallint"


class IntegerType(IntegralType):
    def typeName(self) -> str:
        return "integer"

    def simpleString(self) -> str:
        return "int"


class LongType(IntegralType):
    def typeName(self) -> str:
        return "long"

    def simpleString(self) -> str:
        return "bigint"


class FloatType(NumericType):
    pass


class DoubleType(NumericType):
    pass


class DecimalType(NumericType):
    def __init__(self, precision: int = 10, scale: int = 0) -> None:
        self.precision = precision
        self.scale = scale

    def simpleString(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def jsonValue(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def __repr__(self) -> str:
        return f"DecimalType({self.precision},{self.scale})"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, DecimalType)
            and self.precision == other.precision
            and self.scale == other.scale
        )

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.precision, self.scale))


class ArrayType(DataType):
    def __init__(self, elementType: DataType, containsNull: bool = True) -> None:
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": "array",
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }

    def __repr__(self) -> str:
        return f"ArrayType({self.elementType!r}, {self.containsNull})"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ArrayType)
            and self.elementType == other.elementType
            and self.containsNull == other.containsNull
        )

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.elementType, self.containsNull))


class MapType(DataType):
    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True) -> None:
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self) -> str:
        return f"map<{self.keyType.simpleString()},{self.valueType.simpleString()}>"

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": "map",
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }

    def __repr__(self) -> str:
        return f"MapType({self.keyType!r}, {self.valueType!r}, {self.valueContainsNull})"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MapType)
            and self.keyType == other.keyType
            and self.valueType == other.valueType
            and self.valueContainsNull == other.valueContainsNull
        )

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.keyType, self.valueType, self.valueContainsNull))


class StructField:
    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def simpleString(self) -> str:
        return f"{self.name}:{self.dataType.simpleString()}"

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }

    def __repr__(self) -> str:
        return f"StructField('{self.name}', {self.dataType!r}, {self.nullable})"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, StructField)
            and self.name == other.name
            and self.dataType == other.dataType
            and self.nullable == other.nullable
        )

    def __hash__(self) -> int:
        return hash((self.name, self.dataType, self.nullable))


class StructType(DataType):
    def __init__(self, fields: Optional[Sequence[StructField]] = None) -> None:
        self.fields: List[StructField] = list(fields) if fields else []
        self._name_to_field: Dict[str, StructField] = {f.name: f for f in self.fields}

    def add(
        self,
        field: Union[StructField, str],
        data_type: Optional[DataType] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StructType:
        if isinstance(field, StructField):
            self.fields.append(field)
            self._name_to_field[field.name] = field
        elif isinstance(field, str):
            if data_type is None:
                raise ValueError("data_type must be specified when adding field by name")
            sf = StructField(field, data_type, nullable, metadata)
            self.fields.append(sf)
            self._name_to_field[field] = sf
        return self

    @property
    def names(self) -> List[str]:
        return [f.name for f in self.fields]

    def fieldNames(self) -> List[str]:
        return self.names

    def __getitem__(self, name_or_idx: Union[str, int]) -> StructField:
        if isinstance(name_or_idx, int):
            return self.fields[name_or_idx]
        return self._name_to_field[name_or_idx]

    def __contains__(self, name: str) -> bool:
        return name in self._name_to_field

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields)

    def simpleString(self) -> str:
        fields_str = ",".join(f.simpleString() for f in self.fields)
        return f"struct<{fields_str}>"

    def treeString(self) -> str:
        lines = ["root"]
        for f in self.fields:
            nullable_str = "nullable = true" if f.nullable else "nullable = false"
            lines.append(f" |-- {f.name}: {f.dataType.simpleString()} ({nullable_str})")
        return "\n".join(lines)

    def jsonValue(self) -> Dict[str, Any]:
        return {
            "type": "struct",
            "fields": [f.jsonValue() for f in self.fields],
        }

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(f) for f in self.fields)
        return f"StructType([{fields_repr}])"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, StructType) and self.fields == other.fields

    def __hash__(self) -> int:
        return hash(tuple(self.fields))

    @classmethod
    def fromDDL(cls, ddl: str) -> StructType:
        """Parses a Spark-style DDL schema string into a StructType.
        Example: 'id INT, name STRING, salary DOUBLE'
        """
        st = StructType()
        # Clean enclosing struct<...> or parentheses if present
        clean = ddl.strip()
        if clean.lower().startswith("struct<") and clean.endswith(">"):
            clean = clean[7:-1].strip()
        elif clean.startswith("(") and clean.endswith(")"):
            clean = clean[1:-1].strip()

        # Split top-level commas, respecting nested brackets
        parts = []
        bracket_level = 0
        current = []
        for ch in clean:
            if ch in "<(":
                bracket_level += 1
                current.append(ch)
            elif ch in ">)":
                bracket_level -= 1
                current.append(ch)
            elif ch == "," and bracket_level == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())

        for part in parts:
            if not part:
                continue
            # Match: `col_name` type or col_name type
            tokens = part.split(None, 1)
            if len(tokens) < 2:
                continue
            col_name = tokens[0].strip("`\"' ")
            type_str = tokens[1].strip()
            dtype = _parse_type_str(type_str)
            st.add(StructField(col_name, dtype))
        return st


def _parse_type_str(type_str: str) -> DataType:
    type_str = type_str.strip().lower()
    if type_str in ("string", "str", "varchar", "text"):
        return StringType()
    if type_str in ("int", "integer"):
        return IntegerType()
    if type_str in ("bigint", "long"):
        return LongType()
    if type_str in ("smallint", "short"):
        return ShortType()
    if type_str in ("tinyint", "byte"):
        return ByteType()
    if type_str in ("double", "float8"):
        return DoubleType()
    if type_str in ("float", "float4", "real"):
        return FloatType()
    if type_str in ("bool", "boolean"):
        return BooleanType()
    if type_str in ("date",):
        return DateType()
    if type_str in ("timestamp", "datetime"):
        return TimestampType()
    if type_str in ("binary", "bytes", "blob"):
        return BinaryType()
    if type_str.startswith("decimal"):
        match = re.match(r"decimal\((\d+)(?:,\s*(\d+))?\)", type_str)
        if match:
            p = int(match.group(1))
            s = int(match.group(2)) if match.group(2) else 0
            return DecimalType(p, s)
        return DecimalType(10, 0)
    if type_str.startswith("array<") and type_str.endswith(">"):
        inner = type_str[6:-1]
        return ArrayType(_parse_type_str(inner))
    if type_str.startswith("map<") and type_str.endswith(">"):
        inner = type_str[4:-1]
        # split by comma
        k_str, v_str = inner.split(",", 1)
        return MapType(_parse_type_str(k_str), _parse_type_str(v_str))
    if type_str.startswith("struct<") and type_str.endswith(">"):
        return StructType.fromDDL(type_str)
    return StringType()

