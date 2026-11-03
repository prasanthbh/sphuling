# API Reference: Data Types & Schemas

Sphuling provides a complete PySpark-compatible type system under `pysphuling.sql.types` (typically imported as `import pysphuling.sql.types as T`), mapped directly to physical Apache Arrow memory buffers.

```python
from pysphuling.sql import types as T
```

---

## 1. Type Catalog

### Primitive & Numeric Types
- `T.StringType()`: UTF-8 variable-length text strings.
- `T.IntegerType()`: 32-bit signed integer (`int`).
- `T.LongType()`: 64-bit signed integer (`bigint`).
- `T.ShortType()`: 16-bit signed integer (`smallint`).
- `T.ByteType()`: 8-bit signed integer (`tinyint`).
- `T.DoubleType()`: 64-bit double-precision IEEE floating point.
- `T.FloatType()`: 32-bit single-precision IEEE floating point.
- `T.DecimalType(precision=10, scale=0)`: Fixed-point decimal with custom precision and scale.
- `T.BooleanType()`: Binary boolean (`true` / `false`).
- `T.DateType()`: Calendar date (`YYYY-MM-DD`).
- `T.TimestampType()`: Microsecond-precision timestamp with timezone.
- `T.BinaryType()`: Raw byte sequence.
- `T.NullType()`: Represents untyped null values.

### Complex & Nested Types
- `T.ArrayType(elementType, containsNull=True)`: Ordered sequence of elements.
- `T.MapType(keyType, valueType, valueContainsNull=True)`: Key-value map.
- `T.StructType(fields: List[StructField])`: Tabular schema representation.
- `T.StructField(name, dataType, nullable=True, metadata=None)`: Individual column definition.

---

## 2. Defining Schemas Programmatically

```python
from pysphuling.sql import types as T

custom_schema = T.StructType([
    T.StructField("id", T.IntegerType(), nullable=False),
    T.StructField("username", T.StringType(), nullable=True),
    T.StructField("salary", T.DoubleType(), nullable=True),
    T.StructField("tags", T.ArrayType(T.StringType()), nullable=True),
    T.StructField("address", T.StructType([
        T.StructField("city", T.StringType()),
        T.StructField("zip", T.IntegerType())
    ]))
])
```

---

## 3. Spark DDL Schema Parsing (`StructType.fromDDL`)

Sphuling supports standard Spark SQL DDL string notation for compact schema definitions:

```python
ddl_string = """
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    items ARRAY<STRING>,
    pricing STRUCT<subtotal: DOUBLE, tax: DOUBLE, total: DOUBLE>
"""

schema = T.StructType.fromDDL(ddl_string)
print(schema.treeString())
```

Output:
```text
root
 |-- order_id: bigint (nullable = true)
 |-- customer_id: int (nullable = true)
 |-- order_date: date (nullable = true)
 |-- items: array<string> (nullable = true)
 |-- pricing: struct<subtotal:double,tax:double,total:double> (nullable = true)
```

---

## 4. StructType Inspection Methods

- `schema.names`: List of column name strings `["order_id", "customer_id", ...]`.
- `len(schema)`: Total number of fields.
- `schema["field_name"]`: Accesses the `StructField` by name.
- `schema[0]`: Accesses the `StructField` by 0-based index.
- `schema.simpleString()`: Compact representation `struct<id:int,name:string>`.
- `schema.treeString()`: Formatted tree diagram.
- `schema.json()`: Serialized JSON dictionary matching Spark schema JSON specs.

---

## 5. Bidirectional Apache Arrow Conversion

Sphuling seamlessly maps between PyArrow schemas and Sphuling schemas:

```python
from pysphuling.sql.schema_bridge import arrow_schema_to_sphuling, sphuling_schema_to_arrow
import pyarrow as pa

# Convert PyArrow Schema -> Sphuling StructType
pa_schema = pa.schema([pa.field("x", pa.int32()), pa.field("y", pa.string())])
sphuling_schema = arrow_schema_to_sphuling(pa_schema)

# Convert Sphuling StructType -> PyArrow Schema
converted_pa_schema = sphuling_schema_to_arrow(sphuling_schema)
```

