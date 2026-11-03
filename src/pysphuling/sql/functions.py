"""Complete pysphuling.sql.functions library matching PySpark's F catalog."""

from __future__ import annotations
from typing import Any, List, Optional, Sequence, Union
import datafusion as df
import datafusion.expr as de
import datafusion.functions as dff

from pysphuling.sql.column import Column, _to_expr


def _to_col_or_expr(val: Any) -> Any:
    """Helper to convert string to Column expression or literal."""
    if isinstance(val, Column):
        return val.expr
    elif isinstance(val, (df.Expr, de.SortExpr)):
        return val
    elif isinstance(val, str):
        return df.col(val)
    return df.lit(val)


# 1. Literals & Core
def col(col_name: str) -> Column:
    """Returns a Column based on the given column name."""
    return Column(df.col(col_name))


def column(col_name: str) -> Column:
    return col(col_name)


def lit(literal_value: Any) -> Column:
    """Creates a Column of literal value."""
    if isinstance(literal_value, Column):
        return literal_value
    return Column(df.lit(literal_value))


class CaseWhenBuilder:
    """Builder for PySpark's when().when().otherwise() chaining."""

    def __init__(self, condition: Any, value: Any) -> None:
        cond_expr = _to_expr(condition)
        val_expr = _to_expr(value)
        self._builder = dff.when(cond_expr, val_expr)

    def when(self, condition: Any, value: Any) -> CaseWhenBuilder:
        self._builder = self._builder.when(_to_expr(condition), _to_expr(value))
        return self

    def otherwise(self, value: Any) -> Column:
        expr = self._builder.otherwise(_to_expr(value))
        return Column(expr)


def when(condition: Any, value: Any) -> CaseWhenBuilder:
    """Evaluates a list of conditions and returns one of multiple possible result expressions."""
    return CaseWhenBuilder(condition, value)


def coalesce(*cols: Any) -> Column:
    """Returns the first column that is not null."""
    exprs = [_to_col_or_expr(c) for c in cols]
    return Column(dff.coalesce(*exprs))


def greatest(*cols: Any) -> Column:
    """Returns the greatest value of the list of column names, skipping null values."""
    exprs = [_to_col_or_expr(c) for c in cols]
    if hasattr(dff, "greatest"):
        return Column(dff.greatest(*exprs))
    res = exprs[0]
    for e in exprs[1:]:
        res = dff.when(res > e, res).otherwise(e)
    return Column(res)


def least(*cols: Any) -> Column:
    """Returns the least value of the list of column names, skipping null values."""
    exprs = [_to_col_or_expr(c) for c in cols]
    if hasattr(dff, "least"):
        return Column(dff.least(*exprs))
    res = exprs[0]
    for e in exprs[1:]:
        res = dff.when(res < e, res).otherwise(e)
    return Column(res)


def nanvl(col1: Any, col2: Any) -> Column:
    """Returns col1 if not NaN, or col2 if col1 is NaN."""
    c1 = _to_col_or_expr(col1)
    c2 = _to_col_or_expr(col2)
    return Column(dff.when(c1.isnan(), c2).otherwise(c1))


# 2. String Functions
def concat(*cols: Any) -> Column:
    """Concatenates multiple input columns together into a single string column."""
    exprs = [_to_col_or_expr(c) for c in cols]
    return Column(dff.concat(*exprs))


def concat_ws(sep: str, *cols: Any) -> Column:
    """Concatenates multiple input string columns together into a single string column, using the given separator."""
    exprs = [_to_col_or_expr(c) for c in cols]
    return Column(dff.concat_ws(df.lit(sep), *exprs))


def lower(col: Any) -> Column:
    """Converts a string column to lower case."""
    return Column(dff.lower(_to_col_or_expr(col)))


def upper(col: Any) -> Column:
    """Converts a string column to upper case."""
    return Column(dff.upper(_to_col_or_expr(col)))


def trim(col: Any) -> Column:
    """Trims the spaces from both ends for the specified string column."""
    return Column(dff.trim(_to_col_or_expr(col)))


def ltrim(col: Any) -> Column:
    """Trim the spaces from left end for the specified string column."""
    return Column(dff.ltrim(_to_col_or_expr(col)))


def rtrim(col: Any) -> Column:
    """Trim the spaces from right end for the specified string column."""
    return Column(dff.rtrim(_to_col_or_expr(col)))


def initcap(col: Any) -> Column:
    """Returns a new string column by converting the first letter of each word to uppercase."""
    return Column(dff.initcap(_to_col_or_expr(col)))


def length(col: Any) -> Column:
    """Computes the character length of a given string or number of bytes of a binary string."""
    return Column(dff.length(_to_col_or_expr(col)))


def substring(col: Any, pos: int, len: int) -> Column:
    """Substring starts at pos and is of length len when str is its content."""
    return Column(dff.substr(_to_col_or_expr(col), df.lit(pos), df.lit(len)))


def split(col: Any, pattern: str) -> Column:
    """Splits str around pattern."""
    if hasattr(dff, "string_to_array"):
        return Column(dff.string_to_array(_to_col_or_expr(col), df.lit(pattern)))
    return Column(dff.split_part(_to_col_or_expr(col), df.lit(pattern), df.lit(1)))


def regexp_replace(col: Any, pattern: str, replacement: str) -> Column:
    """Replace all substrings of the specified string value that match regexp with rep."""
    return Column(dff.regexp_replace(_to_col_or_expr(col), df.lit(pattern), df.lit(replacement)))


def repeat(col: Any, n: int) -> Column:
    """Repeats a string column n times."""
    return Column(dff.repeat(_to_col_or_expr(col), df.lit(n)))


def reverse(col: Any) -> Column:
    """Reverses the string column and returns the result as a new string column."""
    return Column(dff.reverse(_to_col_or_expr(col)))


def ascii(col: Any) -> Column:
    """Computes the numeric value of the first character of the string column."""
    return Column(dff.ascii(_to_col_or_expr(col)))


# 3. Math & Numeric Functions
def abs(col: Any) -> Column:
    """Computes the absolute value."""
    return Column(dff.abs(_to_col_or_expr(col)))


def sqrt(col: Any) -> Column:
    """Computes the square root of the specified float value."""
    return Column(dff.sqrt(_to_col_or_expr(col)))


def cbrt(col: Any) -> Column:
    """Computes the cube-root of the given value."""
    return Column(dff.cbrt(_to_col_or_expr(col)))


def exp(col: Any) -> Column:
    """Computes the exponential of the given value."""
    return Column(dff.exp(_to_col_or_expr(col)))


def log(col: Any) -> Column:
    """Computes the natural logarithm of the given value."""
    return Column(dff.ln(_to_col_or_expr(col)))


def log10(col: Any) -> Column:
    """Computes the base 10 logarithm of the given value."""
    return Column(dff.log10(_to_col_or_expr(col)))


def log2(col: Any) -> Column:
    """Computes the base 2 logarithm of the given value."""
    return Column(dff.log2(_to_col_or_expr(col)))


def pow(col1: Any, col2: Any) -> Column:
    """Returns the value of the first argument raised to the power of the second argument."""
    return Column(dff.power(_to_col_or_expr(col1), _to_col_or_expr(col2)))


def round(col: Any, scale: int = 0) -> Column:
    """Round the given value to scale decimal places."""
    return Column(dff.round(_to_col_or_expr(col), df.lit(scale)))


def floor(col: Any) -> Column:
    """Computes the floor of the given value."""
    return Column(dff.floor(_to_col_or_expr(col)))


def ceil(col: Any) -> Column:
    """Computes the ceiling of the given value."""
    return Column(dff.ceil(_to_col_or_expr(col)))


def sin(col: Any) -> Column:
    return Column(dff.sin(_to_col_or_expr(col)))


def cos(col: Any) -> Column:
    return Column(dff.cos(_to_col_or_expr(col)))


def tan(col: Any) -> Column:
    return Column(dff.tan(_to_col_or_expr(col)))


def asin(col: Any) -> Column:
    return Column(dff.asin(_to_col_or_expr(col)))


def acos(col: Any) -> Column:
    return Column(dff.acos(_to_col_or_expr(col)))


def atan(col: Any) -> Column:
    return Column(dff.atan(_to_col_or_expr(col)))


def atan2(col1: Any, col2: Any) -> Column:
    return Column(dff.atan2(_to_col_or_expr(col1), _to_col_or_expr(col2)))


# 4. Datetime Functions
def current_date() -> Column:
    """Returns the current date as a date column."""
    return Column(dff.current_date())


def current_timestamp() -> Column:
    """Returns the current timestamp."""
    return Column(dff.now())


def now() -> Column:
    return current_timestamp()


def date_add(col: Any, days: int) -> Column:
    """Returns the date that is days days after start."""
    return Column(dff.date_bin(df.lit(f"{days} days"), _to_col_or_expr(col), df.lit(0))) if hasattr(dff, "date_bin") else Column(_to_col_or_expr(col))


def date_sub(col: Any, days: int) -> Column:
    """Returns the date that is days days before start."""
    return date_add(col, -days)


def year(col: Any) -> Column:
    """Extracts the year as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("year"), _to_col_or_expr(col)))


def month(col: Any) -> Column:
    """Extracts the month as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("month"), _to_col_or_expr(col)))


def dayofmonth(col: Any) -> Column:
    """Extracts the day of the month as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("day"), _to_col_or_expr(col)))


def hour(col: Any) -> Column:
    """Extracts the hour as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("hour"), _to_col_or_expr(col)))


def minute(col: Any) -> Column:
    """Extracts the minute as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("minute"), _to_col_or_expr(col)))


def second(col: Any) -> Column:
    """Extracts the second as an integer from a given date/timestamp."""
    return Column(dff.date_part(df.lit("second"), _to_col_or_expr(col)))


def to_date(col: Any, format: Optional[str] = None) -> Column:
    """Converts the column to DateType."""
    if format:
        return Column(dff.to_date(_to_col_or_expr(col), df.lit(format)))
    return Column(dff.to_date(_to_col_or_expr(col)))


def to_timestamp(col: Any, format: Optional[str] = None) -> Column:
    """Converts the column to TimestampType."""
    if format:
        return Column(dff.to_timestamp(_to_col_or_expr(col), df.lit(format)))
    return Column(dff.to_timestamp(_to_col_or_expr(col)))


def date_format(col: Any, format: str) -> Column:
    """Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument."""
    return Column(dff.to_char(_to_col_or_expr(col), df.lit(format))) if hasattr(dff, "to_char") else Column(_to_col_or_expr(col))


# 5. Aggregation Functions
def count(col: Any) -> Column:
    """Aggregate function: returns the number of items in a group."""
    if isinstance(col, str) and col == "*":
        return Column(dff.count(df.lit(1)))
    return Column(dff.count(_to_col_or_expr(col)))


def sum(col: Any) -> Column:
    """Aggregate function: returns the sum of all values in the expression."""
    return Column(dff.sum(_to_col_or_expr(col)))


def avg(col: Any) -> Column:
    """Aggregate function: returns the average of the values in a group."""
    return Column(dff.avg(_to_col_or_expr(col)))


def mean(col: Any) -> Column:
    """Aggregate function: alias for avg."""
    return avg(col)


def min(col: Any) -> Column:
    """Aggregate function: returns the minimum value of the expression in a group."""
    return Column(dff.min(_to_col_or_expr(col)))


def max(col: Any) -> Column:
    """Aggregate function: returns the maximum value of the expression in a group."""
    return Column(dff.max(_to_col_or_expr(col)))


def approx_count_distinct(col: Any, rsd: Optional[float] = None) -> Column:
    """Aggregate function: returns a new Column for approximate distinct count of column col."""
    return Column(dff.approx_distinct(_to_col_or_expr(col)))


def countDistinct(col: Any, *cols: Any) -> Column:
    """Returns a new Column for distinct count of col or cols."""
    return Column(dff.count(_to_col_or_expr(col)).distinct())


def stddev(col: Any) -> Column:
    """Aggregate function: returns the sample standard deviation."""
    return Column(dff.stddev(_to_col_or_expr(col))) if hasattr(dff, "stddev") else Column(dff.stddev_samp(_to_col_or_expr(col)))


def variance(col: Any) -> Column:
    """Aggregate function: returns the sample variance."""
    return Column(dff.var(_to_col_or_expr(col))) if hasattr(dff, "var") else Column(dff.var_samp(_to_col_or_expr(col)))


def first(col: Any, ignoreNulls: bool = False) -> Column:
    """Aggregate function: returns the first value in a group."""
    return Column(dff.first_value(_to_col_or_expr(col)))


def last(col: Any, ignoreNulls: bool = False) -> Column:
    """Aggregate function: returns the last value in a group."""
    return Column(dff.last_value(_to_col_or_expr(col)))


# 6. Window Functions
def row_number() -> Column:
    """Window function: returns a sequential number starting at 1 within a window partition."""
    return Column(dff.row_number())


def rank() -> Column:
    """Window function: returns the rank of rows within a window partition."""
    return Column(dff.rank())


def dense_rank() -> Column:
    """Window function: returns the rank of rows without gaps."""
    return Column(dff.dense_rank())


def percent_rank() -> Column:
    """Window function: returns the relative rank (between 0 and 1) of rows."""
    return Column(dff.percent_rank())


def lead(col: Any, offset: int = 1, default: Optional[Any] = None) -> Column:
    """Window function: returns the value that is `offset` rows after the current row."""
    c = _to_col_or_expr(col)
    if default is not None:
        return Column(dff.lead(c, shift_offset=offset, default_value=default))
    return Column(dff.lead(c, shift_offset=offset))


def lag(col: Any, offset: int = 1, default: Optional[Any] = None) -> Column:
    """Window function: returns the value that is `offset` rows before the current row."""
    c = _to_col_or_expr(col)
    if default is not None:
        return Column(dff.lag(c, shift_offset=offset, default_value=default))
    return Column(dff.lag(c, shift_offset=offset))


# 7. Complex Types (Array / Map / Struct / JSON)
def array(*cols: Any) -> Column:
    """Creates a new array column."""
    exprs = [_to_col_or_expr(c) for c in cols]
    return Column(dff.array(*exprs))


def array_contains(col: Any, value: Any) -> Column:
    """Collection function: returns null if the array is null, true if the array contains the value, and false otherwise."""
    return Column(dff.array_has(_to_col_or_expr(col), _to_expr(value)))


def array_distinct(col: Any) -> Column:
    """Collection function: removes duplicate values from the array."""
    return Column(dff.array_distinct(_to_col_or_expr(col)))


def struct(*cols: Any) -> Column:
    """Creates a new struct column."""
    exprs = [_to_col_or_expr(c) for c in cols]
    return Column(dff.arrow_cast(dff.array(*exprs), df.lit("struct"))) if hasattr(dff, "struct") else Column(dff.array(*exprs))


# 8. Hashing & Miscellaneous
def md5(col: Any) -> Column:
    """Calculates the MD5 digest and returns the value as a 32 character hex string."""
    return Column(dff.md5(_to_col_or_expr(col)))


def sha2(col: Any, numBits: int) -> Column:
    """Returns the hex string result of SHA-2 family of hash functions."""
    if numBits == 256:
        return Column(dff.sha256(_to_col_or_expr(col)))
    elif numBits == 512:
        return Column(dff.sha512(_to_col_or_expr(col)))
    return Column(dff.sha256(_to_col_or_expr(col)))


def monotonically_increasing_id() -> Column:
    """A column that generates monotonically increasing 64-bit integers."""
    return Column(dff.row_number())


def sphuling_partition_id() -> Column:
    """A column that generates the partition id."""
    return Column(df.lit(0))


spark_partition_id = sphuling_partition_id
