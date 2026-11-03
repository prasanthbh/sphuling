"""Spark-compatible Column abstraction for PySphuling."""

from __future__ import annotations
from typing import Any, List, Optional, Sequence, Union, TYPE_CHECKING
import datafusion as df
import datafusion.expr as de
import datafusion.functions as dff
import pyarrow as pa

if TYPE_CHECKING:
    from pysphuling.sql.types import DataType
    from pysphuling.sql.window import WindowSpec


class Column:
    """A column or expression in a PySphuling DataFrame.

    Wraps Apache DataFusion's native `Expr` and provides full PySpark operator overloading.
    """

    def __init__(self, expr: Union[df.Expr, de.SortExpr, str, Any]) -> None:
        if isinstance(expr, (df.Expr, de.SortExpr)):
            self._expr = expr
        elif isinstance(expr, str):
            self._expr = df.col(expr)
        elif isinstance(expr, Column):
            self._expr = expr._expr
        else:
            self._expr = df.lit(expr)

    @property
    def expr(self) -> Union[df.Expr, de.SortExpr]:
        return self._expr

    # Arithmetic operators
    def __add__(self, other: Any) -> Column:
        return Column(self._expr + _to_expr(other))

    def __radd__(self, other: Any) -> Column:
        return Column(_to_expr(other) + self._expr)

    def __sub__(self, other: Any) -> Column:
        return Column(self._expr - _to_expr(other))

    def __rsub__(self, other: Any) -> Column:
        return Column(_to_expr(other) - self._expr)

    def __mul__(self, other: Any) -> Column:
        return Column(self._expr * _to_expr(other))

    def __rmul__(self, other: Any) -> Column:
        return Column(_to_expr(other) * self._expr)

    def __truediv__(self, other: Any) -> Column:
        return Column(self._expr / _to_expr(other))

    def __rtruediv__(self, other: Any) -> Column:
        return Column(_to_expr(other) / self._expr)

    def __mod__(self, other: Any) -> Column:
        return Column(self._expr % _to_expr(other))

    def __rmod__(self, other: Any) -> Column:
        return Column(_to_expr(other) % self._expr)

    def __neg__(self) -> Column:
        return Column(-self._expr)

    # Comparison operators
    def __eq__(self, other: Any) -> Column:  # type: ignore[override]
        return Column(self._expr == _to_expr(other))

    def __ne__(self, other: Any) -> Column:  # type: ignore[override]
        return Column(self._expr != _to_expr(other))

    def __lt__(self, other: Any) -> Column:
        return Column(self._expr < _to_expr(other))

    def __le__(self, other: Any) -> Column:
        return Column(self._expr <= _to_expr(other))

    def __gt__(self, other: Any) -> Column:
        return Column(self._expr > _to_expr(other))

    def __ge__(self, other: Any) -> Column:
        return Column(self._expr >= _to_expr(other))

    # Logical operators
    def __and__(self, other: Any) -> Column:
        return Column(self._expr & _to_expr(other))

    def __rand__(self, other: Any) -> Column:
        return Column(_to_expr(other) & self._expr)

    def __or__(self, other: Any) -> Column:
        return Column(self._expr | _to_expr(other))

    def __ror__(self, other: Any) -> Column:
        return Column(_to_expr(other) | self._expr)

    def __invert__(self) -> Column:
        return Column(~self._expr)

    # Spark Column methods
    def alias(self, alias_name: str) -> Column:
        """Returns this column with a new alias name."""
        return Column(self._expr.alias(alias_name))

    def name(self, name: str) -> Column:
        return self.alias(name)

    def cast(self, dataType: Union[str, DataType, pa.DataType]) -> Column:
        """Casts column to the specified data type."""
        from pysphuling.sql.schema_bridge import sphuling_type_to_arrow
        from pysphuling.sql.types import DataType, _parse_type_str

        if isinstance(dataType, str):
            parsed = _parse_type_str(dataType)
            pa_type = sphuling_type_to_arrow(parsed)
        elif isinstance(dataType, DataType):
            pa_type = sphuling_type_to_arrow(dataType)
        elif isinstance(dataType, pa.DataType):
            pa_type = dataType
        else:
            pa_type = pa.string()

        return Column(self._expr.cast(pa_type))

    def asc(self) -> Column:
        """Returns sort expression in ascending order (nulls last by default)."""
        return Column(self._expr.sort(ascending=True, nulls_first=False))

    def desc(self) -> Column:
        """Returns sort expression in descending order (nulls first by default)."""
        return Column(self._expr.sort(ascending=False, nulls_first=True))

    def asc_nulls_first(self) -> Column:
        return Column(self._expr.sort(ascending=True, nulls_first=True))

    def asc_nulls_last(self) -> Column:
        return Column(self._expr.sort(ascending=True, nulls_first=False))

    def desc_nulls_first(self) -> Column:
        return Column(self._expr.sort(ascending=False, nulls_first=True))

    def desc_nulls_last(self) -> Column:
        return Column(self._expr.sort(ascending=False, nulls_first=False))

    def isNull(self) -> Column:
        """True if current column value is null."""
        return Column(self._expr.is_null())

    def isNotNull(self) -> Column:
        """True if current column value is not null."""
        return Column(self._expr.is_not_null())

    def isnan(self) -> Column:
        """True if current column value is NaN."""
        return Column(self._expr.isnan())

    def isin(self, *values: Any) -> Column:
        """True if current expression is contained within values."""
        # Unpack list/tuple if single argument
        if len(values) == 1 and isinstance(values[0], (list, tuple, set)):
            vals = list(values[0])
        else:
            vals = list(values)

        expr_vals = [_to_expr(v) for v in vals]
        # In DataFusion: Expr.is_in(list) or chaining
        # If in_list is available:
        import datafusion.expr as de
        if hasattr(de, "InList"):
            in_list_expr = de.InList(self._expr, expr_vals, False)
            return Column(in_list_expr)
        
        # Fallback: chain equality with OR
        cond = (self == vals[0])
        for v in vals[1:]:
            cond = cond | (self == v)
        return cond

    def between(self, lower: Any, upper: Any) -> Column:
        """True if current expression is between lower and upper inclusive."""
        return Column(self._expr.between(_to_expr(lower), _to_expr(upper)))

    def startswith(self, prefix: str) -> Column:
        """True if current string starts with prefix."""
        return Column(dff.starts_with(self._expr, df.lit(prefix)))

    def endswith(self, suffix: str) -> Column:
        """True if current string ends with suffix."""
        return Column(dff.ends_with(self._expr, df.lit(suffix)))

    def contains(self, substr: str) -> Column:
        """True if current string contains substr."""
        return Column(dff.strpos(self._expr, df.lit(substr)) > df.lit(0))

    def substr(self, startPos: int, length: int) -> Column:
        """Substring starting at 1-based startPos with length."""
        return Column(dff.substr(self._expr, df.lit(startPos), df.lit(length)))

    def getItem(self, key: Any) -> Column:
        """Retrieves item from array (0 or 1-based) or key from map/struct."""
        return Column(dff.array_element(self._expr, _to_expr(key)))

    def getField(self, fieldName: str) -> Column:
        """Extracts field from nested struct."""
        return Column(dff.arrow_field(self._expr, df.lit(fieldName)))

    def eqNullSafe(self, other: Any) -> Column:
        """Null-safe equality test (<=> in SQL)."""
        other_expr = _to_expr(other)
        # (self == other) OR (self IS NULL AND other IS NULL)
        cond = (
            (self._expr == other_expr)
            | (self._expr.is_null() & other_expr.is_null())
        )
        return Column(cond)

    def over(self, windowSpec: WindowSpec) -> Column:
        """Evaluates this window/aggregate expression over a WindowSpec."""
        df_win = windowSpec._to_datafusion_window()
        return Column(self._expr.over(df_win))

    def __repr__(self) -> str:
        return f"Column<'{self._expr}'>"


def _to_expr(val: Any) -> Union[df.Expr, de.SortExpr]:
    """Helper to ensure an object is a DataFusion Expr."""
    if isinstance(val, Column):
        return val.expr
    elif isinstance(val, (df.Expr, de.SortExpr)):
        return val
    elif isinstance(val, str):
        # In PySpark, string literals inside Column operators are treated as literals
        return df.lit(val)
    else:
        return df.lit(val)
