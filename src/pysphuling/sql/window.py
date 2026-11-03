"""PySpark-compatible Window specification for PySphuling."""

from __future__ import annotations
import sys
from typing import Any, List, Optional, Sequence, Union
import datafusion as df
import datafusion.expr as de

from pysphuling.sql.column import Column, _to_expr


class Window:
    """Utility functions for creating WindowSpec objects."""

    unboundedPreceding: int = -sys.maxsize
    unboundedFollowing: int = sys.maxsize
    currentRow: int = 0

    @classmethod
    def partitionBy(cls, *cols: Union[str, Column, df.Expr]) -> WindowSpec:
        return WindowSpec().partitionBy(*cols)

    @classmethod
    def orderBy(cls, *cols: Union[str, Column, df.Expr]) -> WindowSpec:
        return WindowSpec().orderBy(*cols)

    @classmethod
    def rowsBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rowsBetween(start, end)

    @classmethod
    def rangeBetween(cls, start: int, end: int) -> WindowSpec:
        return WindowSpec().rangeBetween(start, end)


class WindowSpec:
    """A window specification that defines partitioning, ordering, and frame boundaries."""

    def __init__(self) -> None:
        self._partition_by: List[df.Expr] = []
        self._order_by: List[df.Expr] = []
        self._frame: Optional[df.WindowFrame] = None

    def partitionBy(self, *cols: Union[str, Column, df.Expr]) -> WindowSpec:
        spec = self._clone()
        for c in cols:
            if isinstance(c, (list, tuple)):
                for inner in c:
                    spec._partition_by.append(_to_col_expr(inner))
            else:
                spec._partition_by.append(_to_col_expr(c))
        return spec

    def orderBy(self, *cols: Union[str, Column, df.Expr]) -> WindowSpec:
        spec = self._clone()
        for c in cols:
            if isinstance(c, (list, tuple)):
                for inner in c:
                    spec._order_by.append(_to_sort_expr(inner))
            else:
                spec._order_by.append(_to_sort_expr(c))
        return spec

    def rowsBetween(self, start: int, end: int) -> WindowSpec:
        spec = self._clone()
        start_val = None if start == Window.unboundedPreceding else start
        end_val = None if end == Window.unboundedFollowing else end
        spec._frame = df.WindowFrame("rows", start_val, end_val)
        return spec

    def rangeBetween(self, start: int, end: int) -> WindowSpec:
        spec = self._clone()
        start_val = None if start == Window.unboundedPreceding else start
        end_val = None if end == Window.unboundedFollowing else end
        spec._frame = df.WindowFrame("range", start_val, end_val)
        return spec

    def _to_datafusion_window(self) -> de.Window:
        return de.Window(
            partition_by=self._partition_by if self._partition_by else None,
            order_by=self._order_by if self._order_by else None,
            window_frame=self._frame,
        )

    def _clone(self) -> WindowSpec:
        new_spec = WindowSpec()
        new_spec._partition_by = list(self._partition_by)
        new_spec._order_by = list(self._order_by)
        new_spec._frame = self._frame
        return new_spec


def _to_col_expr(c: Union[str, Column, df.Expr]) -> df.Expr:
    if isinstance(c, str):
        return df.col(c)
    elif isinstance(c, Column):
        return c.expr
    return c


def _to_sort_expr(c: Union[str, Column, df.Expr]) -> df.Expr:
    if isinstance(c, str):
        return df.col(c).sort(ascending=True, nulls_first=False)
    elif isinstance(c, Column):
        return c.expr
    return c

