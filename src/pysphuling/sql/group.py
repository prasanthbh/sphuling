"""GroupedData implementation for PySphuling."""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING
import datafusion as df
import datafusion.functions as dff

from pysphuling.sql.column import Column, _to_expr

if TYPE_CHECKING:
    from pysphuling.sql.dataframe import DataFrame


class GroupedData:
    """A set of methods for aggregations on a DataFrame after a groupBy."""

    def __init__(self, df_obj: DataFrame, group_cols: Sequence[Union[str, Column, df.Expr]]) -> None:
        self._df = df_obj
        self._group_cols: List[df.Expr] = []
        for c in group_cols:
            if isinstance(c, str):
                self._group_cols.append(df.col(c))
            elif isinstance(c, Column):
                self._group_cols.append(c.expr)
            elif isinstance(c, df.Expr):
                self._group_cols.append(c)

    def agg(self, *exprs: Union[Column, df.Expr, Dict[str, str]]) -> DataFrame:
        """Computes aggregates and returns the result as a DataFrame."""
        from pysphuling.sql.dataframe import DataFrame

        agg_exprs: List[df.Expr] = []
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # PySpark syntax: df.groupBy("dept").agg({"salary": "avg", "age": "max"})
            agg_map = exprs[0]
            func_map = {
                "count": dff.count,
                "sum": dff.sum,
                "avg": dff.avg,
                "mean": dff.avg,
                "min": dff.min,
                "max": dff.max,
            }
            for col_name, func_name in agg_map.items():
                fn = func_map.get(func_name.lower())
                if fn:
                    agg_exprs.append(fn(df.col(col_name)).alias(f"{func_name}({col_name})"))
        else:
            for e in exprs:
                if isinstance(e, Column):
                    agg_exprs.append(e.expr)
                elif isinstance(e, df.Expr):
                    agg_exprs.append(e)

        result_df = self._df._df.aggregate(self._group_cols, agg_exprs)
        return DataFrame(result_df, self._df._session)

    def count(self) -> DataFrame:
        """Count the number of rows for each group."""
        from pysphuling.sql.dataframe import DataFrame
        result_df = self._df._df.aggregate(self._group_cols, [dff.count(df.lit(1)).alias("count")])
        return DataFrame(result_df, self._df._session)

    def sum(self, *cols: str) -> DataFrame:
        """Compute the sum for each numeric columns for each group."""
        from pysphuling.sql.dataframe import DataFrame
        target_cols = cols if cols else [c for c in self._df.columns if c not in [x.name() if hasattr(x, 'name') else str(x) for x in self._group_cols]]
        agg_exprs = [dff.sum(df.col(c)).alias(f"sum({c})") for c in target_cols]
        result_df = self._df._df.aggregate(self._group_cols, agg_exprs)
        return DataFrame(result_df, self._df._session)

    def avg(self, *cols: str) -> DataFrame:
        """Compute the mean value for each numeric columns for each group."""
        from pysphuling.sql.dataframe import DataFrame
        target_cols = cols if cols else [c for c in self._df.columns if c not in [x.name() if hasattr(x, 'name') else str(x) for x in self._group_cols]]
        agg_exprs = [dff.avg(df.col(c)).alias(f"avg({c})") for c in target_cols]
        result_df = self._df._df.aggregate(self._group_cols, agg_exprs)
        return DataFrame(result_df, self._df._session)

    def mean(self, *cols: str) -> DataFrame:
        return self.avg(*cols)

    def min(self, *cols: str) -> DataFrame:
        """Compute the min value for each numeric columns for each group."""
        from pysphuling.sql.dataframe import DataFrame
        target_cols = cols if cols else [c for c in self._df.columns if c not in [x.name() if hasattr(x, 'name') else str(x) for x in self._group_cols]]
        agg_exprs = [dff.min(df.col(c)).alias(f"min({c})") for c in target_cols]
        result_df = self._df._df.aggregate(self._group_cols, agg_exprs)
        return DataFrame(result_df, self._df._session)

    def max(self, *cols: str) -> DataFrame:
        """Compute the max value for each numeric columns for each group."""
        from pysphuling.sql.dataframe import DataFrame
        target_cols = cols if cols else [c for c in self._df.columns if c not in [x.name() if hasattr(x, 'name') else str(x) for x in self._group_cols]]
        agg_exprs = [dff.max(df.col(c)).alias(f"max({c})") for c in target_cols]
        result_df = self._df._df.aggregate(self._group_cols, agg_exprs)
        return DataFrame(result_df, self._df._session)

