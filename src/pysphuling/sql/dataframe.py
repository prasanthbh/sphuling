"""PySpark-compatible DataFrame and Row implementations for PySphuling."""

from __future__ import annotations
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING
import datafusion as df
import datafusion.functions as dff
import pandas as pd
import pyarrow as pa

from pysphuling.sql.column import Column, _to_expr
from pysphuling.sql.group import GroupedData
from pysphuling.sql.readwriter import DataFrameWriter
from pysphuling.sql.schema_bridge import arrow_schema_to_sphuling
from pysphuling.sql.types import StructType

if TYPE_CHECKING:
    from pysphuling.sql.session import SphulingSession


class Row:
    """A row of data in a DataFrame. Supports both positional and named key indexing."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if kwargs:
            self._fields = tuple(kwargs.keys())
            self._values = tuple(kwargs.values())
        else:
            self._fields = ()
            self._values = args

        self._dict: Dict[str, Any] = dict(zip(self._fields, self._values)) if self._fields else {}

    def asDict(self, recursive: bool = False) -> Dict[str, Any]:
        """Returns a copy of this row as a dict."""
        if not recursive:
            return dict(self._dict)
        res = {}
        for k, v in self._dict.items():
            if isinstance(v, Row):
                res[k] = v.asDict(recursive=True)
            elif isinstance(v, list):
                res[k] = [item.asDict(recursive=True) if isinstance(item, Row) else item for item in v]
            else:
                res[k] = v
        return res

    def __getitem__(self, item: Union[int, str]) -> Any:
        if isinstance(item, int):
            return self._values[item]
        return self._dict[item]

    def __getattr__(self, name: str) -> Any:
        if name in self._dict:
            return self._dict[name]
        raise AttributeError(f"'Row' object has no attribute '{name}'")

    def __contains__(self, item: str) -> bool:
        return item in self._dict

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __repr__(self) -> str:
        if self._fields:
            items = ", ".join(f"{k}={v!r}" for k, v in self._dict.items())
            return f"Row({items})"
        items = ", ".join(repr(v) for v in self._values)
        return f"Row({items})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Row):
            return self._fields == other._fields and self._values == other._values
        return False

    def __hash__(self) -> int:
        return hash((self._fields, self._values))


class DataFrameNaFunctions:
    """Functionality for dealing with missing data in a DataFrame."""

    def __init__(self, df_obj: DataFrame) -> None:
        self._df = df_obj

    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Sequence[str]]] = None,
    ) -> DataFrame:
        """Returns a new DataFrame omitting rows with null values."""
        cols = [subset] if isinstance(subset, str) else list(subset or self._df.columns)
        if how == "all":
            # null in all columns
            cond = None
            for c in cols:
                c_is_not_null = df.col(c).is_not_null()
                cond = c_is_not_null if cond is None else (cond | c_is_not_null)
            if cond is not None:
                return self._df.filter(Column(cond))
            return self._df
        else:
            # any null
            res = self._df
            for c in cols:
                res = res.filter(Column(df.col(c).is_not_null()))
            return res

    def fill(self, value: Any, subset: Optional[Union[str, Sequence[str]]] = None) -> DataFrame:
        """Replace null values with the specified value."""
        cols = [subset] if isinstance(subset, str) else list(subset or self._df.columns)
        res = self._df
        for c in cols:
            res = res.withColumn(c, Column(dff.coalesce(df.col(c), df.lit(value))))
        return res

    def replace(
        self,
        to_replace: Any,
        value: Optional[Any] = None,
        subset: Optional[Union[str, Sequence[str]]] = None,
    ) -> DataFrame:
        """Returns a new DataFrame replacing a value with another value."""
        cols = [subset] if isinstance(subset, str) else list(subset or self._df.columns)
        res = self._df
        if isinstance(to_replace, dict):
            for target, replacement in to_replace.items():
                for c in cols:
                    res = res.withColumn(
                        c,
                        Column(dff.when(df.col(c) == df.lit(target), df.lit(replacement)).otherwise(df.col(c)))
                    )
        else:
            for c in cols:
                res = res.withColumn(
                    c,
                    Column(dff.when(df.col(c) == df.lit(to_replace), df.lit(value)).otherwise(df.col(c)))
                )
        return res


class DataFrameStatFunctions:
    """Statistic functions for DataFrames."""

    def __init__(self, df_obj: DataFrame) -> None:
        self._df = df_obj

    def corr(self, col1: str, col2: str, method: str = "pearson") -> float:
        """Calculates the correlation of two columns of a DataFrame."""
        agg_df = self._df._df.aggregate(
            [],
            [dff.corr(df.col(col1), df.col(col2)).alias("corr")]
        )
        table = agg_df.to_arrow_table()
        val = table.column("corr")[0].as_py()
        return float(val) if val is not None else 0.0

    def cov(self, col1: str, col2: str) -> float:
        """Calculates the covariance of two columns of a DataFrame."""
        agg_df = self._df._df.aggregate(
            [],
            [dff.covar(df.col(col1), df.col(col2)).alias("cov")]
        )
        table = agg_df.to_arrow_table()
        val = table.column("cov")[0].as_py()
        return float(val) if val is not None else 0.0

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        """Computes a pair-wise frequency table of the given columns."""
        pdf = self._df.toPandas()
        ct = pd.crosstab(pdf[col1], pdf[col2]).reset_index()
        return self._df._session.createDataFrame(ct)


class DataFrame:
    """A distributed/multi-threaded collection of data organized into named columns.
    
    Equivalent to a PySpark DataFrame, powered by Apache DataFusion and Arrow.
    """

    def __init__(self, datafusion_df: df.DataFrame, session: Optional[SphulingSession] = None) -> None:
        self._df: df.DataFrame = datafusion_df
        self._session: Optional[SphulingSession] = session

    @property
    def schema(self) -> StructType:
        """Returns the schema of this DataFrame as a StructType. (Instant, zero-load!)."""
        arrow_schema = self._df.schema()
        return arrow_schema_to_sphuling(arrow_schema)

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list."""
        return self.schema.names

    @property
    def dtypes(self) -> List[Tuple[str, str]]:
        """Returns all column names and their data types as a list."""
        return [(f.name, f.dataType.simpleString()) for f in self.schema.fields]

    @property
    def write(self) -> DataFrameWriter:
        """Interface for saving the content of the DataFrame to external storage."""
        return DataFrameWriter(self)

    @property
    def na(self) -> DataFrameNaFunctions:
        """Returns a DataFrameNaFunctions object for handling missing values."""
        return DataFrameNaFunctions(self)

    @property
    def stat(self) -> DataFrameStatFunctions:
        """Returns a DataFrameStatFunctions object for statistic calculations."""
        return DataFrameStatFunctions(self)

    @property
    def rdd(self):
        """RDD is intentionally excluded in Sphuling in favor of high-speed Arrow memory."""
        raise NotImplementedError(
            "The low-level JVM RDD API is intentionally excluded from Sphuling. "
            "Use .toArrow(), .toPandas(), or .collect() to process batches with zero JVM overhead."
        )

    # 1. Transformations (Lazy Evaluation)
    def select(self, *cols: Union[str, Column, df.Expr]) -> DataFrame:
        """Projects a set of expressions and returns a new DataFrame."""
        exprs: List[df.Expr] = []
        for c in cols:
            if isinstance(c, str):
                exprs.append(df.col(c))
            elif isinstance(c, Column):
                exprs.append(c.expr)
            elif isinstance(c, df.Expr):
                exprs.append(c)
        return DataFrame(self._df.select(*exprs), self._session)

    def selectExpr(self, *exprs: str) -> DataFrame:
        """Projects a set of SQL expressions and returns a new DataFrame."""
        parsed_exprs = [self._df.parse_sql_expr(e) for e in exprs]
        return DataFrame(self._df.select(*parsed_exprs), self._session)

    def filter(self, condition: Union[Column, df.Expr, str]) -> DataFrame:
        """Filters rows using the given condition (where() is an alias)."""
        if isinstance(condition, str):
            cond_expr = self._df.parse_sql_expr(condition)
        elif isinstance(condition, Column):
            cond_expr = condition.expr
        else:
            cond_expr = condition
        return DataFrame(self._df.filter(cond_expr), self._session)

    def where(self, condition: Union[Column, df.Expr, str]) -> DataFrame:
        """Alias for filter()."""
        return self.filter(condition)

    def withColumn(self, colName: str, colExpr: Union[Column, df.Expr]) -> DataFrame:
        """Returns a new DataFrame by adding a column or replacing the existing column that has the same name."""
        expr = colExpr.expr if isinstance(colExpr, Column) else colExpr
        return DataFrame(self._df.with_column(colName, expr), self._session)

    def withColumns(self, colsMap: Dict[str, Union[Column, df.Expr]]) -> DataFrame:
        """Returns a new DataFrame by adding or replacing multiple columns."""
        res = self
        for name, expr in colsMap.items():
            res = res.withColumn(name, expr)
        return res

    def withColumnRenamed(self, existing: str, new: str) -> DataFrame:
        """Returns a new DataFrame by renaming an existing column."""
        return DataFrame(self._df.with_column_renamed(existing, new), self._session)

    def withColumnsRenamed(self, colsMap: Dict[str, str]) -> DataFrame:
        """Returns a new DataFrame by renaming multiple existing columns."""
        res = self
        for old_name, new_name in colsMap.items():
            res = res.withColumnRenamed(old_name, new_name)
        return res

    def drop(self, *cols: str) -> DataFrame:
        """Returns a new DataFrame that drops the specified column(s)."""
        return DataFrame(self._df.drop(*cols), self._session)

    def distinct(self) -> DataFrame:
        """Returns a new DataFrame containing only the unique rows."""
        return DataFrame(self._df.distinct(), self._session)

    def dropDuplicates(self, subset: Optional[List[str]] = None) -> DataFrame:
        """Return a new DataFrame with duplicate rows removed, optionally considering only certain columns."""
        if not subset:
            return self.distinct()
        # DataFusion distinct_on
        subset_cols = [df.col(c) for c in subset]
        return DataFrame(self._df.distinct_on(subset_cols), self._session)

    def limit(self, num: int) -> DataFrame:
        """Limits the result count to the number specified."""
        return DataFrame(self._df.limit(num), self._session)

    def tail(self, num: int) -> DataFrame:
        """Returns the last num rows."""
        return DataFrame(self._df.tail(num), self._session)

    def sort(self, *cols: Union[str, Column, df.Expr], ascending: Union[bool, List[bool]] = True) -> DataFrame:
        """Returns a new DataFrame sorted by the specified column(s)."""
        sort_exprs: List[df.Expr] = []
        for i, c in enumerate(cols):
            asc = ascending[i] if isinstance(ascending, list) else ascending
            if isinstance(c, str):
                sort_exprs.append(df.col(c).sort(ascending=asc, nulls_first=not asc))
            elif isinstance(c, Column):
                sort_exprs.append(c.expr)
            elif isinstance(c, df.Expr):
                sort_exprs.append(c)
        return DataFrame(self._df.sort(*sort_exprs), self._session)

    def orderBy(self, *cols: Union[str, Column, df.Expr], ascending: Union[bool, List[bool]] = True) -> DataFrame:
        """Alias for sort()."""
        return self.sort(*cols, ascending=ascending)

    def repartition(self, numPartitions: int, *cols: Union[str, Column]) -> DataFrame:
        """Returns a new DataFrame partitioned by the given number of partitions."""
        return DataFrame(self._df.repartition(numPartitions), self._session)

    def coalesce(self, numPartitions: int) -> DataFrame:
        """Returns a new DataFrame that has exactly numPartitions partitions."""
        return DataFrame(self._df.repartition(numPartitions), self._session)

    def join(
        self,
        other: DataFrame,
        on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
    ) -> DataFrame:
        """Joins with another DataFrame, using the given join expression."""
        clean_how = how.lower().replace("_outer", "").replace("_", "")
        how_map = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "full": "full",
            "outer": "full",
            "semi": "semi",
            "leftsemi": "semi",
            "anti": "anti",
            "leftanti": "anti",
        }
        mapped_how = how_map.get(clean_how, "inner")

        if on is None:
            # Cross join
            return self.crossJoin(other)

        join_keys = [on] if isinstance(on, str) else list(on)
        joined_df = self._df.join(other._df, on=join_keys, how=mapped_how)
        return DataFrame(joined_df, self._session)

    def crossJoin(self, other: DataFrame) -> DataFrame:
        """Returns the cartesian product with another DataFrame."""
        # Cartesian join in DataFusion can be achieved via join with no keys or crossJoin
        joined = self._df.join(other._df, on=[], how="inner")
        return DataFrame(joined, self._session)

    def union(self, other: DataFrame) -> DataFrame:
        """Return a new DataFrame containing union of rows in this and another DataFrame."""
        return DataFrame(self._df.union(other._df), self._session)

    def unionByName(self, other: DataFrame, allowMissingColumns: bool = False) -> DataFrame:
        """Returns a new DataFrame containing union of rows in this and another DataFrame by column name."""
        return DataFrame(self._df.union_by_name(other._df), self._session)

    def intersect(self, other: DataFrame) -> DataFrame:
        """Return a new DataFrame containing rows only in both this and another DataFrame."""
        return DataFrame(self._df.intersect(other._df), self._session)

    def subtract(self, other: DataFrame) -> DataFrame:
        """Return a new DataFrame containing rows in this DataFrame but not in another DataFrame."""
        return DataFrame(self._df.except_all(other._df), self._session)

    def exceptAll(self, other: DataFrame) -> DataFrame:
        return self.subtract(other)

    def groupBy(self, *cols: Union[str, Column, df.Expr]) -> GroupedData:
        """Groups the DataFrame using the specified columns, so we can run aggregation on them."""
        return GroupedData(self, cols)

    def transform(self, func: Callable[..., DataFrame], *args: Any, **kwargs: Any) -> DataFrame:
        """Concise syntax for chaining custom DataFrame transformations."""
        return func(self, *args, **kwargs)

    def cache(self) -> DataFrame:
        """Persists the DataFrame in DataFusion memory."""
        self._df.cache()
        return self

    def persist(self) -> DataFrame:
        return self.cache()

    def unpersist(self) -> DataFrame:
        return self

    def createOrReplaceTempView(self, name: str) -> None:
        """Creates or replaces a local temporary view with this DataFrame in the session."""
        if self._session:
            self._session._engine.ctx.register_table(name, self._df.into_view())
        else:
            raise RuntimeError("Cannot register temp view without an active SphulingSession")

    def createGlobalTempView(self, name: str) -> None:
        self.createOrReplaceTempView(name)

    # 2. Actions (Eager Execution)
    def collect(self) -> List[Row]:
        """Returns all the records as a list of Row."""
        table = self.toArrow()
        col_names = table.column_names
        pydict = table.to_pydict()
        num_rows = table.num_rows

        rows: List[Row] = []
        for i in range(num_rows):
            row_dict = {col: pydict[col][i] for col in col_names}
            rows.append(Row(**row_dict))
        return rows

    def show(self, n: int = 20, truncate: Union[bool, int] = True, vertical: bool = False) -> None:
        """Prints the first n rows to the console matching PySpark ASCII table format."""
        table = self.limit(n).toArrow()
        num_rows = table.num_rows
        col_names = table.column_names

        if num_rows == 0:
            print("++\n||\n++\n")
            return

        pydict = table.to_pydict()
        trunc_len = 20 if truncate is True else (truncate if isinstance(truncate, int) else None)

        def fmt_val(v: Any) -> str:
            if v is None:
                s = "null"
            else:
                s = str(v)
            if trunc_len and len(s) > trunc_len:
                return s[:trunc_len - 3] + "..."
            return s

        if not vertical:
            col_widths = {c: len(c) for c in col_names}
            for c in col_names:
                for val in pydict[c]:
                    col_widths[c] = max(col_widths[c], len(fmt_val(val)))

            # Build separator
            sep = "+" + "+".join("-" * (col_widths[c] + 2) for c in col_names) + "+"
            header = "|" + "|".join(f" {c.ljust(col_widths[c])} " for c in col_names) + "|"

            print(sep)
            print(header)
            print(sep)
            for i in range(num_rows):
                row_str = "|" + "|".join(f" {fmt_val(pydict[c][i]).ljust(col_widths[c])} " for c in col_names) + "|"
                print(row_str)
            print(sep)
            if self.count() > n:
                print(f"only showing top {n} rows\n")
        else:
            for i in range(num_rows):
                print(f"-RECORD {i}-------------------")
                max_k = max(len(c) for c in col_names)
                for c in col_names:
                    print(f" {c.ljust(max_k)} | {fmt_val(pydict[c][i])}")

    def count(self) -> int:
        """Returns the number of rows in this DataFrame."""
        return self._df.count()

    def first(self) -> Optional[Row]:
        """Returns the first row as a Row."""
        rows = self.head(1)
        return rows if isinstance(rows, Row) else (rows[0] if rows else None)

    def head(self, n: int = 1) -> Union[Optional[Row], List[Row]]:
        """Returns the first n rows. If n is 1, returns a single Row or None."""
        limited = self.limit(n).collect()
        if n == 1:
            return limited[0] if limited else None
        return limited

    def take(self, num: int) -> List[Row]:
        """Returns the first num rows as a list of Row."""
        return self.limit(num).collect()

    def toPandas(self) -> pd.DataFrame:
        """Returns the contents of this DataFrame as Pandas DataFrame (zero-copy Arrow bridge)."""
        return self._df.to_pandas()

    def toArrow(self) -> pa.Table:
        """Returns the contents of this DataFrame as an Apache Arrow Table."""
        return self._df.to_arrow_table()

    def printSchema(self) -> None:
        """Prints out the schema in the tree format."""
        print(self.schema.treeString())

    def explain(self, mode: str = "simple") -> None:
        """Prints the (logical and physical) plans to the console for debugging purposes."""
        self._df.explain()

    def describe(self, *cols: str) -> DataFrame:
        """Computes basic statistics for numeric and string columns."""
        target_cols = cols if cols else self.columns
        agg_exprs = []
        for c in target_cols:
            agg_exprs.extend([
                dff.count(df.col(c)).alias(f"{c}_count"),
                dff.avg(df.col(c)).alias(f"{c}_mean"),
                dff.min(df.col(c)).alias(f"{c}_min"),
                dff.max(df.col(c)).alias(f"{c}_max"),
            ])
        agg_df = self._df.aggregate([], agg_exprs)
        # Format as Spark describe table
        pydict = agg_df.to_arrow_table().to_pydict()
        summary_rows = ["count", "mean", "min", "max"]
        out_dict: Dict[str, List[Any]] = {"summary": summary_rows}
        for c in target_cols:
            out_dict[c] = [
                str(pydict[f"{c}_count"][0]),
                str(round(pydict[f"{c}_mean"][0], 4)) if pydict[f"{c}_mean"][0] is not None else "null",
                str(pydict[f"{c}_min"][0]),
                str(pydict[f"{c}_max"][0]),
            ]
        return self._session.createDataFrame(pd.DataFrame(out_dict)) if self._session else None  # type: ignore[return-value]

    def summary(self, *statistics: str) -> DataFrame:
        return self.describe()

    def __getitem__(self, item: Union[str, Column]) -> Column:
        """Returns the column as a Column."""
        if isinstance(item, str):
            return Column(df.col(item))
        elif isinstance(item, Column):
            return self.filter(item)  # PySpark syntax: df[df["id"] > 1]
        raise KeyError(f"Unsupported item type for DataFrame.__getitem__: {type(item)}")

    def __getattr__(self, name: str) -> Column:
        """Allow df.colName syntax."""
        if name.startswith("_"):
            raise AttributeError(f"DataFrame object has no attribute '{name}'")
        if name in self.columns:
            return Column(df.col(name))
        raise AttributeError(f"DataFrame object has no column or attribute '{name}'")

