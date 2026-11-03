"""SphulingSession and SessionBuilder implementation for PySphuling."""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING
import datafusion as df
import pandas as pd
import pyarrow as pa

from pysphuling.conf.sphuling_conf import SphulingConf, SparkConf
from pysphuling.core.engine import SphulingEngine
from pysphuling.core.sql_transpiler import transpile_spark_sql
from pysphuling.sql.catalog import Catalog
from pysphuling.sql.dataframe import DataFrame
from pysphuling.sql.readwriter import DataFrameReader
from pysphuling.sql.schema_bridge import sphuling_schema_to_arrow
from pysphuling.sql.types import StructType


class SphulingSession:
    """The entry point to programming PySphuling with the Dataset and DataFrame API.

    Drop-in replacement for PySpark's SparkSession, powered by DataFusion and Arrow.
    """

    _active_session: Optional[SphulingSession] = None
    _default_session: Optional[SphulingSession] = None

    class Builder:
        """Builder for SphulingSession."""

        def __init__(self) -> None:
            self._conf = SphulingConf()

        def appName(self, name: str) -> SphulingSession.Builder:
            self._conf.setAppName(name)
            return self

        def master(self, master: str) -> SphulingSession.Builder:
            self._conf.setMaster(master)
            return self

        def config(self, key: Optional[str] = None, value: Optional[Any] = None, conf: Optional[SphulingConf] = None) -> SphulingSession.Builder:
            if conf is not None:
                for k, v in conf.getAll():
                    self._conf.set(k, v)
            elif key is not None and value is not None:
                self._conf.set(key, value)
            return self

        def enableHiveSupport(self) -> SphulingSession.Builder:
            self._conf.set("sphuling.sql.catalogImplementation", "hive")
            return self

        def getOrCreate(self) -> SphulingSession:
            if SphulingSession._active_session is not None:
                # Update config on active session
                for k, v in self._conf.getAll():
                    SphulingSession._active_session._conf.set(k, v)
                return SphulingSession._active_session

            session = SphulingSession(self._conf)
            SphulingSession._active_session = session
            SphulingSession._default_session = session
            return session

    builder: Builder = Builder()

    def __init__(self, conf: Optional[SphulingConf] = None) -> None:
        self._conf = conf or SphulingConf()
        self._engine = SphulingEngine(self._conf)
        self._catalog = Catalog(self)

    @property
    def conf(self) -> SphulingConf:
        """Runtime configuration interface for Sphuling."""
        return self._conf

    @property
    def catalog(self) -> Catalog:
        """Interface through which the user may create, drop, alter or query underlying databases, tables, etc."""
        return self._catalog

    @property
    def read(self) -> DataFrameReader:
        """Returns a DataFrameReader that can be used to read data in as a DataFrame."""
        return DataFrameReader(self)

    def sql(self, sqlQuery: str) -> DataFrame:
        """Returns a DataFrame representing the result of the given query."""
        # 1. Transpile Spark SQL dialect to DataFusion-compliant ANSI SQL
        normalized_sql = transpile_spark_sql(sqlQuery)
        # 2. Execute on DataFusion context
        ddf = self._engine.ctx.sql(normalized_sql)
        return DataFrame(ddf, self)

    def table(self, tableName: str) -> DataFrame:
        """Returns the specified table/view as a DataFrame."""
        ddf = self._engine.ctx.table(tableName)
        return DataFrame(ddf, self)

    def range(
        self,
        start: int,
        end: Optional[int] = None,
        step: int = 1,
        numPartitions: Optional[int] = None,
    ) -> DataFrame:
        """Create a DataFrame with single LongType column named 'id', containing elements in a range from start to end with step value."""
        if end is None:
            actual_start = 0
            actual_end = start
        else:
            actual_start = start
            actual_end = end

        ids = list(range(actual_start, actual_end, step))
        table = pa.Table.from_pydict({"id": ids})
        ddf = self._engine.ctx.from_arrow(table)
        if numPartitions:
            ddf = ddf.repartition(numPartitions)
        return DataFrame(ddf, self)

    def createDataFrame(
        self,
        data: Any,
        schema: Optional[Union[StructType, Sequence[str], str]] = None,
    ) -> DataFrame:
        """Creates a DataFrame from an RDD, a list or a pandas.DataFrame."""
        # Case 1: Pandas DataFrame
        if isinstance(data, pd.DataFrame):
            if schema:
                if isinstance(schema, (list, tuple)):
                    data.columns = list(schema)
            pa_table = pa.Table.from_pandas(data)
            ddf = self._engine.ctx.from_arrow(pa_table)
            return DataFrame(ddf, self)

        # Case 2: PyArrow Table
        if isinstance(data, pa.Table):
            ddf = self._engine.ctx.from_arrow(data)
            return DataFrame(ddf, self)

        # Case 3: List of dicts
        if isinstance(data, (list, tuple)) and data and isinstance(data[0], dict):
            pa_table = pa.Table.from_pylist(list(data))
            ddf = self._engine.ctx.from_arrow(pa_table)
            return DataFrame(ddf, self)

        # Case 4: List of tuples / rows
        if isinstance(data, (list, tuple)):
            if not data:
                # Empty dataframe
                arrow_schema = sphuling_schema_to_arrow(schema) if isinstance(schema, StructType) else pa.schema([])
                empty_table = pa.Table.from_arrays([], schema=arrow_schema)
                ddf = self._engine.ctx.from_arrow(empty_table)
                return DataFrame(ddf, self)

            col_names: List[str]
            if isinstance(schema, StructType):
                col_names = schema.names
            elif isinstance(schema, (list, tuple)):
                col_names = list(schema)
            elif isinstance(schema, str):
                parsed = StructType.fromDDL(schema)
                col_names = parsed.names
            else:
                col_names = [f"_{i+1}" for i in range(len(data[0]))]

            # Transpose rows to columns
            num_cols = len(col_names)
            col_data = {col_names[j]: [row[j] for row in data] for j in range(num_cols)}
            pa_table = pa.Table.from_pydict(col_data)
            ddf = self._engine.ctx.from_arrow(pa_table)
            return DataFrame(ddf, self)

        raise ValueError(f"Unsupported data source for createDataFrame: {type(data)}")

    def stop(self) -> None:
        """Stop the underlying SphulingEngine and releases resources."""
        self._engine.stop()
        if SphulingSession._active_session is self:
            SphulingSession._active_session = None
        if SphulingSession._default_session is self:
            SphulingSession._default_session = None

    @classmethod
    def getActiveSession(cls) -> Optional[SphulingSession]:
        return cls._active_session

    @classmethod
    def getDefaultSession(cls) -> Optional[SphulingSession]:
        return cls._default_session


# Drop-in alias for PySpark compatibility
SparkSession = SphulingSession

