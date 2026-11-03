"""DataFrameReader and DataFrameWriter for PySphuling."""

from __future__ import annotations
import os
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING
import datafusion as df
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.feather as pfeather
import pyarrow.parquet as pq

from pysphuling.schema.inspector import SchemaInspector
from pysphuling.sql.schema_bridge import sphuling_schema_to_arrow
from pysphuling.sql.types import StructType

if TYPE_CHECKING:
    from pysphuling.sql.dataframe import DataFrame
    from pysphuling.sql.session import SphulingSession


class DataFrameReader:
    """Interface used to load a DataFrame from external storage systems."""

    def __init__(self, session: SphulingSession) -> None:
        self._session = session
        self._format: str = "parquet"
        self._schema: Optional[StructType] = None
        self._options: Dict[str, Any] = {}

    def schema(self, schema: Union[StructType, str]) -> DataFrameReader:
        """Specifies the input schema."""
        if isinstance(schema, str):
            self._schema = StructType.fromDDL(schema)
        else:
            self._schema = schema
        return self

    def option(self, key: str, value: Any) -> DataFrameReader:
        """Adds an input option for the underlying data source."""
        self._options[key] = value
        return self

    def options(self, **options: Any) -> DataFrameReader:
        """Adds input options for the underlying data source."""
        self._options.update(options)
        return self

    def format(self, source: str) -> DataFrameReader:
        """Specifies the input data source format."""
        self._format = source.lower()
        return self

    def schema_of(self, source: Any, format: Optional[str] = None, **options) -> StructType:
        """Zero-Load Schema Extraction: Returns StructType without loading full dataset."""
        opts = dict(self._options)
        opts.update(options)
        return SchemaInspector.inspect(source, format=format or self._format, **opts)

    def parquet(self, *paths: str) -> DataFrame:
        """Loads Parquet files, returning the result as a DataFrame."""
        from pysphuling.sql.dataframe import DataFrame

        if not paths:
            raise ValueError("At least one path must be specified for parquet read")
        
        # Read parquet directly using DataFusion's high-speed multithreaded reader
        first_path = paths[0]
        ddf = self._session._engine.ctx.read_parquet(first_path)
        return DataFrame(ddf, self._session)

    def csv(
        self,
        path: str,
        header: bool = True,
        inferSchema: bool = True,
        sep: str = ",",
        **kwargs,
    ) -> DataFrame:
        """Loads a CSV file and returns the result as a DataFrame."""
        from pysphuling.sql.dataframe import DataFrame

        # Fast reading via DataFusion's native CSV reader
        schema_arrow = sphuling_schema_to_arrow(self._schema) if self._schema else None
        ddf = self._session._engine.ctx.read_csv(
            path,
            has_header=header,
            delimiter=sep,
            schema=schema_arrow,
        )
        return DataFrame(ddf, self._session)

    def json(self, path: str, **kwargs) -> DataFrame:
        """Loads JSON files and returns the results as a DataFrame."""
        from pysphuling.sql.dataframe import DataFrame

        schema_arrow = sphuling_schema_to_arrow(self._schema) if self._schema else None
        ddf = self._session._engine.ctx.read_json(path, schema=schema_arrow)
        return DataFrame(ddf, self._session)

    def arrow(self, *paths: str) -> DataFrame:
        """Loads Feather / Arrow IPC files and returns the result as a DataFrame."""
        from pysphuling.sql.dataframe import DataFrame

        table = pfeather.read_table(paths[0])
        ddf = self._session._engine.ctx.from_arrow(table)
        return DataFrame(ddf, self._session)

    def feather(self, *paths: str) -> DataFrame:
        return self.arrow(*paths)

    def table(self, tableName: str) -> DataFrame:
        """Returns the specified table/view as a DataFrame."""
        return self._session.table(tableName)

    def load(self, path: Optional[str] = None, **options) -> DataFrame:
        """Loads data in the specified format."""
        if options:
            self._options.update(options)
        if not path:
            path = self._options.get("path")
        if not path:
            raise ValueError("Path must be specified to load data")

        fmt = self._format
        if fmt == "parquet":
            return self.parquet(path)
        elif fmt == "csv":
            return self.csv(path, **self._options)
        elif fmt == "json":
            return self.json(path, **self._options)
        elif fmt in ("arrow", "feather"):
            return self.arrow(path)
        else:
            return self.parquet(path)


class DataFrameWriter:
    """Interface used to write a DataFrame to external storage systems."""

    def __init__(self, df_obj: DataFrame) -> None:
        self._df = df_obj
        self._mode: str = "error"
        self._format: str = "parquet"
        self._partition_cols: List[str] = []
        self._options: Dict[str, Any] = {}

    def mode(self, saveMode: str) -> DataFrameWriter:
        """Specifies the behavior when data or table already exists."""
        self._mode = saveMode.lower()
        return self

    def partitionBy(self, *cols: str) -> DataFrameWriter:
        """Partitions the output by the given columns on the file system."""
        self._partition_cols = list(cols)
        return self

    def format(self, source: str) -> DataFrameWriter:
        """Specifies the output data source format."""
        self._format = source.lower()
        return self

    def option(self, key: str, value: Any) -> DataFrameWriter:
        self._options[key] = value
        return self

    def options(self, **options: Any) -> DataFrameWriter:
        self._options.update(options)
        return self

    def parquet(self, path: str, compression: str = "snappy") -> None:
        """Saves the content of the DataFrame in Parquet format at the specified path."""
        # Check overwrite mode
        if os.path.exists(path):
            if self._mode in ("error", "errorifexists"):
                raise FileExistsError(f"Target path '{path}' already exists and mode is 'error'")
            elif self._mode == "ignore":
                return

        # Use PyArrow to write with partitionBy or direct write
        table = self._df.toArrow()
        if self._partition_cols:
            pq.write_to_dataset(
                table,
                root_path=path,
                partition_cols=self._partition_cols,
                compression=compression,
            )
        else:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            pq.write_table(table, path, compression=compression)

    def csv(self, path: str, header: bool = True, sep: str = ",") -> None:
        """Saves the content of the DataFrame in CSV format at the specified path."""
        if os.path.exists(path) and self._mode in ("error", "errorifexists"):
            raise FileExistsError(f"Target path '{path}' already exists")
        table = self._df.toArrow()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        write_options = pcsv.WriteOptions(include_header=header, delimiter=sep)
        pcsv.write_csv(table, path, write_options=write_options)

    def json(self, path: str) -> None:
        """Saves the content of the DataFrame in JSON format at the specified path."""
        df_pd = self._df.toPandas()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        df_pd.to_json(path, orient="records", lines=True)

    def arrow(self, path: str) -> None:
        """Saves the content of the DataFrame in Arrow / Feather format."""
        table = self._df.toArrow()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        pfeather.write_feather(table, path)

    def feather(self, path: str) -> None:
        self.arrow(path)

    def saveAsTable(self, tableName: str) -> None:
        """Saves the content of the DataFrame as the specified table."""
        self._df.createOrReplaceTempView(tableName)

    def save(self, path: Optional[str] = None, **options) -> None:
        """Saves the content of the DataFrame to the data source."""
        if options:
            self._options.update(options)
        target = path or self._options.get("path")
        if not target:
            raise ValueError("Target path must be specified")

        fmt = self._format
        if fmt == "parquet":
            self.parquet(target)
        elif fmt == "csv":
            self.csv(target, **self._options)
        elif fmt == "json":
            self.json(target)
        elif fmt in ("arrow", "feather"):
            self.arrow(target)
        else:
            self.parquet(target)

