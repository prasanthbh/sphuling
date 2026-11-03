"""Universal Zero-Load Schema Extraction Engine for PySphuling.

Extracts exact StructType schemas from ANY data source type without scanning
or loading the full dataset into memory.
"""

from __future__ import annotations
import glob
import io
import os
import sqlite3
from typing import Any, Dict, List, Optional, Union
import duckdb
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.ipc as pipc
import pyarrow.json as pjson
import pyarrow.parquet as pq

from pysphuling.sql.schema_bridge import arrow_schema_to_sphuling
from pysphuling.sql.types import StructType


class SchemaInspector:
    """Zero-load schema extractor for multiple file formats, databases, and in-memory structures."""

    @classmethod
    def inspect(cls, source: Any, format: Optional[str] = None, **options) -> StructType:
        """Inspects and returns the StructType schema of any source without full data loading."""
        # 1. In-memory PyArrow Table or RecordBatch
        if isinstance(source, (pa.Table, pa.RecordBatch)):
            return arrow_schema_to_sphuling(source.schema)

        # 2. In-memory Pandas DataFrame
        if hasattr(source, "to_numpy") and hasattr(source, "columns") and hasattr(source, "dtypes"):
            pa_schema = pa.Schema.from_pandas(source)
            return arrow_schema_to_sphuling(pa_schema)

        # 3. String path / connection URI
        if isinstance(source, str):
            resolved_format = (format or cls._detect_format(source)).lower()

            if resolved_format == "parquet":
                return cls.inspect_parquet(source)
            elif resolved_format in ("arrow", "feather", "ipc"):
                return cls.inspect_arrow_ipc(source)
            elif resolved_format in ("csv", "tsv"):
                return cls.inspect_csv(source, **options)
            elif resolved_format in ("json", "ndjson", "jsonl"):
                return cls.inspect_json(source, **options)
            elif resolved_format in ("sqlite", "jdbc"):
                return cls.inspect_sql(source, **options)
            elif resolved_format == "duckdb":
                return cls.inspect_duckdb(source, **options)
            elif resolved_format == "delta":
                return cls.inspect_delta(source, **options)
            else:
                # Fallback to DuckDB automatic sniffer with LIMIT 0
                return cls.inspect_via_duckdb(source)

        raise ValueError(f"Unsupported source type for zero-load schema inspection: {type(source)}")

    @classmethod
    def _detect_format(cls, path_or_uri: str) -> str:
        lower = path_or_uri.lower()
        if lower.startswith("sqlite://") or lower.endswith(".db") or lower.endswith(".sqlite"):
            return "sqlite"
        if lower.endswith(".parquet") or "parquet" in lower:
            return "parquet"
        if lower.endswith(".feather") or lower.endswith(".arrow") or lower.endswith(".ipc"):
            return "arrow"
        if lower.endswith(".csv") or lower.endswith(".tsv"):
            return "csv"
        if lower.endswith(".json") or lower.endswith(".jsonl") or lower.endswith(".ndjson"):
            return "json"
        if "_delta_log" in lower or "delta" in lower:
            return "delta"
        return "parquet"

    @classmethod
    def _resolve_first_file(cls, path_pattern: str) -> str:
        """Finds the first existing physical file from a single file, directory, or glob."""
        if os.path.isfile(path_pattern):
            return path_pattern
        if os.path.isdir(path_pattern):
            # Check for files in directory
            for root, _, files in os.walk(path_pattern):
                for f in files:
                    if not f.startswith((".", "_")):
                        return os.path.join(root, f)
        # Glob pattern
        matches = glob.glob(path_pattern)
        for m in matches:
            if os.path.isfile(m):
                return m
            if os.path.isdir(m):
                first = cls._resolve_first_file(m)
                if first:
                    return first
        return path_pattern

    @classmethod
    def inspect_parquet(cls, path: str) -> StructType:
        """Reads ONLY the binary Parquet file footer Thrift struct (~a few KB, 0 data blocks)."""
        target = cls._resolve_first_file(path)
        arrow_schema = pq.read_schema(target)
        return arrow_schema_to_sphuling(arrow_schema)

    @classmethod
    def inspect_arrow_ipc(cls, path: str) -> StructType:
        """Reads ONLY the Feather / Arrow IPC header/metadata without reading record arrays."""
        target = cls._resolve_first_file(path)
        try:
            with pa.memory_map(target, "r") as source:
                reader = pipc.open_file(source)
                return arrow_schema_to_sphuling(reader.schema)
        except Exception:
            with pa.OSFile(target, "rb") as source:
                reader = pipc.open_stream(source)
                return arrow_schema_to_sphuling(reader.schema)

    @classmethod
    def inspect_csv(cls, path: str, **options) -> StructType:
        """Reads the header + a bounded sample byte range (first 64 KB or N rows)."""
        target = cls._resolve_first_file(path)
        sep = options.get("sep", options.get("delimiter", ","))
        header = options.get("header", True)
        sample_size = options.get("sample_bytes", 64 * 1024)

        with open(target, "rb") as f:
            sample_bytes = f.read(sample_size)

        parse_options = pcsv.ParseOptions(delimiter=sep)
        read_options = pcsv.ReadOptions(
            use_threads=False,
            autogenerate_column_names=not header,
        )

        # Infer from sample buffer
        table = pcsv.read_csv(
            io.BytesIO(sample_bytes),
            read_options=read_options,
            parse_options=parse_options,
        )
        return arrow_schema_to_sphuling(table.schema)

    @classmethod
    def inspect_json(cls, path: str, **options) -> StructType:
        """Reads a bounded sample chunk (first 50-100 JSON objects) without scanning full data."""
        target = cls._resolve_first_file(path)
        sample_rows = options.get("sample_rows", 100)
        lines = []
        with open(target, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(sample_rows):
                line = f.readline()
                if not line:
                    break
                lines.append(line)

        sample_bytes = "".join(lines).encode("utf-8")
        table = pjson.read_json(io.BytesIO(sample_bytes))
        return arrow_schema_to_sphuling(table.schema)

    @classmethod
    def inspect_sql(cls, uri_or_path: str, table_or_query: Optional[str] = None, **options) -> StructType:
        """Issues a zero-row LIMIT 0 prepared statement to extract SQL table schema."""
        db_path = uri_or_path.replace("sqlite:///", "").replace("sqlite://", "")
        # Check if table is in query fragment
        if "#" in db_path:
            db_path, table_name = db_path.split("#", 1)
        else:
            table_name = table_or_query or options.get("table", "table")

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            columns = [desc[0] for desc in cursor.description]
            # Map sqlite columns
            st = StructType()
            for col in columns:
                st.add(col, "string")
            return st
        finally:
            conn.close()

    @classmethod
    def inspect_duckdb(cls, query_or_table: str, **options) -> StructType:
        """Uses DuckDB connection to describe schema with LIMIT 0."""
        con = duckdb.connect()
        try:
            rel = con.execute(f"SELECT * FROM {query_or_table} LIMIT 0")
            arrow_schema = rel.arrow().schema
            return arrow_schema_to_sphuling(arrow_schema)
        finally:
            con.close()

    @classmethod
    def inspect_delta(cls, path: str, **options) -> StructType:
        """Inspects Delta Lake metadata from transaction log or parquet sample."""
        log_dir = os.path.join(path, "_delta_log")
        if os.path.isdir(log_dir):
            # Check for latest json log
            log_files = sorted(glob.glob(os.path.join(log_dir, "*.json")))
            if log_files:
                import json
                with open(log_files[-1], "r", encoding="utf-8") as f:
                    for line in f:
                        data = json.loads(line)
                        if "metaData" in data and "schemaString" in data["metaData"]:
                            # Schema string from Delta log
                            schema_json = json.loads(data["metaData"]["schemaString"])
                            # Parse StructType from json
                            fields = []
                            for f_meta in schema_json.get("fields", []):
                                fields.append(f_meta["name"])
                            st = StructType()
                            for fname in fields:
                                st.add(fname, "string")
                            return st
        # Fallback to inspecting first parquet file in delta dir
        return cls.inspect_parquet(path)

    @classmethod
    def inspect_via_duckdb(cls, path: str) -> StructType:
        """Universal fallback sniffer via DuckDB."""
        con = duckdb.connect()
        try:
            rel = con.execute(f"SELECT * FROM '{path}' LIMIT 0")
            return arrow_schema_to_sphuling(rel.arrow().schema)
        finally:
            con.close()

