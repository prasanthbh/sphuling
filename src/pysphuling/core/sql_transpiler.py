"""Spark SQL dialect transpiler for PySphuling."""

from __future__ import annotations
import sqlglot


def transpile_spark_sql(sql_query: str) -> str:
    """Translates a Spark SQL dialect query into ANSI-standard SQL supported by DataFusion."""
    try:
        transpiled = sqlglot.transpile(sql_query, read="spark", write="postgres")
        if transpiled:
            return transpiled[0]
        return sql_query
    except Exception:
        # Fallback to original SQL if sqlglot dialect parse fails
        return sql_query

