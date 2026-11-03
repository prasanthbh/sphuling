"""Sphuling / PySphuling: High-performance local PySpark-compatible analytics engine."""

import sys
from pysphuling.conf.sphuling_conf import SphulingConf, SparkConf
from pysphuling.sql.session import SphulingSession, SparkSession
from pysphuling.sql.dataframe import DataFrame, Row
from pysphuling.sql.column import Column
import pysphuling.sql as sql
import pysphuling.conf as conf

__version__ = "0.1.0"

# Register seamless drop-in aliases in sys.modules for 'pyspark'
if "pyspark" not in sys.modules:
    sys.modules["pyspark"] = sys.modules[__name__]
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.conf"] = conf
    sys.modules["pyspark.sql.functions"] = sql.functions
    sys.modules["pyspark.sql.types"] = sql.types
    sys.modules["pyspark.sql.window"] = sql.window

__all__ = [
    "SphulingSession",
    "SparkSession",
    "SphulingConf",
    "SparkConf",
    "DataFrame",
    "Row",
    "Column",
    "sql",
    "conf",
    "__version__",
]
