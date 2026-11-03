import pytest
from pysphuling.conf import SphulingConf, SparkConf
from pysphuling.core.resources import parse_cores, parse_memory_to_bytes


def test_sphuling_conf_basic():
    conf = SphulingConf()
    conf.set("sphuling.app.name", "TestApp")
    assert conf.get("sphuling.app.name") == "TestApp"


def test_legacy_spark_key_normalization():
    conf = SphulingConf()
    conf.set("spark.executor.cores", "8")
    conf.set("spark.executor.memory", "16g")
    
    # Can query via sphuling.* or spark.*
    assert conf.get("sphuling.executor.cores") == "8"
    assert conf.get("spark.executor.cores") == "8"
    assert conf.get("sphuling.executor.memory") == "16g"
    assert conf.get("spark.executor.memory") == "16g"


def test_spark_conf_alias():
    conf = SparkConf()
    conf.setAppName("AliasApp")
    conf.setMaster("local[4]")
    assert conf.get("sphuling.app.name") == "AliasApp"
    assert conf.get("sphuling.master") == "local[4]"


def test_resource_parsing():
    assert parse_cores("local") == 1
    assert parse_cores("local[8]") == 8
    assert parse_cores("local[*]") >= 1
    assert parse_cores("local", configured_cores="16") == 16

    assert parse_memory_to_bytes("512m") == 512 * 1024 * 1024
    assert parse_memory_to_bytes("4g") == 4 * 1024 * 1024 * 1024
    assert parse_memory_to_bytes("16gb") == 16 * 1024 * 1024 * 1024
    assert parse_memory_to_bytes("1073741824") == 1073741824

