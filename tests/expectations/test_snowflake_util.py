"""
Tests for Expectation utility functions related to the snowflake.
"""

import pytest

from great_expectations.expectations.snowflake_util import (
    map_sfsqlalchemy_type_to_sf_type,
    normalize_snowflake_data_type_name,
)


@pytest.mark.snowflake
@pytest.mark.parametrize(
    "val,expected",
    [
        ("Number", "NUMBER"),
        ("number(42)", "NUMBER"),
        ("number(42, 1)", "NUMBER"),
        ("", ""),
    ],
)
def test_normalize_snowflake_data_type_name(val, expected):
    assert normalize_snowflake_data_type_name(val) == expected


@pytest.mark.snowflake
@pytest.mark.parametrize(
    "val,expected",
    [
        ("ARRAY", "ARRAY"),
        ("BIGINT", "NUMBER"),
        ("BINARY", "BINARY"),
        ("BOOLEAN", "BOOLEAN"),
        ("BYTEINT", "NUMBER"),
        ("CHAR", "TEXT"),
        ("CHARACTER", "TEXT"),
        ("DATE", "DATE"),
        ("DATETIME", "TIMESTAMP_NTZ"),
        ("DEC", "NUMBER"),
        ("DECIMAL", "NUMBER"),
        ("DOUBLE", "FLOAT"),
        ("DOUBLE PRECISION", "FLOAT"),
        ("FLOAT", "FLOAT"),
        ("FLOAT4", "FLOAT"),
        ("FLOAT8", "FLOAT"),
        ("GEOGRAPHY", "GEOGRAPHY"),
        ("GEOMETRY", "GEOMETRY"),
        ("INT", "NUMBER"),
        ("INTEGER", "NUMBER"),
        ("NUMBER", "NUMBER"),
        ("NUMERIC", "NUMBER"),
        ("OBJECT", "OBJECT"),
        ("REAL", "FLOAT"),
        ("SMALLINT", "NUMBER"),
        ("STRING", "TEXT"),
        ("TEXT", "TEXT"),
        ("TIME", "TIME"),
        ("TIMESTAMP", "TIMESTAMP_NTZ"),
        ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ"),
        ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
        ("TIMESTAMP_TZ", "TIMESTAMP_TZ"),
        ("TINYINT", "NUMBER"),
        ("VARBINARY", "BINARY"),
        ("VARCHAR", "TEXT"),
        ("VARIANT", "VARIANT"),
        ("foo", "foo"),
    ],
)
def test_map_sfsqlalchemy_type_to_sf_type(val, expected):
    assert map_sfsqlalchemy_type_to_sf_type(val) == expected
