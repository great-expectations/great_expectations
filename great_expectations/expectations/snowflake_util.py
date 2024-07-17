"""
Utilities to account for Expectation behavior that is specific to Snowflake.
"""
from typing import Dict, Literal, Union

SNOWFLAKE_SCHEMA_COLUMN_TYPE_NAMES = Union[
    # all possible values of `select data_type from information_schema.columns;`
    Literal[
        "ARRAY",
        "BINARY",
        "BOOLEAN",
        "DATE",
        "FLOAT",
        "GEOGRAPHY",
        "GEOMETRY",
        "NUMBER",
        "OBJECT",
        "TEXT",
        "TIME",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
        "VARIANT",
    ]
]


def map_sfsqlalchemy_type_to_sf_type(
    sfsqla_type: str,
) -> Union[SNOWFLAKE_SCHEMA_COLUMN_TYPE_NAMES, str]:
    """
    Args:
        sfsqla_type: A normalized snowflake-sqlalchemy type name.

    Returns:
        If recognized, the name of the type as it would appear in `select data_type from information_schema.columns;`.
        If not recognized, the original input value.
    """

    sfsqlalchemy_type_to_sf_type: Dict[str, SNOWFLAKE_SCHEMA_COLUMN_TYPE_NAMES] = {
        "ARRAY": "ARRAY",
        "BIGINT": "NUMBER",
        "BINARY": "BINARY",
        "BOOLEAN": "BOOLEAN",
        "BYTEINT": "NUMBER",
        "CHAR": "TEXT",
        "CHARACTER": "TEXT",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP_NTZ",
        "DEC": "NUMBER",
        "DECIMAL": "NUMBER",
        "DOUBLE": "FLOAT",
        "DOUBLE PRECISION": "FLOAT",
        "FLOAT": "FLOAT",
        "FLOAT4": "FLOAT",
        "FLOAT8": "FLOAT",
        "GEOGRAPHY": "GEOGRAPHY",
        "GEOMETRY": "GEOMETRY",
        "INT": "NUMBER",
        "INTEGER": "NUMBER",
        "NUMBER": "NUMBER",
        "NUMERIC": "NUMBER",
        "OBJECT": "OBJECT",
        "REAL": "FLOAT",
        "SMALLINT": "NUMBER",
        "STRING": "TEXT",
        "TEXT": "TEXT",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ": "TIMESTAMP_TZ",
        "TINYINT": "NUMBER",
        "VARBINARY": "BINARY",
        "VARCHAR": "TEXT",
        "VARIANT": "VARIANT",
    }

    return sfsqlalchemy_type_to_sf_type.get(sfsqla_type, sfsqla_type)


def normalize_snowflake_data_type_name(name) -> str:
    """
    Pure function to transform a snowflake data type name to a normalized form.
    Example: number(42) -> NUMBER
    """
    # normalize as case insensitive, ignored precision
    return str(name).split("(")[0].upper()
