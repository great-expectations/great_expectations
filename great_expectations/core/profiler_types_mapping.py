class ProfilerTypeMapping:
    """Useful backend type mapping for building profilers."""

    INT_TYPE_NAMES = [
        "INTEGER",
        "integer",
        "int",
        "int_",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "Int8Dtype",
        "Int16Dtype",
        "Int32Dtype",
        "Int64Dtype",
        "UInt8Dtype",
        "UInt16Dtype",
        "UInt32Dtype",
        "UInt64Dtype",
        "INT",
        "INTEGER",
        "INT64",
        "TINYINT",
        "BYTEINT",
        "SMALLINT",
        "BIGINT",
        "IntegerType",
        "LongType",
    ]
    FLOAT_TYPE_NAMES = [
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "FLOAT64",
        "DOUBLE",
        "DOUBLE_PRECISION",
        "NUMERIC",
        "FloatType",
        "DoubleType",
        "float",
        "float_",
        "float16",
        "float32",
        "float64",
        "number",
        "DECIMAL",
        "REAL",
    ]
    STRING_TYPE_NAMES = [
        "CHAR",
        "NCHAR",
        "VARCHAR",
        "NVARCHAR",
        "TEXT",
        "NTEXT",
        "STRING",
        "StringType",
        "string",
        "str",
        "object",
        "dtype('O')",
    ]
    BOOLEAN_TYPE_NAMES = [
        "BOOLEAN",
        "boolean",
        "BOOL",
        "TINYINT",
        "BIT",
        "bool",
        "BooleanType",
    ]
    DATETIME_TYPE_NAMES = [
        "DATE",
        "TIME",
        "DATETIME",
        "DATETIME2",
        "DATETIME64",
        "SMALLDATETIME",
        "DATETIMEOFFSET",
        "TIMESTAMP",
        "Timestamp",
        "TimestampType",
        "DateType",
        "datetime64",
        "datetime64[ns]",
        "timedelta[ns]",
        "<M8[ns]",
    ]
    BINARY_TYPE_NAMES = [
        "BINARY",
        "binary",
        "VARBINARY",
        "varbinary",
        "IMAGE",
        "image",
    ]
    CURRENCY_TYPE_NAMES = [
        "MONEY",
        "money",
        "SMALLMONEY",
        "smallmoney",
    ]
    IDENTIFIER_TYPE_NAMES = [
        "UNIQUEIDENTIFIER",
        "uniqueidentifier",
    ]
    MISCELLANEOUS_TYPE_NAMES = [
        "SQL_VARIANT",
        "sql_variant",
    ]
    RECORD_TYPE_NAMES = [
        "JSON",
        "json",
    ]
