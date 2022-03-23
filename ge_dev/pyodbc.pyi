from __future__ import annotations
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Sequence, Tuple, Union


# SQLSetConnectAttr attributes
# ref: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function
SQL_ATTR_ACCESS_MODE: int
SQL_ATTR_AUTOCOMMIT: int
SQL_ATTR_CURRENT_CATALOG: int
SQL_ATTR_LOGIN_TIMEOUT: int
SQL_ATTR_ODBC_CURSORS: int
SQL_ATTR_QUIET_MODE: int
SQL_ATTR_TRACE: int
SQL_ATTR_TRACEFILE: int
SQL_ATTR_TRANSLATE_LIB: int
SQL_ATTR_TRANSLATE_OPTION: int
SQL_ATTR_TXN_ISOLATION: int
# other (e.g. specific to certain RDBMSs)
SQL_ACCESS_MODE: int
SQL_AUTOCOMMIT: int
SQL_CURRENT_QUALIFIER: int
SQL_LOGIN_TIMEOUT: int
SQL_ODBC_CURSORS: int
SQL_OPT_TRACE: int
SQL_OPT_TRACEFILE: int
SQL_PACKET_SIZE: int
SQL_QUIET_MODE: int
SQL_TRANSLATE_DLL: int
SQL_TRANSLATE_OPTION: int
SQL_TXN_ISOLATION: int
# Unicode
SQL_ATTR_ANSI_APP: int

# ODBC column data types
# https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-d-data-types
SQL_UNKNOWN_TYPE: int
SQL_CHAR: int
SQL_VARCHAR: int
SQL_LONGVARCHAR: int
SQL_WCHAR: int
SQL_WVARCHAR: int
SQL_WLONGVARCHAR: int
SQL_DECIMAL: int
SQL_NUMERIC: int
SQL_SMALLINT: int
SQL_INTEGER: int
SQL_REAL: int
SQL_FLOAT: int
SQL_DOUBLE: int
SQL_BIT: int
SQL_TINYINT: int
SQL_BIGINT: int
SQL_BINARY: int
SQL_VARBINARY: int
SQL_LONGVARBINARY: int
SQL_TYPE_DATE: int
SQL_TYPE_TIME: int
SQL_TYPE_TIMESTAMP: int
SQL_SS_TIME2: int
SQL_SS_XML: int
SQL_INTERVAL_MONTH: int
SQL_INTERVAL_YEAR: int
SQL_INTERVAL_YEAR_TO_MONTH: int
SQL_INTERVAL_DAY: int
SQL_INTERVAL_HOUR: int
SQL_INTERVAL_MINUTE: int
SQL_INTERVAL_SECOND: int
SQL_INTERVAL_DAY_TO_HOUR: int
SQL_INTERVAL_DAY_TO_MINUTE: int
SQL_INTERVAL_DAY_TO_SECOND: int
SQL_INTERVAL_HOUR_TO_MINUTE: int
SQL_INTERVAL_HOUR_TO_SECOND: int
SQL_INTERVAL_MINUTE_TO_SECOND: int
SQL_GUID: int
# SQLDescribeCol 
SQL_NO_NULLS: int
SQL_NULLABLE: int
SQL_NULLABLE_UNKNOWN: int
# specific to pyodbc
SQL_WMETADATA: int

# SQL_CONVERT_X
SQL_CONVERT_FUNCTIONS: int
SQL_CONVERT_BIGINT: int
SQL_CONVERT_BINARY: int
SQL_CONVERT_BIT: int
SQL_CONVERT_CHAR: int
SQL_CONVERT_DATE: int
SQL_CONVERT_DECIMAL: int
SQL_CONVERT_DOUBLE: int
SQL_CONVERT_FLOAT: int
SQL_CONVERT_GUID: int
SQL_CONVERT_INTEGER: int
SQL_CONVERT_INTERVAL_DAY_TIME: int
SQL_CONVERT_INTERVAL_YEAR_MONTH: int
SQL_CONVERT_LONGVARBINARY: int
SQL_CONVERT_LONGVARCHAR: int
SQL_CONVERT_NUMERIC: int
SQL_CONVERT_REAL: int
SQL_CONVERT_SMALLINT: int
SQL_CONVERT_TIME: int
SQL_CONVERT_TIMESTAMP: int
SQL_CONVERT_TINYINT: int
SQL_CONVERT_VARBINARY: int
SQL_CONVERT_VARCHAR: int
SQL_CONVERT_WCHAR: int
SQL_CONVERT_WLONGVARCHAR: int
SQL_CONVERT_WVARCHAR: int

# transaction isolation
# ref: https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-cursors/properties/cursor-transaction-isolation-level
SQL_TXN_READ_COMMITTED: int
SQL_TXN_READ_UNCOMMITTED: int
SQL_TXN_REPEATABLE_READ: int
SQL_TXN_SERIALIZABLE: int

# outer join capabilities
SQL_OJ_LEFT: int
SQL_OJ_RIGHT: int
SQL_OJ_FULL: int
SQL_OJ_NESTED: int
SQL_OJ_NOT_ORDERED: int
SQL_OJ_INNER: int
SQL_OJ_ALL_COMPARISON_OPS: int

# other ODBC database constants
SQL_SCOPE_CURROW: int
SQL_SCOPE_TRANSACTION: int
SQL_SCOPE_SESSION: int
SQL_PC_UNKNOWN: int
SQL_PC_NOT_PSEUDO: int
SQL_PC_PSEUDO: int
# SQL_INDEX_BTREE: int
# SQL_INDEX_CLUSTERED: int
# SQL_INDEX_CONTENT: int
# SQL_INDEX_HASHED: int
# SQL_INDEX_OTHER: int

# attributes for the ODBC SQLGetInfo function
# https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function
SQL_ACCESSIBLE_PROCEDURES: int
SQL_ACCESSIBLE_TABLES: int
SQL_ACTIVE_ENVIRONMENTS: int
SQL_AGGREGATE_FUNCTIONS: int
SQL_ALTER_DOMAIN: int
SQL_ALTER_TABLE: int
SQL_ASYNC_MODE: int
SQL_BATCH_ROW_COUNT: int
SQL_BATCH_SUPPORT: int
SQL_BOOKMARK_PERSISTENCE: int
SQL_CATALOG_LOCATION: int
SQL_CATALOG_NAME: int
SQL_CATALOG_NAME_SEPARATOR: int
SQL_CATALOG_TERM: int
SQL_CATALOG_USAGE: int
SQL_COLLATION_SEQ: int
SQL_COLUMN_ALIAS: int
SQL_CONCAT_NULL_BEHAVIOR: int
SQL_CONVERT_VARCHAR: int
SQL_CORRELATION_NAME: int
SQL_CREATE_ASSERTION: int
SQL_CREATE_CHARACTER_SET: int
SQL_CREATE_COLLATION: int
SQL_CREATE_DOMAIN: int
SQL_CREATE_SCHEMA: int
SQL_CREATE_TABLE: int
SQL_CREATE_TRANSLATION: int
SQL_CREATE_VIEW: int
SQL_CURSOR_COMMIT_BEHAVIOR: int
SQL_CURSOR_ROLLBACK_BEHAVIOR: int
# SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY: int
SQL_DATABASE_NAME: int
SQL_DATA_SOURCE_NAME: int
SQL_DATA_SOURCE_READ_ONLY: int
SQL_DATETIME_LITERALS: int
SQL_DBMS_NAME: int
SQL_DBMS_VER: int
SQL_DDL_INDEX: int
SQL_DEFAULT_TXN_ISOLATION: int
SQL_DESCRIBE_PARAMETER: int
SQL_DM_VER: int
SQL_DRIVER_HDESC: int
SQL_DRIVER_HENV: int
SQL_DRIVER_HLIB: int
SQL_DRIVER_HSTMT: int
SQL_DRIVER_NAME: int
SQL_DRIVER_ODBC_VER: int
SQL_DRIVER_VER: int
SQL_DROP_ASSERTION: int
SQL_DROP_CHARACTER_SET: int
SQL_DROP_COLLATION: int
SQL_DROP_DOMAIN: int
SQL_DROP_SCHEMA: int
SQL_DROP_TABLE: int
SQL_DROP_TRANSLATION: int
SQL_DROP_VIEW: int
SQL_DYNAMIC_CURSOR_ATTRIBUTES1: int
SQL_DYNAMIC_CURSOR_ATTRIBUTES2: int
SQL_EXPRESSIONS_IN_ORDERBY: int
SQL_FILE_USAGE: int
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: int
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: int
SQL_GETDATA_EXTENSIONS: int
SQL_GROUP_BY: int
SQL_IDENTIFIER_CASE: int
SQL_IDENTIFIER_QUOTE_CHAR: int
SQL_INDEX_KEYWORDS: int
SQL_INFO_SCHEMA_VIEWS: int
SQL_INSERT_STATEMENT: int
SQL_INTEGRITY: int
SQL_KEYSET_CURSOR_ATTRIBUTES1: int
SQL_KEYSET_CURSOR_ATTRIBUTES2: int
SQL_KEYWORDS: int
SQL_LIKE_ESCAPE_CLAUSE: int
SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: int
SQL_MAX_BINARY_LITERAL_LEN: int
SQL_MAX_CATALOG_NAME_LEN: int
SQL_MAX_CHAR_LITERAL_LEN: int
SQL_MAX_COLUMNS_IN_GROUP_BY: int
SQL_MAX_COLUMNS_IN_INDEX: int
SQL_MAX_COLUMNS_IN_ORDER_BY: int
SQL_MAX_COLUMNS_IN_SELECT: int
SQL_MAX_COLUMNS_IN_TABLE: int
SQL_MAX_COLUMN_NAME_LEN: int
SQL_MAX_CONCURRENT_ACTIVITIES: int
SQL_MAX_CURSOR_NAME_LEN: int
SQL_MAX_DRIVER_CONNECTIONS: int
SQL_MAX_IDENTIFIER_LEN: int
SQL_MAX_INDEX_SIZE: int
SQL_MAX_PROCEDURE_NAME_LEN: int
SQL_MAX_ROW_SIZE: int
SQL_MAX_ROW_SIZE_INCLUDES_LONG: int
SQL_MAX_SCHEMA_NAME_LEN: int
SQL_MAX_STATEMENT_LEN: int
SQL_MAX_TABLES_IN_SELECT: int
SQL_MAX_TABLE_NAME_LEN: int
SQL_MAX_USER_NAME_LEN: int
SQL_MULTIPLE_ACTIVE_TXN: int
SQL_MULT_RESULT_SETS: int
SQL_NEED_LONG_DATA_LEN: int
SQL_NON_NULLABLE_COLUMNS: int
SQL_NULL_COLLATION: int
SQL_NUMERIC_FUNCTIONS: int
SQL_ODBC_INTERFACE_CONFORMANCE: int
SQL_ODBC_VER: int
SQL_OJ_CAPABILITIES: int
SQL_ORDER_BY_COLUMNS_IN_SELECT: int
SQL_PARAM_ARRAY_ROW_COUNTS: int
SQL_PARAM_ARRAY_SELECTS: int
SQL_PARAM_TYPE_UNKNOWN: int
SQL_PARAM_INPUT: int
SQL_PARAM_INPUT_OUTPUT: int
SQL_PARAM_OUTPUT: int
SQL_RETURN_VALUE: int
SQL_RESULT_COL: int
SQL_PROCEDURES: int
SQL_PROCEDURE_TERM: int
SQL_QUOTED_IDENTIFIER_CASE: int
SQL_ROW_UPDATES: int
SQL_SCHEMA_TERM: int
SQL_SCHEMA_USAGE: int
SQL_SCROLL_OPTIONS: int
SQL_SEARCH_PATTERN_ESCAPE: int
SQL_SERVER_NAME: int
SQL_SPECIAL_CHARACTERS: int
SQL_SQL92_DATETIME_FUNCTIONS: int
SQL_SQL92_FOREIGN_KEY_DELETE_RULE: int
SQL_SQL92_FOREIGN_KEY_UPDATE_RULE: int
SQL_SQL92_GRANT: int
SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: int
SQL_SQL92_PREDICATES: int
SQL_SQL92_RELATIONAL_JOIN_OPERATORS: int
SQL_SQL92_REVOKE: int
SQL_SQL92_ROW_VALUE_CONSTRUCTOR: int
SQL_SQL92_STRING_FUNCTIONS: int
SQL_SQL92_VALUE_EXPRESSIONS: int
SQL_SQL_CONFORMANCE: int
SQL_STANDARD_CLI_CONFORMANCE: int
SQL_STATIC_CURSOR_ATTRIBUTES1: int
SQL_STATIC_CURSOR_ATTRIBUTES2: int
SQL_STRING_FUNCTIONS: int
SQL_SUBQUERIES: int
SQL_SYSTEM_FUNCTIONS: int
SQL_TABLE_TERM: int
SQL_TIMEDATE_ADD_INTERVALS: int
SQL_TIMEDATE_DIFF_INTERVALS: int
SQL_TIMEDATE_FUNCTIONS: int
SQL_TXN_CAPABLE: int
SQL_TXN_ISOLATION_OPTION: int
SQL_UNION: int
SQL_USER_NAME: int
SQL_XOPEN_CLI_YEAR: int


# pyodbc-specific constants
BinaryNull: Any  # to distinguish binary NULL values from char NULL values

# module attributes
# https://www.python.org/dev/peps/pep-0249/#globals
# read-only
version: str  # not pep-0249
apilevel: str
threadsafety: int
paramstyle: str
# read-write
pooling: bool
lowercase: bool
native_uuid: bool


# exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions
class Warning(Exception): ...
class Error(Exception): ...
class DatabaseError(Error): ...
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...


# an ODBC connection to the database, for managing database transactions and creating cursors
# https://www.python.org/dev/peps/pep-0249/#connection-objects
class Connection:

    # read-write attributes
    autocommit: bool
    maxwrite: int
    timeout: int

    # read-only attributes
    searchescape: str

    # implemented dunder methods
    def __enter__(self) -> Connection: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...

    # define text encoding for data, metadata, etc.
    def setencoding(self, encoding: str, ctype: Optional[int]) -> None: ...
    def setdecoding(self, encoding: str, ctype: Optional[int]) -> None: ...

    # connection attributes
    def getinfo(self, infotype: int, /) -> Any: ...
    def set_attr(self, attr_id: int, value: int, /) -> None: ...

    # handle non-standard database data types
    def add_output_converter(self, sqltype: int, new_converter: Callable, /) -> None: ...
    def get_output_converter(self, sqltype: int, /) -> Optional[Callable]: ...
    def remove_output_converter(self, sqltype: int, /) -> None: ...
    def clear_output_converters(self) -> None: ...

    # query functions (in rough order of use)
    def cursor(self) -> Cursor: ...
    def execute(self, sql: str, *params) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


# cursors are vehicles for executing SQL statements and returning their results
# https://www.python.org/dev/peps/pep-0249/#cursor-objects
class Cursor:

    # read-write attributes
    arraysize: int
    fast_executemany: bool
    noscan: bool

    # read-only attributes
    description: Tuple[Tuple[str, Any, int, int, int, int, bool]]
    messages: Optional[List[Tuple[str, Union[str, bytes]]]]
    rowcount: int
    connection: Connection

    # implemented dunder methods
    def __enter__(self) -> Cursor: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def __iter__(self, /) -> Cursor: ...
    def __next__(self, /) -> Row: ...

    # query functions (in rough order of use)
    def setinputsizes(self, sizes: List[Tuple[int, int, int]], /) -> None: ...
    def setoutputsize(self) -> None: ...
    def execute(self, sql: str, *params) -> Cursor: ...
    def executemany(self, sql: str, params: Union[Sequence, Iterator, Generator], /) -> None: ...
    def fetchone(self) -> Row: ...
    def fetchmany(self, size: int, /) -> List[Row]: ...
    def fetchall(self) -> List[Row]: ...
    def fetchval(self) -> Any: ...
    def skip(self, count: int, /) -> None: ...
    def nextset(self) -> bool: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...

    # metadata functions
    def tables(self) -> Cursor: ...
    def columns(self) -> Cursor: ...
    def statistics(self) -> Cursor: ...
    def rowIdColumns(self) -> Cursor: ...
    def rowVerColumns(self) -> Cursor: ...
    def primaryKeys(self) -> Cursor: ...
    def foreignKeys(self) -> Cursor: ...
    def getTypeInfo(self) -> Cursor: ...
    def procedures(self) -> Cursor: ...
    def procedureColumns(self) -> Cursor: ...


# a Row object represents a single database record, and behaves somewhat similar to a NamedTuple
class Row:
    cursor_description: Tuple[Tuple[str, Any, int, int, int, int, bool]]

    # implemented dunder methods
    def __contains__(self, key, /) -> int: ...
    def __delattr__(self, name, /) -> None: ...
    def __delitem__(self, key, /) -> None: ...
    def __eq__(self, value, /) -> bool: ...
    def __ge__(self, value, /) -> bool: ...
    def __getattribute__(self, name, /) -> Any: ...
    def __getitem__(self, key, /) -> Any: ...
    def __gt__(self, value, /) -> bool: ...
    def __le__(self, value, /) -> bool: ...
    def __len__(self, /) -> int: ...
    def __lt__(self, value, /) -> bool: ...
    def __ne__(self, value, /) -> bool: ...
    def __reduce__(self) -> Any: ...
    def __repr__(self, /) -> str: ...
    def __setattr__(self, name, value, /) -> None: ...
    def __setitem__(self, key, value, /) -> None: ...


# module functions

def dataSources() -> Dict[str, str]: ...
def drivers() -> List[str]: ...

def setDecimalSeparator(sep: str, /) -> None: ...
def getDecimalSeparator() -> str: ...

# https://www.python.org/dev/peps/pep-0249/#connect
def connect(connstring: str,
            /, *,  # only positional parameters before, only named parameters after
            autocommit: bool = False,
            encoding: str = 'utf-16le',
            ansi: bool = False,
            readonly: bool = False,
            timeout: int = 0,
            attrs_before: dict = {},
            **kwargs) -> Connection: ...
