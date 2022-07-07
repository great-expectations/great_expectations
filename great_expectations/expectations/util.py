import logging
from enum import Enum
from itertools import chain
from typing import Set

from great_expectations.exceptions import GreatExpectationsError
from great_expectations.render.types import RenderedStringTemplateContent

logger = logging.getLogger(__name__)


def render_evaluation_parameter_string(render_func):
    def inner_func(*args, **kwargs):
        rendered_string_template = render_func(*args, **kwargs)
        current_expectation_params = list()
        app_template_str = (
            "\n - $eval_param = $eval_param_value (at time of validation)."
        )
        configuration = kwargs.get("configuration", None)
        kwargs_dict = configuration.kwargs
        for key, value in kwargs_dict.items():
            if isinstance(value, dict) and "$PARAMETER" in value.keys():
                current_expectation_params.append(value["$PARAMETER"])

        # if expectation configuration has no eval params, then don't look for the values in runtime_configuration
        if len(current_expectation_params) > 0:
            runtime_configuration = kwargs.get("runtime_configuration", None)
            if runtime_configuration:
                eval_params = runtime_configuration.get("evaluation_parameters", {})
                styling = runtime_configuration.get("styling")
                for key, val in eval_params.items():
                    # this needs to be more complicated?
                    # the possibility that it is a substring?
                    for param in current_expectation_params:
                        # "key in param" condition allows for eval param values to be rendered if arithmetic is present
                        if key == param or key in param:
                            app_params = {}
                            app_params["eval_param"] = key
                            app_params["eval_param_value"] = val
                            to_append = RenderedStringTemplateContent(
                                **{
                                    "content_block_type": "string_template",
                                    "string_template": {
                                        "template": app_template_str,
                                        "params": app_params,
                                        "styling": styling,
                                    },
                                }
                            )
                            rendered_string_template.append(to_append)
            else:
                raise GreatExpectationsError(
                    f"""GE was not able to render the value of evaluation parameters.
                        Expectation {render_func} had evaluation parameters set, but they were not passed in."""
                )
        return rendered_string_template

    return inner_func


def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    """
    Utility function used in _atomic_prescriptive_template() to take list values from a given params dict key,
    convert each value to a dict with JSON schema type info, then add it to params_with_json_schema (dict).
    """
    target_list = params.get(param_key_with_list)
    if target_list is not None and len(target_list) > 0:
        for i, v in enumerate(target_list):
            params_with_json_schema[f"v__{str(i)}"] = {
                "schema": {"type": list_values_type},
                "value": v,
            }
    return params_with_json_schema


class ValidSqlTokens(Enum):
    SELECT = "SELECT"
    ASTERISK = "*"
    DISTINCT = "DISTINCT"
    INTO = "INTO"
    TOP = "TOP"
    AS = "AS"
    FROM = "FROM"
    WHERE = "WHERE"
    AND = "AND"
    OR = "OR"
    BETWEEN = "BETWEEN"
    LIKE = "LIKE"
    IN = "IN"
    IS = "IS"
    NULL = "NULL"
    NOT = "NOT"
    CREATE = "CREATE"
    DATABASE = "DATABASE"
    TABLE = "TABLE"
    INDEX = "INDEX"
    VIEW = "VIEW"
    DROP = "DROP"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    ALTER = "ALTER"
    COLUMN = "COLUMN"
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    GROUP = "GROUP"
    BY = "BY"
    HAVING = "HAVING"
    ORDER = "ORDER"
    DESC = "DESC"
    OFFSET = "OFFSET"
    FETCH = "FETCH"
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    EXISTS = "EXISTS"
    GRANT = "GRANT"
    REVOKE = "REVOKE"
    SAVEPOINT = "SAVEPOINT"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    TRUNCATE = "TRUNCATE"
    UNION = "UNION"
    ALL = "ALL"
    CAST = "CAST"


class ValidSqlAlchemyTypes(Enum):
    ARRAY = "ARRAY"
    BIGINT = "BIGINT"
    BINARY = "BINARY"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    BOOLEANTYPE = "BOOLEANTYPE"
    CHAR = "CHAR"
    CLOB = "CLOB"
    DATE = "DATE"
    DATETIME = "DATETIME"
    DATETIME_TIMEZONE = "DATETIME_TIMEZONE"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    INT = "INT"
    INTEGER = "INTEGER"
    INTEGERTYPE = "INTEGERTYPE"
    JSON = "JSON"
    MATCHTYPE = "MATCHTYPE"
    NCHAR = "NCHAR"
    NO_ARG = "NO_ARG"
    NULLTYPE = "NULLTYPE"
    NUMERIC = "NUMERIC"
    NVARCHAR = "NVARCHAR"
    REAL = "REAL"
    SMALLINT = "SMALLINT"
    STRINGTYPE = "STRINGTYPE"
    TABLEVALUE = "TABLEVALUE"
    TEXT = "TEXT"
    TIMESTAMP = "TIMESTAMP"
    TIME_TIMEZONE = "TIME_TIMEZONE"
    VARBINARY = "VARBINARY"
    VARCHAR = "VARCHAR"


class ValidSparkSqlTypes(Enum):
    ARRAYTYPE = "ARRAY"
    BINARYTYPE = "BINARY"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    TINYINT = "TINYINT"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DEC = "DEC"
    NUMERIC = "NUMERIC"
    INTERVAL = "INTERVAL"
    DAY = "DAY"
    YEAR = "YEAR"
    MONTH = "MONTH"
    HOUR = "HOUR"
    SECOND = "SECOND"
    MINUTE = "MINUTE"
    DOUBLE = "DOUBLE"
    FLOAT = "FLOAT"
    REAL = "REAL"
    INTEGER = "INTEGER"
    INT = "INT"
    LONG = "LONG"
    BIGINT = "BIGINT"
    MAP = "MAP"
    SHORT = "SHORT"
    SMALLINT = "SMALLINT"
    STRING = "STRING"
    STRUCT = "STRUCT"
    TIMESTAMP = "TIMESTAMP"


class ValidSparkSqlTokens(Enum):
    SELECT = "SELECT"
    CLUSTER = "CLUSTER"
    ALTER = "ALTER"
    DATABASE = "DATABASE"
    TABLE = "TABLE"
    VIEW = "VIEW"
    FUNCTION = "FUNCTION"
    DROP = "DROP"
    REPAIR = "REPAIR"
    TRUNCATE = "TRUNCATE"
    USE = "USE"
    INSERT = "INSERT"
    LOAD = "LOAD"
    OVERWRITE = "OVERWRITE"
    DIRECTORY = "DIRECTORY"
    BY = "BY"
    DISTRIBUTE = "DISTRIBUTE"
    HAVING = "HAVING"
    GROUP = "GROUP"
    JOIN = "JOIN"
    LIKE = "LIKE"
    ORDER = "ORDER"
    SORT = "SORT"
    TABLESAMPLE = "TABLESAMPLE"
    WHERE = "WHERE"
    CASE = "CASE"
    PIVOT = "PIVOT"
    LATERAL = "LATERAL"
    TRANSFORM = "TRANSFORM"
    EXPLAIN = "EXPLAIN"
    ADD = "ADD"
    FILE = "FILE"
    JAR = "JAR"
    CACHE = "CACHE"
    CLEAR = "CLEAR"
    ANALYZE = "ANALYZE"
    DESCRIBE = "DESCRIBE"
    QUERY = "QUERY"
    LIST = "LIST"
    REFRESH = "REFRESH"
    RESET = "RESET"
    SET = "SET"
    SHOW = "SHOW"
    COLUMNS = "COLUMNS"
    DATABASES = "DATABASES"
    FUNCTIONS = "FUNCTIONS"
    PARTITIONS = "PARTITIONS"
    EXTENDED = "EXTENDED"
    TABLES = "TABLES"
    TBLPROPERTIES = "TBLPROPERTIES"
    VIEWS = "VIEWS"
    UNCACHE = "UNCACHE"
    EXCEPT = "EXCEPT"
    MINUS = "MINUS"
    INTERSECT = "INTERSECT"
    UNION = "UNION"
    ALL = "ALL"
    FROM = "FROM"
    DISTINCT = "DISTINCT"
    WITH = "WITH"
    AS = "AS"
    MAX = "MAX"
    VALUES = "VALUES"
    GROUPING = "GROUPING"
    SETS = "SETS"
    ROLLUP = "ROLLUP"
    CUBE = "CUBE"
    MIN = "MIN"
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    FILTER = "FILTER"
    FIRST = "FIRST"
    IGNORE = "IGNORE"
    LAST = "LAST"
    NATURAL = "NATURAL"
    INNER = "INNER"
    CROSS = "CROSS"
    LEFT = "LEFT"
    OUTER = "OUTER"
    SEMI = "SEMI"
    RIGHT = "RIGHT"
    FULL = "FULL"
    ANTI = "ANTI"
    ON = "ON"
    USING = "USING"
    NOT = "NOT"
    ESCAPE = "ESCAPE"
    LIMIT = "LIMIT"
    LENGTH = "LENGTH"
    ASTERISK = "*"
    ASC = "ASC"
    DESC = "DESC"
    NULLS = "NULLS"
    ROWS = "ROWS"
    OUT = "OUT"
    OF = "OF"
    RANGE = "RANGE"
    EXPLODE = "EXPLODE"
    EXPLODE_OUTER = "EXPLODE_OUTER"
    INLINE = "INLINE"
    INLINE_OUTER = "INLINE_OUTER"
    POSEXPLODE = "POSEXPLODE"
    POSEXPLODE_OUTER = "POSEXPLODE_OUTER"
    STACK = "STACK"
    JSON_TUPE = "JSON_TUPE"
    PARSE_URL = "PARSE_URL"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    PERCENT_RANK = "PERCENT_RANK"
    NTILE = "NTILE"
    ROW_NUMBER = "ROW_NUMBER"
    RESPECT = "RESPECT"
    CUME_DIST = "CUME_DIST"
    LAG = "LAG"
    LEAD = "LEAD"
    NTH_VALUE = "NTH_VALUE"
    FIRST_VALUE = "FIRST_VALUE"
    LAST_VALUE = "LAST_VALUE"
    PRECEDING = "PRECEDING"
    BETWEEN = "BETWEEN"
    AND = "AND"
    CURRENT = "CURRENT"
    FOLLOWING = "FOLLOWING"
    UNBOUNDED = "UNBOUNDED"
    WHEN = "WHEN"
    THEN = "THEN"
    ELSE = "ELSE"
    FOR = "FOR"


valid_tokens_and_types: Set[str] = set(
    chain.from_iterable(
        [
            list(map(lambda i: i.lower(), ValidSqlTokens.__members__.keys())),
            list(map(lambda i: i.lower(), ValidSqlAlchemyTypes.__members__.keys())),
            list(map(lambda i: i.lower(), ValidSparkSqlTokens.__members__.keys())),
            list(map(lambda i: i.lower(), ValidSparkSqlTypes.__members__.keys())),
        ]
    )
)
