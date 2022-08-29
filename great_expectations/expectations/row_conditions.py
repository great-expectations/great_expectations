import enum
from dataclasses import dataclass

from pyparsing import (
    CaselessLiteral,
    Combine,
    Literal,
    ParseException,
    Regex,
    Suppress,
    Word,
    alphanums,
    alphas,
)

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot

try:
    import pyspark.sql.functions as F
except ImportError:
    F = None

try:
    import sqlalchemy as sa
except ImportError:
    sa = None


def _set_notnull(s, l, t) -> None:
    t["notnull"] = True


column_name = Combine(
    Suppress(Literal('col("'))
    + Word(alphas, f"{alphanums}_.").setResultsName("column")
    + Suppress(Literal('")'))
)
gt = Literal(">")
lt = Literal("<")
ge = Literal(">=")
le = Literal("<=")
eq = Literal("==")
ops = (gt ^ lt ^ ge ^ le ^ eq).setResultsName("op")
fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?").setResultsName("fnumber")
condition_value = Suppress('"') + Word(f"{alphanums}._").setResultsName(
    "condition_value"
) + Suppress('"') ^ Suppress("'") + Word(f"{alphanums}._").setResultsName(
    "condition_value"
) + Suppress(
    "'"
)
not_null = CaselessLiteral(".notnull()").setResultsName("notnull")
condition = (column_name + not_null).setParseAction(_set_notnull) ^ (
    column_name + ops + (fnumber ^ condition_value)
)


class ConditionParserError(ge_exceptions.GreatExpectationsError):
    pass


class RowConditionParserType(enum.Enum):
    """Type of condition or parser to be used to interpret a RowCondition

    Note that many of these are forward looking and are not yet implemented.
    In the future `GE` can replace the `great_expectations__experimental__`
    name for the condition_parser and this enum can be used internally
    instead of strings for the condition_parser user input.
    """

    GE = "ge"  # GE intermediate language
    SPARK = "spark"  # Spark pyspark.sql.Column type
    SPARK_SQL = "spark_sql"  # String type
    PANDAS = "pandas"  # pandas parser for pandas DataFrame.query()
    PYTHON = "python"  # python parser for DataFrame.query()
    SQL = "sql"  # Selectable type


@dataclass
class RowCondition(SerializableDictDot):
    """Condition that can be used to filter rows in a data set.

    Attributes:
        condition: String of the condition
        condition_type: Format of the condition e.g. for parsing
    """

    condition: str
    condition_type: RowConditionParserType

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return {
            "condition": self.condition,
            "condition_type": self.condition_type.value,
        }

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())


def _parse_great_expectations_condition(row_condition: str):
    try:
        return condition.parseString(row_condition)
    except ParseException:
        raise ConditionParserError(f"unable to parse condition: {row_condition}")


# noinspection PyUnresolvedReferences
def parse_condition_to_spark(row_condition: str) -> "pyspark.sql.Column":
    parsed = _parse_great_expectations_condition(row_condition)
    column = parsed["column"]
    if "condition_value" in parsed:
        if parsed["op"] == "==":
            return F.col(column) == parsed["condition_value"]
        else:
            raise ConditionParserError(
                f"Invalid operator: {parsed['op']} for string literal spark condition."
            )
    elif "fnumber" in parsed:
        try:
            num = int(parsed["fnumber"])
        except ValueError:
            num = float(parsed["fnumber"])
        op = parsed["op"]
        if op == ">":
            return F.col(column) > num
        elif op == "<":
            return F.col(column) < num
        elif op == ">=":
            return F.col(column) >= num
        elif op == "<=":
            return F.col(column) <= num
        elif op == "==":
            return F.col(column) == num
    elif "notnull" in parsed and parsed["notnull"] is True:
        return F.col(column).isNotNull()
    else:
        raise ConditionParserError(f"unrecognized column condition: {row_condition}")


def parse_condition_to_sqlalchemy(
    row_condition: str,
) -> "sqlalchemy.sql.expression.ColumnElement":
    parsed = _parse_great_expectations_condition(row_condition)
    column = parsed["column"]
    if "condition_value" in parsed:
        if parsed["op"] == "==":
            return sa.column(column) == parsed["condition_value"]
        else:
            raise ConditionParserError(
                f"Invalid operator: {parsed['op']} for string literal spark condition."
            )
    elif "fnumber" in parsed:
        try:
            num = int(parsed["fnumber"])
        except ValueError:
            num = float(parsed["fnumber"])
        op = parsed["op"]
        if op == ">":
            return sa.column(column) > num
        elif op == "<":
            return sa.column(column) < num
        elif op == ">=":
            return sa.column(column) >= num
        elif op == "<=":
            return sa.column(column) <= num
        elif op == "==":
            return sa.column(column) == num
    elif "notnull" in parsed and parsed["notnull"] is True:
        return sa.not_(sa.column(column).is_(None))
    else:
        raise ConditionParserError(f"unrecognized column condition: {row_condition}")
