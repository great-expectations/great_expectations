from __future__ import annotations

import enum
import operator
from dataclasses import dataclass
from string import punctuation
from typing import TYPE_CHECKING

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

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark, sqlalchemy


def _set_notnull(s, l, t) -> None:  # noqa: E741 # ambiguous name `l`
    t["notnull"] = True


WHITESPACE_CHARS = " \t"
column_name = Combine(
    Suppress(Literal('col("'))
    + Word(alphas, f"{alphanums}_-.").setResultsName("column")
    + Suppress(Literal('")'))
)
gt = Literal(">")
lt = Literal("<")
ge = Literal(">=")
le = Literal("<=")
eq = Literal("==")
ne = Literal("!=")
ops = (gt ^ lt ^ ge ^ le ^ eq ^ ne).setResultsName("op")
fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?").setResultsName("fnumber")
punctuation_without_apostrophe = punctuation.replace('"', "").replace("'", "")
condition_value_chars = alphanums + punctuation_without_apostrophe + WHITESPACE_CHARS
condition_value = Suppress('"') + Word(f"{condition_value_chars}._").setResultsName(
    "condition_value"
) + Suppress('"') ^ Suppress("'") + Word(f"{condition_value_chars}._").setResultsName(
    "condition_value"
) + Suppress(
    "'"
)
date = (
    Literal("date").setResultsName("date")
    + Suppress(Literal("("))
    + condition_value
    + Suppress(Literal(")"))
)
not_null = CaselessLiteral(".notnull()").setResultsName("notnull")
condition = (column_name + not_null).setParseAction(_set_notnull) ^ (
    column_name + ops + (fnumber ^ condition_value ^ date)
)


class ConditionParserError(gx_exceptions.GreatExpectationsError):
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
def parse_condition_to_spark(  # noqa: PLR0911, PLR0912
    row_condition: str,
) -> pyspark.Column:
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


def generate_condition_by_operator(column, op, value):
    operators = {
        "==": operator.eq,
        "<": operator.lt,
        ">": operator.gt,
        ">=": operator.ge,
        "<=": operator.le,
        "!=": operator.ne,
    }
    return operators[op](column, value)


def parse_condition_to_sqlalchemy(
    row_condition: str,
) -> sqlalchemy.ColumnElement:
    parsed = _parse_great_expectations_condition(row_condition)
    column = parsed["column"]
    if "date" in parsed:
        date_value: str = parsed["condition_value"]
        cast_as_date = f"date({date_value})"
        return generate_condition_by_operator(
            sa.column(column), parsed["op"], cast_as_date
        )

    elif "condition_value" in parsed:
        return generate_condition_by_operator(
            sa.column(column), parsed["op"], parsed["condition_value"]
        )
    elif "fnumber" in parsed:
        number_value = parsed["fnumber"]
        num = int(number_value) if number_value.isdigit() else float(number_value)
        return generate_condition_by_operator(sa.column(column), parsed["op"], num)

    elif "notnull" in parsed and parsed["notnull"] is True:
        return sa.not_(sa.column(column).is_(None))
    else:
        raise ConditionParserError(f"unrecognized column condition: {row_condition}")
