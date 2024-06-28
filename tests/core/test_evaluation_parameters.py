import math
from datetime import datetime, timedelta
from timeit import timeit
from typing import Any

import dateutil
import pytest

from great_expectations.core.suite_parameters import (
    _deduplicate_suite_parameter_dependencies,
    get_suite_parameter_key,
    is_suite_parameter,
    parse_suite_parameter,
)
from great_expectations.exceptions import SuiteParameterError


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, False),
        (1, False),
        ("foo", False),
        ({}, False),
        ({"PARAMETER": 123}, False),
        ({"$PARAMETER": 123}, True),
        ({"$PARAMETER": 123, "another_field": 234}, True),
    ],
)
@pytest.mark.unit
def test_is_suite_parameter(value: Any, expected: bool):
    assert is_suite_parameter(value) == expected


@pytest.mark.unit
def test_get_suite_parameter_key():
    key = "foo"
    assert get_suite_parameter_key({"$PARAMETER": key}) == key


@pytest.mark.unit
def test_parse_suite_parameter():
    # Substitution alone is ok
    assert parse_suite_parameter("a", {"a": 1}) == 1
    assert (
        parse_suite_parameter(
            "my_value",
            {"my_value": 1},
        )
        == 1
    )

    # Very basic arithmetic is allowed as-is:
    assert parse_suite_parameter("1 + 1", {}) == 2

    # So is simple variable substitution:
    assert parse_suite_parameter("a + 1", {"a": 2}) == 3

    # bare decimal is accepted
    assert parse_suite_parameter("my_value * .9", {"my_value": 10}) == 9

    assert parse_suite_parameter("trunc(my_value * 0.9)", {"my_value": 11}) == 9

    # Valid variables but invalid expression is no good
    with pytest.raises(SuiteParameterError) as err:
        parse_suite_parameter("1 / a", {"a": 0})
    assert "Error while evaluating suite parameter expression: division by zero" in str(err.value)

    # It is okay to *substitute* strings in the expression...
    assert parse_suite_parameter("foo", {"foo": "bar"}) == "bar"

    # ...and to have whitespace in substituted values...
    assert parse_suite_parameter("foo", {"foo": "bar "}) == "bar "

    # ...but whitespace is *not* preserved from the parameter name if we evaluate it
    assert parse_suite_parameter("foo ", {"foo": "bar"}) == "bar"  # NOT "bar "

    # We can use multiple parameters...
    assert parse_suite_parameter("foo * bar", {"foo": 2, "bar": 3}) == 6

    # ...but we cannot leave *partially* evaluated expressions (phew!)
    with pytest.raises(SuiteParameterError) as e:
        parse_suite_parameter("foo + bar", {"foo": 2})
    assert "Error while evaluating suite parameter expression: Unknown string format: bar" in str(
        e.value
    )


@pytest.mark.unit
def test_parser_timing():
    """We currently reuse the parser, clearing the stack between calls, which is about 10 times faster than not
    doing so. But these operations are really quick, so this may not be necessary."""  # noqa: E501
    assert (
        timeit(
            "parse_suite_parameter('x', {'x': 1})",
            setup="from great_expectations.core.suite_parameters import parse_suite_parameter",
            number=100,
        )
        < 1
    )


@pytest.mark.unit
def test_math_suite_paramaters():
    assert parse_suite_parameter("sin(2*PI)") == math.sin(math.pi * 2)
    assert parse_suite_parameter("cos(2*PI)") == math.cos(math.pi * 2)
    assert parse_suite_parameter("tan(2*PI)") == math.tan(math.pi * 2)


@pytest.mark.unit
def test_temporal_suite_parameters():
    # allow 1 second for "now" tolerance
    now = datetime.now()  # noqa: DTZ005
    assert (
        (now - timedelta(weeks=1, seconds=3))
        < dateutil.parser.parse(parse_suite_parameter("now() - timedelta(weeks=1, seconds=2)"))
        < now - timedelta(weeks=1, seconds=1)
    )


@pytest.mark.unit
def test_temporal_suite_parameters_complex():
    # allow 1 second for "now" tolerance
    now = datetime.now()  # noqa: DTZ005
    # Choosing "2*3" == 6 weeks shows we can parse an expression inside a kwarg.
    assert (
        (now - timedelta(weeks=2 * 3, seconds=3))
        < dateutil.parser.parse(parse_suite_parameter("now() - timedelta(weeks=2*3, seconds=2)"))
        < now - timedelta(weeks=2 * 3, seconds=1)
    )


@pytest.mark.unit
def test_deduplicate_suite_parameter_dependencies():
    dependencies = {
        "profile": [
            {
                "metric_kwargs_id": {
                    "column=norm": ["expect_column_mean_to_be_between.result.observed_value"]
                }
            },
            {
                "metric_kwargs_id": {
                    "column=norm": ["expect_column_stdev_to_be_between.result.observed_value"]
                }
            },
        ]
    }

    deduplicated = _deduplicate_suite_parameter_dependencies(dependencies)

    # For test, use set to ignore order
    deduplicated["profile"][0]["metric_kwargs_id"]["column=norm"] = set(
        deduplicated["profile"][0]["metric_kwargs_id"]["column=norm"]
    )

    assert deduplicated == {
        "profile": [
            {
                "metric_kwargs_id": {
                    "column=norm": {
                        "expect_column_mean_to_be_between.result.observed_value",
                        "expect_column_stdev_to_be_between.result.observed_value",
                    }
                }
            }
        ]
    }


@pytest.mark.unit
def test_now_suite_parameter():
    """
    now() is unique in the fact that it is the only suite param built-in that has zero arity (takes no arguments).
    The following tests ensure that it is properly parsed and evaluated in a variety of contexts.
    """  # noqa: E501
    # By itself
    res = parse_suite_parameter("now()")
    assert dateutil.parser.parse(res), "Provided suite parameter is not dateutil-parseable"

    # In conjunction with timedelta
    res = parse_suite_parameter("now() - timedelta(weeks=1)")
    assert dateutil.parser.parse(res), "Provided suite parameter is not dateutil-parseable"

    # Require parens to actually invoke
    with pytest.raises(SuiteParameterError):
        parse_suite_parameter("now")
