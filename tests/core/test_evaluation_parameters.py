import math
from datetime import datetime, timedelta
from timeit import timeit
from typing import Any, Dict

import dateutil
import pandas as pd
import pytest

from great_expectations.core import (
    ExpectationSuite,
    ExpectationValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.suite_parameters import (
    _deduplicate_suite_parameter_dependencies,
    find_suite_parameter_dependencies,
    get_suite_parameter_key,
    is_suite_parameter,
    parse_suite_parameter,
)
from great_expectations.exceptions import SuiteParameterError
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


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
            "urn:great_expectations:validations:blarg",
            {"urn:great_expectations:validations:blarg": 1},
        )
        == 1
    )

    # Very basic arithmetic is allowed as-is:
    assert parse_suite_parameter("1 + 1", {}) == 2

    # So is simple variable substitution:
    assert parse_suite_parameter("a + 1", {"a": 2}) == 3

    # URN syntax works
    assert (
        parse_suite_parameter(
            "urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 10
            },
        )
        == 9
    )

    # bare decimal is accepted
    assert (
        parse_suite_parameter(
            "urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * .9",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 10
            },
        )
        == 9
    )

    # We have basic operations (trunc)
    assert (
        parse_suite_parameter(
            "urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 11
            },
        )
        != 9
    )

    assert (
        parse_suite_parameter(
            "trunc(urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9)",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 11
            },
        )
        == 9
    )

    # Non GX URN syntax fails
    with pytest.raises(SuiteParameterError) as err:
        parse_suite_parameter("urn:ieee:not_ge * 10", {"urn:ieee:not_ge": 1})
    assert "Parse Failure" in str(err.value)

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


@pytest.mark.filesystem
def test_query_store_results_in_suite_parameters(data_context_with_query_store):
    TITANIC_ROW_COUNT = 1313  # taken from the titanic db conftest
    DISTINCT_TITANIC_ROW_COUNT = 4

    # parse_suite_parameters correctly resolves a stores URN
    res1 = parse_suite_parameter(
        parameter_expression="urn:great_expectations:stores:my_query_store:col_count",
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert res1 == TITANIC_ROW_COUNT

    # and can handle an operator
    res2 = parse_suite_parameter(
        parameter_expression="urn:great_expectations:stores:my_query_store:col_count * 2",
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert res2 == TITANIC_ROW_COUNT * 2

    # can even handle multiple operators
    res3 = parse_suite_parameter(
        parameter_expression="urn:great_expectations:stores:my_query_store:col_count * 0 + 100",
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert res3 == 100

    # allows stores URNs with functions
    res4 = parse_suite_parameter(
        parameter_expression="cos(urn:great_expectations:stores:my_query_store:col_count)",
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert math.isclose(math.cos(TITANIC_ROW_COUNT), res4)

    # multiple stores URNs can be used
    res5 = parse_suite_parameter(
        parameter_expression="urn:great_expectations:stores:my_query_store:col_count - urn:great_expectations:stores:my_query_store:dist_col_count",  # noqa: E501
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert res5 == TITANIC_ROW_COUNT - DISTINCT_TITANIC_ROW_COUNT

    # complex expressions can combine operators, urns, and functions
    res6 = parse_suite_parameter(
        parameter_expression="abs(-urn:great_expectations:stores:my_query_store:col_count - urn:great_expectations:stores:my_query_store:dist_col_count)",  # noqa: E501
        suite_parameters=None,
        data_context=data_context_with_query_store,
    )
    assert res6 == TITANIC_ROW_COUNT + DISTINCT_TITANIC_ROW_COUNT


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
    now = datetime.now()
    assert (
        (now - timedelta(weeks=1, seconds=3))
        < dateutil.parser.parse(parse_suite_parameter("now() - timedelta(weeks=1, seconds=2)"))
        < now - timedelta(weeks=1, seconds=1)
    )


@pytest.mark.unit
def test_temporal_suite_parameters_complex():
    # allow 1 second for "now" tolerance
    now = datetime.now()
    # Choosing "2*3" == 6 weeks shows we can parse an expression inside a kwarg.
    assert (
        (now - timedelta(weeks=2 * 3, seconds=3))
        < dateutil.parser.parse(parse_suite_parameter("now() - timedelta(weeks=2*3, seconds=2)"))
        < now - timedelta(weeks=2 * 3, seconds=1)
    )


@pytest.mark.unit
def test_find_suite_parameter_dependencies():
    parameter_expression = "(-3 * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between.result.observed_value:column=norm) + urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value:column=norm"  # noqa: E501
    dependencies = find_suite_parameter_dependencies(parameter_expression)
    assert dependencies == {
        "urns": {
            "urn:great_expectations:validations:profile:expect_column_stdev_to_be_between.result.observed_value:column=norm",
            "urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value:column=norm",
        },
        "other": set(),
    }

    parameter_expression = "upstream_value * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between.result.observed_value:column=norm"  # noqa: E501
    dependencies = find_suite_parameter_dependencies(parameter_expression)
    assert dependencies == {
        "urns": {
            "urn:great_expectations:validations:profile:expect_column_stdev_to_be_between.result.observed_value:column=norm",
        },
        "other": {"upstream_value"},
    }

    parameter_expression = "upstream_value"
    dependencies = find_suite_parameter_dependencies(parameter_expression)
    assert dependencies == {"urns": set(), "other": {"upstream_value"}}

    parameter_expression = "3 * upstream_value"
    dependencies = find_suite_parameter_dependencies(parameter_expression)
    assert dependencies == {"urns": set(), "other": {"upstream_value"}}

    parameter_expression = "3"
    dependencies = find_suite_parameter_dependencies(parameter_expression)
    assert dependencies == {"urns": set(), "other": set()}


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

    assert {
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
    } == deduplicated


@pytest.mark.filesystem
@pytest.mark.parametrize(
    "dataframe,suite_parameters,expectation_type,expectation_kwargs,expected_expectation_validation_result",
    [
        (
            pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            (
                ("my_min", 1),
                ("my_max", 5),
            ),
            "expect_table_row_count_to_be_between",
            {
                "min_value": {
                    "$PARAMETER": "my_min",
                },
                "max_value": {
                    "$PARAMETER": "my_max",
                },
            },
            ExpectationValidationResult(
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={
                        "min_value": 1,
                        "max_value": 5,
                    },
                    meta={"substituted_parameters": {"min_value": 1, "max_value": 5}},
                    id=None,
                ),
                meta={},
                exception_info={
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                success=True,
                result={"observed_value": 3},
            ),
        ),
        (
            pd.DataFrame(
                {
                    "my_date": [
                        datetime(year=2017, month=1, day=1),
                        datetime(year=2018, month=1, day=1),
                        datetime(year=2019, month=1, day=1),
                        datetime(year=2020, month=1, day=1),
                    ]
                }
            ),
            (
                ("my_min_date", datetime(2016, 12, 10)),
                ("my_max_date", datetime(2022, 12, 13)),
            ),
            "expect_column_values_to_be_between",
            {
                "column": "my_date",
                "min_value": {"$PARAMETER": "my_min_date"},
                "max_value": {"$PARAMETER": "my_max_date - timedelta(weeks=1)"},
            },
            ExpectationValidationResult(
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "my_date",
                        "min_value": datetime(year=2016, month=12, day=10),
                        "max_value": datetime(year=2022, month=12, day=6),
                        "batch_id": "15fe04adb6ff20b9fc6eda486b7a36b7",
                    },
                    meta={
                        "substituted_parameters": {
                            "min_value": "2016-12-10T00:00:00",
                            "max_value": "2022-12-06T00:00:00",
                        }
                    },
                    id=None,
                ),
                meta={},
                exception_info={
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                success=True,
                result={
                    "element_count": 4,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                },
            ),
        ),
    ],
)
def test_suite_parameters_for_between_expectations_parse_correctly(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    dataframe: pd.DataFrame,
    suite_parameters: Dict[str, Any],
    expectation_type: str,
    expectation_kwargs: Dict[str, dict],
    expected_expectation_validation_result: ExpectationValidationResult,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    expectation_suite_name = "test_suite"
    context.suites.add(ExpectationSuite(name=expectation_suite_name))

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="foo",
        runtime_parameters={"batch_data": dataframe},
        batch_identifiers={
            "pipeline_stage_name": "kickoff",
            "airflow_run_id": "1234",
        },
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    for suite_parameter in suite_parameters:
        validator.set_suite_parameter(*suite_parameter)

    actual_expectation_validation_result = getattr(validator, expectation_type)(
        **expectation_kwargs
    )

    assert actual_expectation_validation_result == expected_expectation_validation_result


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
