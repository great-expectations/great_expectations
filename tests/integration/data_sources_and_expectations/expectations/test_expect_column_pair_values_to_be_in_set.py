import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

DATA = pd.DataFrame({"foo": [1, 2, 4], "bar": [1, 1, 1]})

NULL_PAIR_NOT_AT_END = pd.DataFrame(
    {
        "currency": ["EUR", None, "EUR"],
        "country": ["DE", None, "US"],
    }
)

NON_DEFAULT_INDEX = pd.DataFrame(
    {
        "currency": ["EUR", "EUR"],
        "country": ["DE", "FR"],
    },
    index=[10, 20],
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnPairValuesToBeInSet(
        column_A="foo", column_B="bar", value_pairs_set=[(2, 1), (1, 1)], mostly=0.5
    )

    # act
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.BASIC)

    # assert
    assert result.success

    result_dict = result.to_json_dict()["result"]
    assert type(result_dict) is dict

    # these are not deterministic
    result_dict.pop("unexpected_index_query", None)
    result_dict.pop("unexpected_index_list", None)

    assert result_dict == {
        "element_count": 3,
        "missing_count": 0,
        "missing_percent": 0.0,
        "partial_unexpected_list": [
            [4, 1],
        ],
        "unexpected_count": 1,
        "unexpected_percent": 33.33333333333333,
        "unexpected_percent_nonmissing": 33.33333333333333,
        "unexpected_percent_total": 33.33333333333333,
    }


def assert_successful_pair_validation(batch_for_datasource: Batch) -> dict:
    expectation = gxe.ExpectColumnPairValuesToBeInSet(
        column_A="currency",
        column_B="country",
        value_pairs_set=[("EUR", "DE"), ("EUR", "FR")],
    )

    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    assert result.success, f"expected a verdict, got exception_info={result.exception_info}"
    result_dict = result.to_json_dict()["result"]
    assert type(result_dict) is dict
    assert result_dict["unexpected_count"] == 0
    return result_dict


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=NULL_PAIR_NOT_AT_END
)
def test_pandas_ignored_row_in_middle_returns_verdict(batch_for_datasource: Batch) -> None:
    # The row after the ignored (null) row is a genuine violation of value_pairs_set. If
    # validation loses the post-filter index alignment (e.g. by rebuilding a plain
    # pd.Series(results) instead of reindexing to temp_df.index), the violation would be
    # attributed to the wrong row and this assertion would fail even though
    # `unexpected_count` still came out correct.
    expectation = gxe.ExpectColumnPairValuesToBeInSet(
        column_A="currency",
        column_B="country",
        value_pairs_set=[("EUR", "DE"), ("EUR", "FR")],
    )

    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    assert not result.success, f"expected a verdict, got exception_info={result.exception_info}"
    result_dict = result.to_json_dict()["result"]
    assert type(result_dict) is dict

    assert result_dict["element_count"] == 3
    assert result_dict["missing_count"] == 1
    assert result_dict["unexpected_count"] == 1
    assert result_dict["partial_unexpected_list"] == [["EUR", "US"]]


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=NON_DEFAULT_INDEX
)
def test_pandas_non_default_index_returns_verdict(batch_for_datasource: Batch) -> None:
    result_dict = assert_successful_pair_validation(batch_for_datasource)

    assert result_dict["element_count"] == 2
    assert result_dict["missing_count"] == 0


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param([(2, 1), (1, 1)], True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_value_pairs_set_(
    batch_for_datasource: Batch, suite_param_value: list[tuple], expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_pair_values_to_be_in_set"

    expectation = gxe.ExpectColumnPairValuesToBeInSet(
        column_A="foo",
        column_B="bar",
        value_pairs_set=[(2, 1), (1, 1)],
        mostly=0.5,
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param("both_values_are_missing", True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_ignore_row_if_(
    batch_for_datasource: Batch, suite_param_value: str, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_pair_values_to_be_in_set"

    expectation = gxe.ExpectColumnPairValuesToBeInSet(
        column_A="foo",
        column_B="bar",
        value_pairs_set=[(2, 1), (1, 1)],
        mostly=0.5,
        ignore_row_if={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
