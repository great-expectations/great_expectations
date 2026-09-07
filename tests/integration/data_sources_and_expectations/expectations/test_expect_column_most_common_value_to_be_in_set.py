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

COL_WITHOUT_TIE = "without_tie"
COL_WITH_TIE = "with_tie"
STRING_COL = "string_col"

A_LOT_OF_NONES = [None] * 10
DATA = pd.DataFrame(
    {
        COL_WITHOUT_TIE: [0, 0, 1, 1, 2, 2, 3, 4, 4, 4, *A_LOT_OF_NONES],
        COL_WITH_TIE: [0, 1, 1, 2, 2, 2, 3, 4, 4, 4, *A_LOT_OF_NONES],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(column=COL_WITHOUT_TIE, value_set=[4])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": [4]}


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({COL_WITHOUT_TIE: ["foo", "foo", "foo", "bar", "baz"]}),
)
def test_strings(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(
        column=COL_WITHOUT_TIE, value_set=["foo"]
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_WITH_TIE, value_set=[2, 4, 100], ties_okay=True
            ),
            id="tie_and_value_set_has_extra_values",
        ),
        pytest.param(
            # This test case surprises me
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_WITH_TIE, value_set=[2], ties_okay=True
            ),
            id="value_set_is_subset",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMostCommonValueToBeInSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_WITH_TIE, value_set=[100, 101], ties_okay=True
            ),
            id="no_matches_to_value_set",
        ),
        pytest.param(
            # This feels like it conflicts with the 2 positive cases above
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_WITH_TIE, value_set=[2, 100], ties_okay=False
            ),
            id="value_set_and_observed_have_partial_overlap",
        ),
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_WITH_TIE, value_set=[2, 4], ties_okay=False
            ),
            id="ties_not_okay",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMostCommonValueToBeInSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_ties_okay_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_most_common_value_to_be_in_set"
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(
        column=COL_WITH_TIE,
        value_set=[2, 4, 100],
        ties_okay={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
