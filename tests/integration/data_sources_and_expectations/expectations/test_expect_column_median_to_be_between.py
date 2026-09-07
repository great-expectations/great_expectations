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

COL_NAME = "my_col"

ODD_COUNT = pd.DataFrame({COL_NAME: [1, 10, 11, None]}, dtype="object")
EVEN_COUNT = pd.DataFrame({COL_NAME: [1, 10, 11, 12, None]}, dtype="object")


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=EVEN_COUNT)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, min_value=10, max_value=11)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": 10.5}


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=EVEN_COUNT)
def test_success_even_count(batch_for_datasource: Batch) -> None:
    """Ensure the median is calculated as the mean of the two middle values."""
    expectation = gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, min_value=10.5, max_value=10.5)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(column=COL_NAME),
            id="vacuous_success",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, min_value=9),
            id="just_min",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, max_value=11),
            id="just_max",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(
                column=COL_NAME, min_value=9, max_value=11, strict_min=True, strict_max=True
            ),
            id="strict_min_and_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ODD_COUNT)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMedianToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, min_value=11),
            id="just_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(
                column=COL_NAME, min_value=10, strict_min=True, max_value=11
            ),
            id="strict_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(column=COL_NAME, max_value=9),
            id="just_max_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMedianToBeBetween(
                column=COL_NAME, min_value=9, max_value=10, strict_max=True
            ),
            id="strict_max_fail",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ODD_COUNT)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMedianToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: []})
)
def test_no_data(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMedianToBeBetween(
        column=COL_NAME, min_value=1, max_value=3, result_format=ResultFormat.SUMMARY
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert result.to_json_dict()["result"] == {"observed_value": None}


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=EVEN_COUNT)
def test_success_with_suite_param_strict_min_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_median_to_be_between"
    expectation = gxe.ExpectColumnMedianToBeBetween(
        column=COL_NAME,
        min_value=10,
        max_value=11,
        strict_min={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )

    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=EVEN_COUNT)
def test_success_with_suite_param_strict_max_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_median_to_be_between"
    expectation = gxe.ExpectColumnMedianToBeBetween(
        column=COL_NAME,
        min_value=10,
        max_value=11,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
