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

DATA = pd.DataFrame(
    {
        COL_NAME: [1, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 55],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_its_over_9000(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
        column=COL_NAME, min_value=4, max_value=6
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": 5}


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(column=COL_NAME),
            id="vacuous_truth",
        ),
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(column=COL_NAME, min_value=4),
            id="no_max",
        ),
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(column=COL_NAME, max_value=6),
            id="no_min",
        ),
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(
                column=COL_NAME, min_value=4, max_value=6, strict_min=True, strict_max=True
            ),
            id="stict_bounds",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnUniqueValueCountToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(column=COL_NAME, min_value=6, max_value=8),
            id="bad_range",
        ),
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(
                column=COL_NAME, min_value=5, strict_min=True
            ),
            id="strict_min",
        ),
        pytest.param(
            gxe.ExpectColumnUniqueValueCountToBeBetween(
                column=COL_NAME, max_value=5, strict_max=True
            ),
            id="strict_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnUniqueValueCountToBeBetween
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
def test_success_with_suite_param_strict_min_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_unique_value_count_to_be_between"
    expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
        column=COL_NAME,
        min_value=4,
        max_value=6,
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
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_strict_max_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_unique_value_count_to_be_between"
    expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
        column=COL_NAME,
        min_value=4,
        max_value=6,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
