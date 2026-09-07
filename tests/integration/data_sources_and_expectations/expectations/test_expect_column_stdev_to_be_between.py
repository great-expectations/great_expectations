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

NUM_COL = "all_numbers"
ALL_THE_SAME = "all_zeros"
MISSING_COL = "one_missing"

DATA = pd.DataFrame(
    {
        NUM_COL: [1, 1, 3],
        ALL_THE_SAME: [1, 1, 1],
        MISSING_COL: [None, -1, 1],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, min_value=1.15, max_value=1.5)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": pytest.approx(1.1547005383792517)}


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=0, max_value=0),
            id="all_the_same",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=MISSING_COL, min_value=1.414, max_value=1.415),
            id="missing_values",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, min_value=1),
            id="no_max",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, max_value=1.5),
            id="no_min",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(
                column=NUM_COL, min_value=1, max_value=1.5, strict_min=True, strict_max=True
            ),
            id="strict_bounds",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL),
            id="vacuous_truth",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnStdevToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=1, max_value=2),
            id="bad_range",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=0, strict_min=True),
            id="strict_min",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, max_value=0, strict_max=True),
            id="strict_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnStdevToBeBetween
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
    suite_param_key = "test_expect_column_stdev_to_be_between"
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column=NUM_COL,
        min_value=1,
        max_value=1.5,
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
    suite_param_key = "test_expect_column_stdev_to_be_between"
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column=NUM_COL,
        min_value=1,
        max_value=1.5,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result
