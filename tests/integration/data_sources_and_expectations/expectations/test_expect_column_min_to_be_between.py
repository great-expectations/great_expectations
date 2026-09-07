from datetime import datetime, timezone

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

COL_NAME = "my_col"

THREES_AND_FIVES = pd.DataFrame({COL_NAME: [3, 5]})


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=THREES_AND_FIVES)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=1, max_value=4)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": 3}


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME),
            id="vacuous_success",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=1, max_value=4),
            id="min_and_max",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=1),
            id="just_min",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME, max_value=4),
            id="just_max",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(
                column=COL_NAME, min_value=1, max_value=4, strict_min=True, strict_max=True
            ),
            id="strict_min_and_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: [2, 3, None]})
)
def test_success(batch_for_datasource: Batch, expectation: gxe.ExpectColumnMinToBeBetween) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    data=pd.DataFrame({COL_NAME: [datetime(2024, 11, 22).date(), datetime(2024, 11, 26).date()]}),  # noqa: DTZ001 # FIXME CoP
)
def test_dates(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(
        column=COL_NAME,
        min_value=datetime(2024, 11, 20).date(),  # noqa: DTZ001 # FIXME CoP
        max_value=datetime(2024, 11, 22).date(),  # noqa: DTZ001 # FIXME CoP
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: [1, 2, None]})
)
def test_ignores_nulls(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=1, max_value=3)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=4),
            id="just_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(
                column=COL_NAME, min_value=3, strict_min=True, max_value=100
            ),
            id="strict_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(column=COL_NAME, max_value=2),
            id="just_max_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMinToBeBetween(
                column=COL_NAME, min_value=1, max_value=3, strict_max=True
            ),
            id="strict_max_fail",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=THREES_AND_FIVES
)
def test_failure(batch_for_datasource: Batch, expectation: gxe.ExpectColumnMinToBeBetween) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: []})
)
def test_no_data(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(
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
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=THREES_AND_FIVES
)
def test_success_with_suite_param_strict_min_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_min_to_be_between"
    expectation = gxe.ExpectColumnMinToBeBetween(
        column=COL_NAME,
        min_value=1,
        max_value=4,
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
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=THREES_AND_FIVES
)
def test_success_with_suite_param_strict_max_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "expect_column_min_to_be_between"
    expectation = gxe.ExpectColumnMinToBeBetween(
        column=COL_NAME,
        min_value=1,
        max_value=4,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


# The two tests below moved here from `test_canonical_expectations.py` when that module was retired.
# They parameterize over the same `ALL_DATA_SOURCES` they always did, and their names are unchanged,
# so the only thing that moved is which module owns them: the expectation module that owns every
# other test of this expectation.


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_expect_column_min_to_be_between(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(column="a", min_value=1, max_value=1)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame(
        {
            "date": [
                datetime(year=2021, month=1, day=31, tzinfo=timezone.utc).date(),
                datetime(year=2022, month=1, day=31, tzinfo=timezone.utc).date(),
                datetime(year=2023, month=1, day=31, tzinfo=timezone.utc).date(),
            ]
        }
    ),
)
def test_expect_column_min_to_be_between__date(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(
        column="date",
        min_value=datetime(year=2021, month=1, day=1, tzinfo=timezone.utc).date(),
        max_value=datetime(year=2022, month=1, day=1, tzinfo=timezone.utc).date(),
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
