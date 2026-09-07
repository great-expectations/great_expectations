import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

INT_COL_A = "INT_COL_A"
INT_COL_B = "INT_COL_B"
INT_COL_C = "INT_COL_C"
STRING_COL_A = "STRING_COL_A"
STRING_COL_B = "STRING_COL_B"


DATA = pd.DataFrame(
    {
        INT_COL_A: [1, 1, 2, 3],
        INT_COL_B: [2, 2, 3, 4],
        INT_COL_C: [3, 3, 4, 4],
        STRING_COL_A: ["a", "b", "c", "d"],
        STRING_COL_B: ["x", "y", "z", "a"],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnCountToBeBetween(min_value=4, max_value=6)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=None, max_value=None),
            id="vacuously_true",
        ),
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=4, max_value=None),
            id="just_min",
        ),
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=None, max_value=6),
            id="just_max",
        ),
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=5, max_value=5),
            id="inclusivity",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnCountToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=8, max_value=None),
            id="just_min",
        ),
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=None, max_value=1),
            id="just_max",
        ),
        pytest.param(
            gxe.ExpectTableColumnCountToBeBetween(min_value=4, max_value=4),
            id="bad_range",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnCountToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.xfail(reason="Fails at validation, but should fail when instantiating")
@pytest.mark.unit
def test_valid_range() -> None:
    with pytest.raises(ValueError, match="min_value must be less than or equal to max_value"):
        gxe.ExpectTableColumnCountToBeBetween(min_value=5, max_value=4)
