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

COL_A = "col_a"
COL_B = "col_b"
COL_C = "col_c"


DATA = pd.DataFrame(
    {
        COL_A: [1],
        COL_B: [2],
        COL_C: [3],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B]),
            id="missing_cols",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C, "col_d"]),
            id="extra_cols",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_A, COL_B, COL_C.upper()]),
            id="wrong_value",
        ),
        pytest.param(
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=[COL_C, COL_B, COL_A]),
            id="wrong_order",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectTableColumnsToMatchOrderedList
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success
