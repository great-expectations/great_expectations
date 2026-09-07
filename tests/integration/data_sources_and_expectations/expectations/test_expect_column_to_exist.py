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

EXISTING_COLUMN = "existing_column"
ANOTHER_EXISTING_COLUMN = "another_existing_column"

DATA = pd.DataFrame(
    {
        EXISTING_COLUMN: [1, 2],
        ANOTHER_EXISTING_COLUMN: ["a", "b"],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnToExist(
        column=EXISTING_COLUMN,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnToExist(
                column=EXISTING_COLUMN,
            ),
            id="basic_column_exists",
        ),
        pytest.param(
            gxe.ExpectColumnToExist(
                column=ANOTHER_EXISTING_COLUMN,
            ),
            id="another_column_exists",
        ),
        pytest.param(
            gxe.ExpectColumnToExist(
                column=EXISTING_COLUMN,
                column_index=0,
            ),
            id="column_exists_at_index",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnToExist,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnToExist(
                column="non_existent_column",
            ),
            id="column_does_not_exist",
        ),
        pytest.param(
            gxe.ExpectColumnToExist(
                column=EXISTING_COLUMN,
                column_index=1,
            ),
            id="wrong_column_index",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnToExist,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success
