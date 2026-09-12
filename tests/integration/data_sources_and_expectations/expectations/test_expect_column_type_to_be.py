import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    SqliteDatasourceTestConfig,
)

INTEGER_COLUMN = "integers"
STRING_COLUMN = "strings"

DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: [1, 2, 3, 4, 5],
        STRING_COLUMN: ["a", "b", "c", "d", "e"],
    },
    dtype="object",
)

TYPED_DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: pd.Series([1, 2, 3, 4, 5], dtype="int64"),
        STRING_COLUMN: pd.Series(["a", "b", "c", "d", "e"], dtype="str"),
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=TYPED_DATA,
)
def test_success_pandas(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="int64")
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert set(result.result) == {"observed_value"}


@parameterize_batch_for_data_sources(
    data_source_configs=[
        SqliteDatasourceTestConfig(),
    ],
    data=DATA,
)
def test_success_sql_integer(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.result["observed_value"] == "INTEGER"


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column=INTEGER_COLUMN, type_="NUMBER")
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=DATA,
)
def test_missing_column_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnTypeToBe(column="non_existent_column", type_="INTEGER")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
