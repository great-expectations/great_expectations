import pandas as pd

import great_expectations.expectations as gxe
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    PandasDataFrameDatasourceTestConfig,
)


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
    ],
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_expect_column_min_to_be_between(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(column="a", min_value=1, max_value=1)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceTestConfig(),
    ],
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_expect_column_max_to_be_between(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMaxToBeBetween(column="a", min_value=2, max_value=2)
    result = batch_for_datasource.validate(expectation)
    assert result.success
