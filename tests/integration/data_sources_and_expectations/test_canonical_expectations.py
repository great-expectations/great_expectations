import pandas as pd

import great_expectations.expectations as gxe
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    PandasDataFrameDatasourceConfig,
)


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceConfig(),
    ],
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_min(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMinToBeBetween(column="a", min_value=1, max_value=1)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[
        PandasDataFrameDatasourceConfig(),
    ],
    data=pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
)
def test_max(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMaxToBeBetween(column="a", min_value=2, max_value=2)
    result = batch_for_datasource.validate(expectation)
    assert result.success
