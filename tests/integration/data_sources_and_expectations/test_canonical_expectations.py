import pandas as pd

from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import DataSourceType, parameterize_batch_for_data_sources


@parameterize_batch_for_data_sources(
    types=[DataSourceType.FOO, DataSourceType.BAR],
    data=pd.DataFrame([[1, 2]]),
    # description="test_stuff",
)
def test_stuff(batch_for_datasource) -> None:
    assert isinstance(batch_for_datasource, Batch)


@parameterize_batch_for_data_sources(
    types=[DataSourceType.FOO, DataSourceType.BAR],
    data=pd.DataFrame([[1, 2]]),
    # description="test_stuff",
)
def test_more_stuff(batch_for_datasource) -> None:
    assert isinstance(batch_for_datasource, Batch)
