import pytest

from great_expectations.datasource.fluent import PandasDatasource


@pytest.mark.cloud
def test_mock_cloud_pandas_datasource(mock_cloud_pandas_datasource: PandasDatasource):
    assert isinstance(mock_cloud_pandas_datasource, PandasDatasource)
