from unittest import mock

import pytest
from contrib.experimental.metrics.snowflake_provider import ColumnMeanMetric, SnowflakeColumnMeanMetricImplementation, SnowflakeConnectionMetricProvider
from contrib.experimental.metrics.snowflake_provider_datasource import build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource
from great_expectations.core.partitioners import ColumnPartitionerDaily
from great_expectations.datasource.fluent.snowflake_datasource import SnowflakeDatasource
from great_expectations.datasource.fluent.sql_datasource import TableAsset


@pytest.fixture
def fluent_snowflake_datasource():
    datasource = SnowflakeDatasource(
        name="test",
        user="test",
        account="test",
        password="test",
        database="test",
        schema="test",
        warehouse="test",
        role="test",
        
    )
    with mock.patch.object(TableAsset, 'test_connection') as mock_asset_test_connection:
        asset = datasource.add_table_asset("test_table_asset", table_name="demo")
        asset.add_batch_definition("test_batch_definition", partitioner=ColumnPartitionerDaily(column_name="date"))
    return datasource

def test_build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource)
    assert len(datasource._assets) == 1
    assert datasource._assets[0].table == "demo"



def test_get_metric_new_style(fluent_snowflake_datasource):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource)
    with mock.patch('contrib.experimental.metrics.snowflake_provider_datasource.SnowflakeUsernamePasswordConnector.get_connection') as mock_snowflake_conn:
        mock_cursor = mock.MagicMock()
        mock_connection = mock_snowflake_conn.return_value
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = (123,)

        provider: SnowflakeConnectionMetricProvider = datasource.get_metric_provider()
        provider.register_metric(ColumnMeanMetric, SnowflakeColumnMeanMetricImplementation)
        
        asset = datasource._assets[0]
        batch_definition = asset._batch_definitions[0]
        value = provider.get_metric(ColumnMeanMetric(batch_definition=batch_definition, batch_parameters={"year": 2024, "month": 3, "day": 3}, column="test"))
        assert value == (123,)
        mock_cursor.execute.assert_called_once_with("SELECT AVG(test) FROM demo WHERE YEAR(date) = 2024")