from unittest import mock

import pytest
from contrib.experimental.metrics.metric import ColumnValuesMatchRegexMetric, ColumnValuesMatchRegexUnexpectedValuesMetric
from contrib.experimental.metrics.metric_provider import MPBatchParameters
from contrib.experimental.metrics.provider_controller import InMemoryMPCache, MetricProviderController
from contrib.experimental.metrics.snowflake_provider import ColumnMeanMetric, SnowflakeColumnMeanMetricImplementation, SnowflakeColumnValuesMatchRegexMetricImplementation, SnowflakeColumnValuesMatchRegexUnexpectedValuesMetricImplementation, SnowflakeConnectionMetricProvider
from contrib.experimental.metrics.snowflake_provider_batch_definition import SnowflakeMPBatchDefinition
from contrib.experimental.metrics.snowflake_provider_data_source import build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource
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


@pytest.fixture
def mock_snowflake_connection_metric_provider():
    mp = mock.Mock(spec=SnowflakeConnectionMetricProvider)
    mock_connection = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mp.get_connection.return_value = mock_connection
    mp.get_selectable.return_value = "MOCK SELECTABLE"
    mp.mock_cursor = mock_cursor
    return mp


def test_build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource)
    assert len(datasource._assets) == 1
    assert datasource._assets[0].table_name == "demo"


def test_provider_controller_get_metric(fluent_snowflake_datasource):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource)
    with mock.patch('contrib.experimental.metrics.snowflake_provider_data_source.SnowflakeUsernamePasswordConnector.get_connection') as mock_snowflake_conn:
        mock_cursor = mock.MagicMock()
        mock_connection = mock_snowflake_conn.return_value
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = (123,)

        asset = datasource._assets[0]
        batch_definition = asset._batch_definitions[0]

        orchestrator = MetricProviderController(
            metric_providers=[datasource.get_metric_provider()],
            caches=[InMemoryMPCache()]
        )
        metric = ColumnMeanMetric(batch_definition=batch_definition, batch_parameters=MPBatchParameters({"year": 2024, "month": 3, "day": 3}), column="test")
        value = orchestrator.get_metric(metric)
        assert value == 123
        value = orchestrator.get_metric(metric)
        assert value == 123
        # We should have been called *exactly* once!
        mock_cursor.execute.assert_called_once_with("SELECT AVG(test) FROM demo WHERE YEAR(date) = 2024")


def test_metric_provider_get_metric(fluent_snowflake_datasource):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(fluent_snowflake_datasource)
    with mock.patch('contrib.experimental.metrics.snowflake_provider_data_source.SnowflakeUsernamePasswordConnector.get_connection') as mock_snowflake_conn:
        mock_cursor = mock.MagicMock()
        mock_connection = mock_snowflake_conn.return_value
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = (123,)

        asset = datasource._assets[0]
        batch_definition = asset._batch_definitions[0]

        provider: SnowflakeConnectionMetricProvider = datasource.get_metric_provider()
        metric = ColumnMeanMetric(batch_definition=batch_definition, batch_parameters=MPBatchParameters({"year": 2024, "month": 3, "day": 3}), column="test")
        value = provider.get_metric_implementation(metric).compute()
        assert value == 123
        mock_cursor.execute.assert_called_once_with("SELECT AVG(test) FROM demo WHERE YEAR(date) = 2024")


def test_snowflake_mean_column_metric_implementation(mock_snowflake_connection_metric_provider):
    mock_cursor = mock_snowflake_connection_metric_provider.mock_cursor
    mock_cursor.fetchone.return_value = (0.5,)
    metric_impl = SnowflakeColumnMeanMetricImplementation(
        metric=ColumnMeanMetric(
            batch_definition=mock.MagicMock(spec=SnowflakeMPBatchDefinition),
            batch_parameters=mock.MagicMock(),
            column="test"
        ),
        provider=mock_snowflake_connection_metric_provider
    )

    result = metric_impl.compute()
    assert result == 0.5
    mock_cursor.execute.assert_called_once_with("SELECT AVG(test) FROM MOCK SELECTABLE")


def test_snowflake_regex_column_metric_implementation(mock_snowflake_connection_metric_provider):
    mock_cursor = mock_snowflake_connection_metric_provider.mock_cursor
    mock_cursor.fetchone.return_value = (50,)
    metric_impl = SnowflakeColumnValuesMatchRegexMetricImplementation(
        metric=ColumnValuesMatchRegexMetric(
            batch_definition=mock.MagicMock(spec=SnowflakeMPBatchDefinition),
            batch_parameters=mock.MagicMock(),
            column="test",
            regex="[a-z]+"
        ),
        provider=mock_snowflake_connection_metric_provider
    )

    result = metric_impl.compute()
    assert result == 50
    mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM MOCK SELECTABLE WHERE test REGEXP '[a-z]+'")


def test_snowflake_regex_column_unexpected_values_metric_implementation(mock_snowflake_connection_metric_provider):
    limit = 10
    mock_return_value = [("cat",)] * limit
    mock_cursor = mock_snowflake_connection_metric_provider.mock_cursor
    mock_cursor.fetchall.return_value = mock_return_value
    metric_impl = SnowflakeColumnValuesMatchRegexUnexpectedValuesMetricImplementation(
        metric=ColumnValuesMatchRegexUnexpectedValuesMetric(
            batch_definition=mock.MagicMock(spec=SnowflakeMPBatchDefinition),
            batch_parameters=mock.MagicMock(),
            column="test",
            regex="[a-z]+",
            limit=limit
        ),
        provider=mock_snowflake_connection_metric_provider
    )

    result = metric_impl.compute()
    assert result == [val[0] for val in mock_return_value]
    mock_cursor.execute.assert_called_once_with("SELECT test FROM MOCK SELECTABLE WHERE test REGEXP '[a-z]+' LIMIT 10")
