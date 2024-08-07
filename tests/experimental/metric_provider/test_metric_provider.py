from unittest import mock

import pytest
from contrib.experimental.metrics.column_metric import (
    ColumnMeanMetric,
)
from contrib.experimental.metrics.domain import ColumnMetricDomain
from contrib.experimental.metrics.mp_asset import (
    MPBatchParameters,
)
from contrib.experimental.metrics.provider_controller import (
    InMemoryMPCache,
    MetricProviderController,
)
from contrib.experimental.metrics.snowflake_provider import (
    SnowflakeConnectionMetricProvider,
)
from contrib.experimental.metrics.snowflake_provider_data_source import (
    build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource,
)
from great_expectations.core.partitioners import (
    ColumnPartitionerDaily,
)
from great_expectations.datasource.fluent.snowflake_datasource import (
    SnowflakeDatasource,
)
from great_expectations.datasource.fluent.sql_datasource import (
    TableAsset,
)


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
    with mock.patch.object(
        TableAsset, "test_connection"
    ) as mock_asset_test_connection:
        asset = datasource.add_table_asset(
            "test_table_asset",
            table_name="demo",
        )
        asset.add_batch_definition(
            "test_batch_definition",
            partitioner=ColumnPartitionerDaily(
                column_name="date"
            ),
        )
    return datasource


def test_build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(
    fluent_snowflake_datasource,
):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(
        fluent_snowflake_datasource
    )
    assert len(datasource._assets) == 1
    assert (
        datasource._assets[0].table_name
        == "demo"
    )


def test_snowflake_connection_metric_provider_is_supported_batch_definition(
    fluent_snowflake_datasource,
):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(
        fluent_snowflake_datasource
    )
    asset = datasource._assets[0]
    batch_definition = (
        asset._batch_definitions[0]
    )
    with mock.patch(
        "contrib.experimental.metrics.snowflake_provider_data_source.SnowflakeUsernamePasswordConnector.get_connection"
    ) as mock_snowflake_conn:
        provider = (
            datasource.get_metric_provider()
        )
    assert provider.is_supported_domain(
        batch_definition
    )


def test_snowflake_connection_metric_provider_get_supported_metrics():
    mp = SnowflakeConnectionMetricProvider(
        connection=mock.MagicMock(),
    )
    assert (
        ColumnMeanMetric
        in mp.get_supported_metrics()
    )


def test_provider_controller_get_metric(
    fluent_snowflake_datasource,
):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(
        fluent_snowflake_datasource
    )
    with mock.patch(
        "contrib.experimental.metrics.snowflake_provider_data_source.SnowflakeUsernamePasswordConnector.get_connection"
    ) as mock_snowflake_conn:
        mock_cursor = mock.MagicMock()
        mock_connection = (
            mock_snowflake_conn.return_value
        )
        mock_connection.cursor.return_value.__enter__.return_value = (
            mock_cursor
        )

        mock_cursor.execute.return_value = (
            None
        )
        mock_cursor.fetchone.return_value = (
            123,
        )

        asset = datasource._assets[0]
        batch_definition = (
            asset._batch_definitions[0]
        )

        orchestrator = MetricProviderController(
            metric_providers=[
                datasource.get_metric_provider()
            ],
            caches=[InMemoryMPCache()],
        )
        metric = ColumnMeanMetric(
            domain=ColumnMetricDomain(
                batch_definition=batch_definition,
                batch_parameters=MPBatchParameters(
                    {
                        "year": 2024,
                        "month": 3,
                        "day": 3,
                    }
                ),
                column="test",
            )
        )
        value = orchestrator.get_metric(
            metric
        )
        assert value == 123
        value = orchestrator.get_metric(
            metric
        )
        assert value == 123
        # We should have been called *exactly* once!
        mock_cursor.execute.assert_called_once_with(
            "SELECT AVG(test) FROM demo WHERE YEAR(date) = 2024"
        )


def test_metric_provider_get_metric(
    fluent_snowflake_datasource,
):
    datasource = build_MetricProviderSnowflakeDatasource_from_SnowflakeDatasource(
        fluent_snowflake_datasource
    )
    with mock.patch(
        "contrib.experimental.metrics.snowflake_provider_data_source.SnowflakeUsernamePasswordConnector.get_connection"
    ) as mock_snowflake_conn:
        mock_cursor = mock.MagicMock()
        mock_connection = (
            mock_snowflake_conn.return_value
        )
        mock_connection.cursor.return_value.__enter__.return_value = (
            mock_cursor
        )

        mock_cursor.execute.return_value = (
            None
        )
        mock_cursor.fetchone.return_value = (
            123,
        )

        asset = datasource._assets[0]
        batch_definition = (
            asset._batch_definitions[0]
        )

        provider: SnowflakeConnectionMetricProvider = (
            datasource.get_metric_provider()
        )
        metric = ColumnMeanMetric(
            domain=ColumnMetricDomain(
                batch_definition=batch_definition,
                batch_parameters=MPBatchParameters(
                    {
                        "year": 2024,
                        "month": 3,
                        "day": 3,
                    }
                ),
                column="test",
            )
        )
        value = provider.get_metric_implementation(
            metric
        ).compute()
        assert value == 123
        mock_cursor.execute.assert_called_once_with(
            "SELECT AVG(test) FROM demo WHERE YEAR(date) = 2024"
        )
