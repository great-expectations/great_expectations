from unittest import mock

import pytest
from contrib.experimental.metrics.column_metric import (
    ColumnMeanMetric,
    ColumnValuesMatchRegexMetric,
)
from contrib.experimental.metrics.column_metric import (
    ColumnValuesMatchRegexUnexpectedValuesMetric,
)
from contrib.experimental.metrics.domain import (
    ColumnMetricDomain,
)
from contrib.experimental.metrics.snowflake_provider import (
    SnowflakeColumnMeanMetricImplementation,
    SnowflakeColumnValuesMatchRegexMetricImplementation,
    SnowflakeColumnValuesMatchRegexUnexpectedValuesMetricImplementation,
    SnowflakeConnectionMetricProvider,
)
from contrib.experimental.metrics.snowflake_provider_domain import (
    SnowflakeMPBatchDefinition,
)


@pytest.fixture
def mock_snowflake_connection_metric_provider():
    mp = mock.Mock(
        spec=SnowflakeConnectionMetricProvider
    )
    mock_connection = mock.MagicMock()
    mock_cursor = mock.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )
    mp.get_connection.return_value = (
        mock_connection
    )
    mp.get_selectable.return_value = (
        "MOCK SELECTABLE"
    )
    mp.mock_cursor = mock_cursor
    return mp


def test_snowflake_mean_column_metric_implementation(
    mock_snowflake_connection_metric_provider,
):
    mock_cursor = (
        mock_snowflake_connection_metric_provider.mock_cursor
    )
    mock_cursor.fetchone.return_value = (
        0.5,
    )
    metric_impl = SnowflakeColumnMeanMetricImplementation(
        metric=ColumnMeanMetric(
            domain=ColumnMetricDomain(
                batch_definition=mock.MagicMock(
                    spec=SnowflakeMPBatchDefinition
                ),
                batch_parameters=mock.MagicMock(),
                column="test",
            )
        ),
        provider=mock_snowflake_connection_metric_provider,
    )

    result = metric_impl.compute()
    assert result == 0.5
    mock_cursor.execute.assert_called_once_with(
        "SELECT AVG(test) FROM MOCK SELECTABLE"
    )


def test_snowflake_regex_column_metric_implementation(
    mock_snowflake_connection_metric_provider,
):
    mock_cursor = (
        mock_snowflake_connection_metric_provider.mock_cursor
    )
    mock_cursor.fetchone.return_value = (
        50,
    )
    metric_impl = SnowflakeColumnValuesMatchRegexMetricImplementation(
        metric=ColumnValuesMatchRegexMetric(
            domain=ColumnMetricDomain(
                batch_definition=mock.MagicMock(
                    spec=SnowflakeMPBatchDefinition
                ),
                batch_parameters=mock.MagicMock(),
                column="test",
            ),
            regex="[a-z]+",
        ),
        provider=mock_snowflake_connection_metric_provider,
    )

    result = metric_impl.compute()
    assert result == 50
    mock_cursor.execute.assert_called_once_with(
        "SELECT COUNT(*) FROM MOCK SELECTABLE WHERE test REGEXP '[a-z]+'"
    )


def test_snowflake_regex_column_unexpected_values_metric_implementation(
    mock_snowflake_connection_metric_provider,
):
    limit = 10
    mock_return_value = [("cat",)] * limit
    mock_cursor = (
        mock_snowflake_connection_metric_provider.mock_cursor
    )
    mock_cursor.fetchall.return_value = (
        mock_return_value
    )
    metric_impl = SnowflakeColumnValuesMatchRegexUnexpectedValuesMetricImplementation(
        metric=ColumnValuesMatchRegexUnexpectedValuesMetric(
            domain=ColumnMetricDomain(
                batch_definition=mock.MagicMock(
                    spec=SnowflakeMPBatchDefinition
                ),
                batch_parameters=mock.MagicMock(),
                column="test",
            ),
            regex="[a-z]+",
            limit=limit,
        ),
        provider=mock_snowflake_connection_metric_provider,
    )

    result = metric_impl.compute()
    assert result == [
        val[0] for val in mock_return_value
    ]
    mock_cursor.execute.assert_called_once_with(
        "SELECT test FROM MOCK SELECTABLE WHERE test REGEXP '[a-z]+' LIMIT 10"
    )
