from typing import Dict, List

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import Batch, DataAsset
from great_expectations.experimental.metric_repository.metric_list_metric_retriever import (
    MetricListMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricException,
    MetricTypes,
    TableMetric,
)
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfigurationID
from great_expectations.validator.validation_graph import MetricsCalculatorErrorResultValue
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit

import logging

from pytest_mock import MockerFixture

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def mock_data_asset(mocker):
    data_asset = mocker.Mock(spec=DataAsset)
    data_asset.name = "some_data_asset_name"
    return data_asset


@pytest.fixture(scope="function")
def mock_validator(mocker, mock_batch):
    validator = mocker.Mock(spec=Validator)
    validator.active_batch = mock_batch
    return validator


@pytest.fixture(scope="function")
def mock_context(mocker, mock_validator):
    context = mocker.Mock(spec=CloudDataContext)
    context.get_validator.return_value = mock_validator
    return context


@pytest.fixture(scope="function")
def mock_batch(mocker, mock_data_asset):
    batch = mocker.Mock(spec=Batch)
    batch.id = "batch_id"
    batch.data_asset = mock_data_asset
    return batch


@pytest.fixture(scope="function")
def metric_retriever(mock_context):
    return MetricListMetricRetriever(context=mock_context)


@pytest.fixture(scope="function")
def mock_batch_request(mocker):
    batch_request = mocker.Mock(spec=BatchRequest)
    batch_request.data_asset_name = "some_data_asset_name"
    return batch_request


@pytest.fixture(scope="function")
def mock_batch_request_variant(mocker):
    batch_request = mocker.Mock(spec=BatchRequest)
    batch_request.data_asset_name = "other_data_asset_name"
    return batch_request


def test_get_metrics_table_metrics_only(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
    }
    table_metrics_list = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    aborted_metrics: Dict[str, str] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )

    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request,
        metric_list=table_metrics_list,
    )
    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float", "primary_key": False},
                {"name": "col2", "type": "float", "primary_key": False},
            ],
            exception=None,
        ),
    ]


def test_get_metrics_full_list(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        ("column.min", "column=col1", ()): 2.5,
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column.non_null_count", "column=col1", ()): 0,
        ("column.non_null_count", "column=col2", ()): 0,
        ("column.unique_proportion", "column=col1", ()): 0.5,
        ("column.unique_proportion", "column=col2", ()): 0.75,
    }
    metrics_list = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_MEAN,
        MetricTypes.COLUMN_MEDIAN,
        MetricTypes.COLUMN_NON_NULL_COUNT,
        MetricTypes.COLUMN_UNIQUE_PROPORTION,
    ]
    aborted_metrics: Dict[str, str] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )

    patch_get_numeric_column_names_with(mocker, ["col1", "col2"])
    patch_get_timestamp_column_names_with(mocker, [])
    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request,
        metric_list=metrics_list,
    )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id", metric_name="table.row_count", value=2, exception=None
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float", "primary_key": False},
                {"name": "col2", "type": "float", "primary_key": False},
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            value=5.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            value=5.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column.non_null_count",
            value=0,
            exception=None,
            column="col1",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column.non_null_count",
            value=0,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.unique_proportion",
            value=0.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.unique_proportion",
            value=0.75,
            exception=None,
            column="col2",
        ),
    ]


def test_column_metrics_not_returned_if_column_types_missing(
    mocker: MockerFixture,
    caplog,
    mock_context,
    mock_validator,
    mock_batch_request,
    metric_retriever,
):
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["timestamp_col"],
    }
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        # MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_NON_NULL_COUNT,
    ]
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )

    patch_get_numeric_column_names_with(mocker, [])
    patch_get_timestamp_column_names_with(mocker, ["timestamp_col"])
    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["timestamp_col"],
            exception=None,
        ),
    ]
    assert "TABLE_COLUMN_TYPES metric is required to compute column metrics." in caplog.text


def patch_get_numeric_column_names_with(mocker, return_value):
    mocker.patch(
        f"{MetricListMetricRetriever.__module__}.{MetricListMetricRetriever.__name__}._get_numeric_column_names",
        return_value=return_value,
    )


def test_get_metrics_metrics_missing(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    """This test is meant to simulate metrics missing from the computed metrics."""
    mock_computed_metrics = {
        # ("table.row_count", (), ()): 2, # Missing table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        # ("column.min", "column=col1", ()): 2.5, # Missing column.min metric for col1
        ("column.min", "column=col2", ()): 2.7,
    }

    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
    ]
    mock_aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (
        mock_computed_metrics,
        mock_aborted_metrics,
    )
    patch_get_numeric_column_names_with(mocker, ["col1", "col2"])
    patch_get_timestamp_column_names_with(mocker, [])
    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )
    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            ),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float", "primary_key": False},
                {"name": "col2", "type": "float", "primary_key": False},
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            value=None,
            exception=MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            ),
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            value=2.7,
            exception=None,
            column="col2",
        ),
    ]


def patch_get_timestamp_column_names_with(mocker, return_value):
    mocker.patch(
        f"{MetricListMetricRetriever.__module__}.{MetricListMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=return_value,
    )


def test_get_metrics_with_exception(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    """This test is meant to simulate failed metrics in the computed metrics."""

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )
    aborted_metrics = {
        ("table.row_count", (), ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
        ("column.min", "column=col1", ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
    }
    computed_metrics = {
        # ("table.row_count", (), ()): 2, # Error in table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
    }
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]

    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float", "primary_key": False},
                {"name": "col2", "type": "float", "primary_key": False},
            ],
            exception=None,
        ),
    ]


def test_get_metrics_with_column_type_missing(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    """This test is meant to simulate failed metrics in the computed metrics."""

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )

    aborted_metrics = {
        ("table.row_count", (), ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
        ("column.min", "column=col1", ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
    }

    computed_metrics = {
        # ("table.row_count", (), ()): 2, # Error in table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {
                "name": "col2",
            },  # Missing type for col2
        ],
        # ("column.min", "column=col1", ()): 2.5, # Error in column.min metric for col1
        ("column.min", "column=col2", ()): 2.7,
    }
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
    ]

    patch_get_numeric_column_names_with(mocker, ["col1", "col2"])
    patch_get_timestamp_column_names_with(mocker, [])

    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )
    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float", "primary_key": False},
                {
                    "name": "col2",
                    "primary_key": False,
                },  # Note: No type for col2
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col1",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col2",
            value=2.7,
            exception=None,
        ),
    ]


def test_get_metrics_with_timestamp_columns(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["timestamp_col"],
        ("table.column_types", (), ()): [
            {"name": "timestamp_col", "type": "TIMESTAMP_NTZ"},
        ],
        ("column.min", "column=timestamp_col", ()): "2023-01-01T00:00:00",
        ("column.max", "column=timestamp_col", ()): "2023-12-31T00:00:00",
        ("column.non_null_count", "column=timestamp_col", ()): 0,
    }
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_NON_NULL_COUNT,
    ]
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    patch_get_numeric_column_names_with(mocker, [])
    patch_get_timestamp_column_names_with(mocker, ["timestamp_col"])

    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["timestamp_col"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[{"name": "timestamp_col", "type": "TIMESTAMP_NTZ", "primary_key": False}],
            exception=None,
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.max",
            value="2023-12-31T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.min",
            value="2023-01-01T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column.non_null_count",
            value=0,
            exception=None,
            column="timestamp_col",
        ),
    ]


def test_get_metrics_only_gets_a_validator_once(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}

    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
    }
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    # No column filtering needed for table-only metrics
    metric_retriever.get_metrics(batch_request=mock_batch_request, metric_list=metrics_list)

    mock_context.get_validator.assert_called_once_with(batch_request=mock_batch_request)


def test_get_metrics_only_gets_new_validator_on_asset_change(
    mocker: MockerFixture,
    mock_context,
    mock_validator,
    mock_batch_request_variant,
    metric_retriever,
):
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}

    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
    }
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    # No column filtering needed for table-only metrics
    metric_retriever.get_metrics(batch_request=mock_batch_request_variant, metric_list=metrics_list)

    assert mock_context.get_validator.call_count == 4
    mock_context.get_validator.assert_called_with(batch_request=mock_batch_request_variant)


def test_get_metrics_with_no_metrics(
    mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {}
    metrics_list: List[MetricTypes] = []
    aborted_metrics = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    with pytest.raises(ValueError):
        metric_retriever.get_metrics(batch_request=mock_batch_request, metric_list=metrics_list)


def test_valid_metric_types_true(mock_context, metric_retriever):
    valid_metric_types = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_MEAN,
        MetricTypes.COLUMN_MEDIAN,
        MetricTypes.COLUMN_NON_NULL_COUNT,
        MetricTypes.COLUMN_UNIQUE_PROPORTION,
    ]
    assert metric_retriever._check_valid_metric_types(valid_metric_types) is True


def test_valid_metric_types_false(mocker: MockerFixture, mock_context, metric_retriever):
    invalid_metric_type = ["I_am_invalid"]
    assert metric_retriever._check_valid_metric_types(invalid_metric_type) is False


def test_column_metrics_in_metrics_list_only_table_metrics(
    mocker: MockerFixture, mock_context, metric_retriever
):
    table_metrics_only = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    assert metric_retriever._column_metrics_in_metric_list(table_metrics_only) is False


def test_column_metrics_in_metrics_list_with_column_metrics(
    mocker: MockerFixture, mock_context, metric_retriever
):
    metrics_list_with_column_metrics = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
    ]
    assert metric_retriever._column_metrics_in_metric_list(metrics_list_with_column_metrics) is True


def test_get_table_column_types(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {
        ("table.column_types", (), ()): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
    }
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    ret = metric_retriever._get_table_column_types(mock_batch_request)
    print(ret)


def test_get_table_columns(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {
        ("table.columns", (), ()): ["col1", "col2"],
    }
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (computed_metrics, aborted_metrics)

    ret = metric_retriever._get_table_columns(mock_batch_request)
    assert ret == TableMetric[List[str]](
        batch_id="batch_id",
        metric_name="table.columns",
        value=["col1", "col2"],
        exception=None,
    )


def test_get_table_row_count(
    mocker: MockerFixture, mock_context, mock_validator, mock_batch_request, metric_retriever
):
    computed_metrics = {("table.row_count", (), ()): 2}
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (computed_metrics, aborted_metrics)

    ret = metric_retriever._get_table_row_count(mock_batch_request)
    assert ret == TableMetric[int](
        batch_id="batch_id",
        metric_name="table.row_count",
        value=2,
        exception=None,
    )


def test_get_metrics_with_timestamp_columns_exclude_time(
    mocker: MockerFixture, mock_context, mock_validator, metric_retriever, mock_batch_request
):
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["timestamp_col", "time_col"],
        ("table.column_types", (), ()): [
            {"name": "timestamp_col", "type": "TIMESTAMP_NTZ"},
            {"name": "time_col", "type": "TIME"},
        ],
        ("column.min", "column=timestamp_col", ()): "2023-01-01T00:00:00",
        ("column.max", "column=timestamp_col", ()): "2023-12-31T00:00:00",
        ("column.non_null_count", "column=timestamp_col", ()): 0,
        ("column.non_null_count", "column=time_col", ()): 0,
    }
    metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_NON_NULL_COUNT,
    ]
    aborted_metrics: Dict[MetricConfigurationID, MetricsCalculatorErrorResultValue] = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    patch_get_numeric_column_names_with(mocker, [])
    patch_get_timestamp_column_names_with(mocker, ["timestamp_col"])
    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request, metric_list=metrics_list
    )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["timestamp_col", "time_col"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "timestamp_col", "type": "TIMESTAMP_NTZ", "primary_key": False},
                {"name": "time_col", "type": "TIME", "primary_key": False},
            ],
            exception=None,
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.max",
            value="2023-12-31T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.min",
            value="2023-01-01T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column.non_null_count",
            value=0,
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column.non_null_count",
            value=0,
            exception=None,
            column="time_col",
        ),
    ]
