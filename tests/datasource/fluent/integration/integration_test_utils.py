from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Tuple

import pytest

from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
    HeadData,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator
from tests.expectations.test_util import get_table_columns_metric

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch


logger = logging.getLogger(__name__)


def run_batch_head(  # noqa: C901, PLR0915
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
    fetch_all: bool | str,
    n_rows: int | float | str | None,  # noqa: PYI041
    success: bool,
) -> None:
    _, datasource, _, batch_request = datasource_test_data
    batch_list: list[Batch] = datasource.get_batch_list_from_batch_request(
        batch_request=batch_request
    )
    assert len(batch_list) > 0

    # arbitrarily select the first batch for testing
    batch: Batch = batch_list[0]
    if success:
        assert n_rows is None or isinstance(n_rows, int)
        assert isinstance(fetch_all, bool)

        execution_engine: ExecutionEngine = batch.data.execution_engine
        execution_engine.batch_manager.load_batch_list(batch_list=[batch])

        metrics: Dict[Tuple[str, str, str], MetricValue] = {}

        table_columns_metric: MetricConfiguration
        results: Dict[Tuple[str, str, str], MetricValue]

        table_columns_metric, results = get_table_columns_metric(execution_engine=execution_engine)
        metrics.update(results)

        metrics_calculator = MetricsCalculator(execution_engine=execution_engine)

        # define metric for the total number of rows
        row_count_metric = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs={"batch_id": batch.id},
            metric_value_kwargs=None,
        )
        total_row_count: int = metrics_calculator.get_metric(metric=row_count_metric)

        # get the expected column names
        expected_columns: set[str] = set(metrics_calculator.columns())

        head_data: HeadData
        if n_rows is not None and not fetch_all:
            if n_rows < 0 or abs(n_rows) > total_row_count:
                n_rows = 0

            head_data = batch.head(n_rows=n_rows, fetch_all=fetch_all)
            assert isinstance(head_data, HeadData)

            head_data_row_count: int = len(head_data.data.index)

            # if n_rows is between 0 and the total_row_count, that's how many rows we expect
            if 0 <= n_rows <= total_row_count:
                assert head_data_row_count == n_rows
            # if n_rows is greater than the total_row_count, we only expect total_row_count rows
            elif n_rows > total_row_count:
                assert head_data_row_count == total_row_count
            # if n_rows is negative and abs(n_rows) is larger than total_row_count we expect zero rows  # noqa: E501
            elif n_rows < 0 and abs(n_rows) > total_row_count:
                assert head_data_row_count == 0
            # if n_rows is negative, we expect all but the final abs(n_rows)
            else:
                assert head_data_row_count == n_rows + total_row_count
        elif fetch_all:
            total_row_count = metrics_calculator.get_metric(metric=row_count_metric)
            if n_rows:
                head_data = batch.head(n_rows=n_rows, fetch_all=fetch_all)
            else:
                head_data = batch.head(fetch_all=fetch_all)
            assert isinstance(head_data, HeadData)
            assert len(head_data.data.index) == total_row_count
        else:
            # default to 5 rows
            head_data = batch.head(fetch_all=fetch_all)
            assert isinstance(head_data, HeadData)
            assert len(head_data.data.index) == 5

        assert set(metrics[table_columns_metric.id]) == expected_columns  # type: ignore[arg-type]

    else:
        with pytest.raises(ValidationError) as e:
            batch.head(n_rows=n_rows, fetch_all=fetch_all)  # type: ignore[arg-type]
        n_rows_validation_error = (
            "1 validation error for Head\n"
            "n_rows\n"
            "  value is not a valid integer (type=type_error.integer)"
        )
        fetch_all_validation_error = (
            "1 validation error for Head\n"
            "fetch_all\n"
            "  value is not a valid boolean (type=value_error.strictbool)"
        )
        assert n_rows_validation_error in str(e.value) or fetch_all_validation_error in str(e.value)
