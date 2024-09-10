from __future__ import annotations

import logging
from typing import Dict, Tuple

import pytest

from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import BatchRequest, PandasDatasource
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
    HeadData,
)
from great_expectations.exceptions.exceptions import DataContextError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator
from tests.expectations.test_util import get_table_columns_metric

logger = logging.getLogger(__name__)


def run_checkpoint_and_data_doc(
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
):
    # context, datasource, asset, batch_request
    context, datasource, asset, batch_request = datasource_test_data

    # Define an expectation suite
    suite_name = "my_suite"
    try:
        context.suites.add(ExpectationSuite(name=suite_name))
    except DataContextError:
        ...

    context.suites.get(name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    validator.expect_table_row_count_to_be_between(0, 10000)
    validator.expect_column_max_to_be_between(column="passenger_count", min_value=1, max_value=7)
    validator.expect_column_median_to_be_between(column="passenger_count", min_value=1, max_value=4)

    suite = validator.expectation_suite
    suite.save()
    batch_def = asset.add_batch_definition(name="my_batch_definition")

    # Configure and run a checkpoint
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="my_validation_definition", suite=suite, data=batch_def)
    )
    metadata = validator.active_batch.metadata  # type: ignore[union-attr] # active_batch could be None
    if isinstance(datasource, PandasDatasource):
        checkpoint_name = "single_batch_checkpoint"
    else:
        checkpoint_name = (
            f"batch_with_year_{metadata['year']}_month_{metadata['month']}_{suite_name}"
        )
    checkpoint = Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
    )
    checkpoint_result = checkpoint.run()

    # Verify checkpoint runs successfully
    assert checkpoint_result.success, "Running expectation suite failed"
    number_of_runs = len(checkpoint_result.run_results)
    assert number_of_runs == 1, f"{number_of_runs} runs were done when we only expected 1"

    # Grab the validation result and verify it is correct
    validation_result = checkpoint_result.run_results[list(checkpoint_result.run_results.keys())[0]]
    assert validation_result.success

    expected_metric_values = {
        "expect_table_row_count_to_be_between": {
            "value": 10000,
            "rendered_template": "Must have greater than or equal to $min_value and less than or equal to $max_value rows.",  # noqa: E501
        },
        "expect_column_max_to_be_between": {
            "value": 6,
            "rendered_template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
        },
        "expect_column_median_to_be_between": {
            "value": 1,
            "rendered_template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",  # noqa: E501
        },
    }
    assert len(validation_result.results) == 3

    for r in validation_result.results:
        assert r.success
        assert r.expectation_config
        assert (
            r.result["observed_value"] == expected_metric_values[r.expectation_config.type]["value"]
        )

        assert r.rendered_content is None
        assert r.expectation_config.rendered_content is None

    # Rudimentary test for data doc generation
    docs_dict = context.build_data_docs()
    assert "local_site" in docs_dict, "build_data_docs returned dictionary has changed"
    assert docs_dict["local_site"].startswith(
        "file://"
    ), "build_data_docs returns file path in unexpected form"
    path = docs_dict["local_site"][7:]
    with open(path) as f:
        data_doc_index = f.read()

    # Checking for ge-success-icon tests the result table was generated and it was populated with a successful run.  # noqa: E501
    assert "ge-success-icon" in data_doc_index
    assert "ge-failed-icon" not in data_doc_index


def run_batch_head(  # noqa: C901
    datasource_test_data: tuple[AbstractDataContext, Datasource, DataAsset, BatchRequest],
    fetch_all: bool | str,
    n_rows: int | float | str | None,  # noqa: PYI041
    success: bool,
) -> None:
    _, datasource, _, batch_request = datasource_test_data
    batch = datasource.get_batch(batch_request=batch_request)

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
