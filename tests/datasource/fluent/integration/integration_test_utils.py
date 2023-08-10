from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Tuple

import pytest
from pydantic import ValidationError

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import BatchRequest, PandasDatasource
from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
    HeadData,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator
from tests.expectations.test_util import get_table_columns_metric

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch


logger = logging.getLogger(__name__)


def run_checkpoint_and_data_doc(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    include_rendered_content: bool,
):
    # context, datasource, asset, batch_request
    context, datasource, asset, batch_request = datasource_test_data
    if include_rendered_content:
        context.variables.include_rendered_content.globally = True

    # Define an expectation suite
    suite_name = "my_suite"
    context.add_expectation_suite(expectation_suite_name=suite_name)
    # noinspection PyTypeChecker
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    validator.expect_table_row_count_to_be_between(0, 10000)
    validator.expect_column_max_to_be_between(
        column="passenger_count", min_value=1, max_value=7
    )
    validator.expect_column_median_to_be_between(
        column="passenger_count", min_value=1, max_value=4
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Configure and run a checkpoint
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {"batch_request": batch_request, "expectation_suite_name": suite_name}
        ],
    }
    metadata = validator.active_batch.metadata  # type: ignore[union-attr] # active_batch could be None
    if isinstance(datasource, PandasDatasource):
        checkpoint_name = "single_batch_checkpoint"
    else:
        checkpoint_name = (
            f"batch_with_year_{metadata['year']}_month_{metadata['month']}_{suite_name}"
        )
    checkpoint = SimpleCheckpoint(
        checkpoint_name,
        context,
        **checkpoint_config,  # type: ignore[arg-type]
    )
    checkpoint_result = checkpoint.run()

    # Verify checkpoint runs successfully
    assert checkpoint_result._success, "Running expectation suite failed"
    number_of_runs = len(checkpoint_result.run_results)
    assert (
        number_of_runs == 1
    ), f"{number_of_runs} runs were done when we only expected 1"

    # Grab the validation result and verify it is correct
    result = checkpoint_result["run_results"][
        list(checkpoint_result["run_results"].keys())[0]
    ]
    validation_result = result["validation_result"]
    assert validation_result.success

    expected_metric_values = {
        "expect_table_row_count_to_be_between": {
            "value": 10000,
            "rendered_template": "Must have greater than or equal to $min_value and less than or equal to $max_value rows.",
        },
        "expect_column_max_to_be_between": {
            "value": 6,
            "rendered_template": "$column maximum value must be greater than or equal to $min_value and less than or equal to $max_value.",
        },
        "expect_column_median_to_be_between": {
            "value": 1,
            "rendered_template": "$column median must be greater than or equal to $min_value and less than or equal to $max_value.",
        },
    }
    assert len(validation_result.results) == 3

    for r in validation_result.results:
        assert r.success
        assert (
            r.result["observed_value"]
            == expected_metric_values[r.expectation_config.expectation_type]["value"]
        )

        if include_rendered_content:
            # There is a prescriptive atomic renderer on r.expectation_config
            num_prescriptive_renderer = len(r.expectation_config.rendered_content)
            assert (
                num_prescriptive_renderer == 1
            ), f"Expected exactly 1 rendered content, found {num_prescriptive_renderer}"
            rendered_content = r.expectation_config.rendered_content[0]
            assert rendered_content.name == AtomicPrescriptiveRendererType.SUMMARY
            assert (
                rendered_content.value.template
                == expected_metric_values[r.expectation_config.expectation_type][
                    "rendered_template"
                ]
            )

            # There is a diagnostic atomic renderer on r, a validation result result.
            num_diagnostic_render = len(r.rendered_content)
            assert (
                num_diagnostic_render == 1
            ), f"Expected 1 diagnostic renderer, found {num_diagnostic_render}"
            diagnostic_renderer = r.rendered_content[0]
            assert (
                diagnostic_renderer.name == AtomicDiagnosticRendererType.OBSERVED_VALUE
            )
            assert (
                diagnostic_renderer.value.schema["type"]
                == "com.superconductive.rendered.string"
            )
        else:
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

    # Checking for ge-success-icon tests the result table was generated and it was populated with a successful run.
    assert "ge-success-icon" in data_doc_index
    assert "ge-failed-icon" not in data_doc_index


def run_data_assistant_and_checkpoint(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ]
):
    context, ds, _, batch_request = datasource_test_data
    data_assistant_result, checkpoint_result = _configure_and_run_data_assistant(
        context, batch_request
    )
    batch_num = len(
        data_assistant_result._batch_id_to_batch_identifier_display_name_map  # type: ignore[arg-type] # could be None
    )
    assert batch_num == 1, f"Only expected 1 batch but found {batch_num}"

    # We assert the data assistant successfully generated expectations.
    # We don't care about the exact number since that may change as data assistants evolve.
    expectation_num = len(data_assistant_result.expectation_configurations)  # type: ignore[arg-type] # could be None
    assert checkpoint_result.success, "Running expectation suite failed"
    # Verify that the number of checkpoint validations is the number of expectations generated by the data assistant
    assert (
        len(
            checkpoint_result["run_results"][
                list(checkpoint_result["run_results"].keys())[0]
            ]["validation_result"].results
        )
        == expectation_num
    )


def run_multibatch_data_assistant_and_checkpoint(
    multibatch_datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ]
):
    context, ds, _, batch_request = multibatch_datasource_test_data
    data_assistant_result, checkpoint_result = _configure_and_run_data_assistant(
        context, batch_request
    )
    # Assert multiple batches were processed
    batch_num = len(
        data_assistant_result._batch_id_to_batch_identifier_display_name_map  # type: ignore[arg-type] # could be None
    )
    assert batch_num == 12, f"Expected exactly 12 batches but found {batch_num}"

    expectation_num = len(data_assistant_result.expectation_configurations)  # type: ignore[arg-type] # could be None
    """
    Exact number of "ExpectationConfiguration" objects, emitted by "DataAssistant" implementation depends on "Batch"
    schema (including, significantly, column types).  Different "ExecutionEngine" backends (even different versions
    thereof) infer underlying "Batch" schema using their specific mechanisms, resulting in varying results, produced by
    same "DataAssistant" (as "DataAssistant" employs "DomainBuilder", whose output dynamically depends on column types).

    In this test, using "PandasExecutionEngine" yields 109 "ExpectationConfiguration" objects in all environments.
    However, using "SparkDFExecutionEngine" yields 109 "ExpectationConfiguration" objects for some PySpark versions,
    while 111 "ExpectationConfiguration" objects for other PySpark versions.  Using "SqlAlchemyExecutionEngine" yields
    111 "ExpectationConfiguration" objects for all SQLAlchemy environments in test suite.  Hence, assertion is ">= 109".
    """
    assert expectation_num >= 109, f"{expectation_num} >= 109"

    assert checkpoint_result.success, "Running expectation suite failed"
    # Verify that the number of checkpoint validations is the number of expectations generated by the data assistant
    assert (
        len(
            checkpoint_result["run_results"][
                list(checkpoint_result["run_results"].keys())[0]
            ]["validation_result"].results
        )
        == expectation_num
    )


def run_batch_head(  # noqa: PLR0915
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    fetch_all: bool | str,
    n_rows: int | float | str | None,
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

        table_columns_metric, results = get_table_columns_metric(
            execution_engine=execution_engine
        )
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
            # if n_rows is negative and abs(n_rows) is larger than total_row_count we expect zero rows
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
        assert n_rows_validation_error in str(
            e.value
        ) or fetch_all_validation_error in str(e.value)


def _configure_and_run_data_assistant(
    context: AbstractDataContext,
    batch_request: BatchRequest,
) -> tuple[DataAssistantResult, CheckpointResult]:
    expectation_suite_name = "my_onboarding_assistant_suite"
    context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
    data_assistant_result = context.assistants.onboarding.run(
        batch_request=batch_request,
        numeric_columns_rule={
            "estimator": "exact",
            "random_seed": 2022080401,
        },
        # We exclude congestion_surcharge due to this bug:
        # https://greatexpectations.atlassian.net/browse/GREAT-1465
        exclude_column_names=["congestion_surcharge"],
    )
    expectation_suite = data_assistant_result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.add_or_update_expectation_suite(expectation_suite=expectation_suite)

    # Run a checkpoint
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    }
    checkpoint = SimpleCheckpoint(
        f"yellow_tripdata_sample_{expectation_suite_name}",
        context,
        **checkpoint_config,  # type: ignore[arg-type]
    )
    checkpoint_result = checkpoint.run()

    return data_assistant_result, checkpoint_result
