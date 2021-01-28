import pandas as pd

import great_expectations.expectations.metrics
from great_expectations.core import IDDict
from great_expectations.core.batch import Batch, BatchRequest, PartitionRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core import ExpectColumnMaxToBeBetween
from great_expectations.expectations.core.expect_column_value_z_scores_to_be_less_than import (
    ExpectColumnValueZScoresToBeLessThan,
)
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.validator.validation_graph import (
    MetricConfiguration,
    MetricEdge,
    ValidationGraph,
)
from great_expectations.validator.validator import Validator


def test_parse_validation_graph():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, engine)

        for metric_configuration in validation_dependencies["metrics"].values():
            Validator(execution_engine=engine).build_metric_dependency_graph(
                graph, metric_configuration, configuration, execution_engine=engine
            )
    ready_metrics, needed_metrics = Validator(engine)._parse_validation_graph(
        validation_graph=graph, metrics=dict()
    )

    assert len(ready_metrics) == 4 and len(needed_metrics) == 5


# Should be passing tests even if given incorrect MetricProvider data
def test_parse_validation_graph_with_bad_metrics_args():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    validator = Validator(execution_engine=engine)
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(
            configuration,
            execution_engine=engine,
        )

        for metric_configuration in validation_dependencies["metrics"].values():
            validator.build_metric_dependency_graph(
                graph, metric_configuration, configuration, execution_engine=engine
            )
    ready_metrics, needed_metrics = validator._parse_validation_graph(
        validation_graph=graph, metrics=("nonexistent", "NONE")
    )
    assert len(ready_metrics) == 4 and len(needed_metrics) == 5


def test_populate_dependencies():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(
            configuration,
            engine,
        )

        for metric_configuration in validation_dependencies["metrics"].values():
            Validator(execution_engine=engine).build_metric_dependency_graph(
                graph, metric_configuration, configuration, execution_engine=engine
            )
    assert len(graph.edges) == 10


def test_populate_dependencies_with_incorrect_metric_name():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(
            configuration,
            engine,
        )

        try:
            Validator(execution_engine=engine).build_metric_dependency_graph(
                graph,
                MetricConfiguration("column_values.not_a_metric", IDDict()),
                configuration,
                execution_engine=engine,
            )
        except MetricProviderError as e:
            graph = e

    assert isinstance(graph, MetricProviderError)


def test_graph_validate(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )

    batch = basic_datasource.get_single_batch_from_batch_request(
        BatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "batch_data": df,
                "partition_request": PartitionRequest(
                    **{
                        "partition_identifiers": {
                            "pipeline_stage_name": 0,
                            "airflow_run_id": 0,
                            "custom_key_0": 0,
                        }
                    }
                ),
            }
        )
    )

    result = Validator(
        execution_engine=PandasExecutionEngine(), batches=[batch]
    ).graph_validate(configurations=[expectationConfiguration])
    assert result == [
        ExpectationValidationResult(
            success=True,
            expectation_config=None,
            meta={},
            result={
                "element_count": 6,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "missing_count": 1,
                "missing_percent": 16.666666666666664,
                "unexpected_percent_nonmissing": 0.0,
            },
            exception_info=None,
        )
    ]


# this might indicate that we need to validate configuration a little more strictly prior to actually validating
def test_graph_validate_with_bad_config(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={"column": "not_in_table", "min_value": 1, "max_value": 29},
    )
    expectation = ExpectColumnMaxToBeBetween(expectationConfiguration)

    batch = basic_datasource.get_single_batch_from_batch_request(
        BatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "batch_data": df,
                "partition_request": PartitionRequest(
                    **{
                        "partition_identifiers": {
                            "pipeline_stage_name": 0,
                            "airflow_run_id": 0,
                            "custom_key_0": 0,
                        }
                    }
                ),
            }
        )
    )

    try:
        result = Validator(
            execution_engine=PandasExecutionEngine(), batches=[batch]
        ).graph_validate(configurations=[expectationConfiguration])
    except KeyError as e:
        result = e
    assert isinstance(result, KeyError)


# Tests that runtime configuration actually works during graph validation
def test_graph_validate_with_runtime_config(basic_datasource):
    df = pd.DataFrame(
        {"a": [1, 5, 22, 3, 5, 10, 2, 3], "b": [97, 332, 3, 4, 5, 6, 7, None]}
    )
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "b", "mostly": 1, "threshold": 2, "double_sided": True},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)

    batch = basic_datasource.get_single_batch_from_batch_request(
        BatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "batch_data": df,
                "partition_request": PartitionRequest(
                    **{
                        "partition_identifiers": {
                            "pipeline_stage_name": 0,
                            "airflow_run_id": 0,
                            "custom_key_0": 0,
                        }
                    }
                ),
            }
        )
    )

    try:
        result = Validator(
            execution_engine=PandasExecutionEngine(), batches=(batch,)
        ).graph_validate(
            configurations=[expectationConfiguration],
            runtime_configuration={"result_format": "COMPLETE"},
        )
    except AssertionError as e:
        result = e
    assert result == [
        ExpectationValidationResult(
            success=False,
            meta={},
            result={
                "element_count": 8,
                "unexpected_count": 1,
                "unexpected_percent": 12.5,
                "partial_unexpected_list": [332.0],
                "missing_count": 1,
                "missing_percent": 12.5,
                "unexpected_percent_nonmissing": 14.285714285714285,
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [{"value": 332.0, "count": 1}],
                "unexpected_list": [332.0],
                "unexpected_index_list": None,
            },
            expectation_config=None,
            exception_info=None,
        )
    ]


def test_validator_default_expectation_args__pandas(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )

    batch = basic_datasource.get_single_batch_from_batch_request(
        BatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "batch_data": df,
                "partition_request": PartitionRequest(
                    **{
                        "partition_identifiers": {
                            "pipeline_stage_name": 0,
                            "airflow_run_id": 0,
                            "custom_key_0": 0,
                        }
                    }
                ),
            }
        )
    )

    my_validator = Validator(execution_engine=PandasExecutionEngine(), batches=[batch])

    print(my_validator.get_default_expectation_arguments())


def test_validator_default_expectation_args__sql(
    data_context_with_sql_datasource_for_testing_get_batch,
):
    context = data_context_with_sql_datasource_for_testing_get_batch

    my_validator = context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        partition_identifiers={"date": "2020-01-15"},
        create_expectation_suite_with_name="test_suite",
    )

    print(my_validator.get_default_expectation_arguments())
