from typing import Any, Dict, List, Set, Union

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchMarkers,
    BatchRequest,
    IDDict,
    RuntimeBatchRequest,
)
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_value_z_scores_to_be_less_than import (
    ExpectColumnValueZScoresToBeLessThan,
)
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.validator.validation_graph import (
    MetricConfiguration,
    ValidationGraph,
)
from great_expectations.validator.validator import Validator


def test_parse_validation_graph():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    # noinspection PyUnusedLocal
    expectation = ExpectColumnValueZScoresToBeLessThan(expectation_configuration)
    # noinspection PyUnusedLocal
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectation_configuration]:
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
    assert len(ready_metrics) == 2 and len(needed_metrics) == 9


# Should be passing tests even if given incorrect MetricProvider data
def test_parse_validation_graph_with_bad_metrics_args():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectation_configuration = ExpectationConfiguration(
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
    for configuration in [expectation_configuration]:
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
    assert len(ready_metrics) == 2 and len(needed_metrics) == 9


def test_populate_dependencies():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    # noinspection PyUnusedLocal
    expectation = ExpectColumnValueZScoresToBeLessThan(expectation_configuration)
    # noinspection PyUnusedLocal
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectation_configuration]:
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
    assert len(graph.edges) == 17


def test_populate_dependencies_with_incorrect_metric_name():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    # noinspection PyUnusedLocal
    expectation = ExpectColumnValueZScoresToBeLessThan(expectation_configuration)
    # noinspection PyUnusedLocal
    batch = Batch(data=df)
    graph = ValidationGraph()
    engine = PandasExecutionEngine()
    for configuration in [expectation_configuration]:
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
        except ge_exceptions.MetricProviderError as e:
            graph = e

    assert isinstance(graph, ge_exceptions.MetricProviderError)


def test_graph_validate(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    result = Validator(
        execution_engine=PandasExecutionEngine(), batches=[batch]
    ).graph_validate(configurations=[expectation_configuration])
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


def test_graph_validate_with_exception(basic_datasource):
    def mock_error(*args, **kwargs):
        raise Exception("Mock Error")

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )

    validator = Validator(execution_engine=PandasExecutionEngine(), batches=[batch])
    validator.build_metric_dependency_graph = mock_error

    result = validator.graph_validate(configurations=[expectation_configuration])

    assert len(result) == 1
    assert result[0].expectation_config is not None


def test_graph_validate_with_bad_config(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={"column": "not_in_table", "min_value": 1, "max_value": 29},
    )
    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        result = Validator(
            execution_engine=PandasExecutionEngine(), batches=[batch]
        ).graph_validate(configurations=[expectation_configuration])
    assert (
        str(eee.value)
        == 'Error: The column "not_in_table" in BatchData does not exist.'
    )


# Tests that runtime configuration actually works during graph validation
def test_graph_validate_with_runtime_config(basic_datasource):
    df = pd.DataFrame(
        {"a": [1, 5, 22, 3, 5, 10, 2, 3], "b": [97, 332, 3, 4, 5, 6, 7, None]}
    )

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "b", "mostly": 1, "threshold": 2, "double_sided": True},
    )
    try:
        result = Validator(
            execution_engine=PandasExecutionEngine(), batches=(batch,)
        ).graph_validate(
            configurations=[expectation_configuration],
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
                "unexpected_percent": 14.285714285714285,
                "partial_unexpected_list": [332.0],
                "missing_count": 1,
                "missing_percent": 12.5,
                "unexpected_percent_total": 12.5,
                "unexpected_percent_nonmissing": 14.285714285714285,
                "partial_unexpected_index_list": [1],
                "partial_unexpected_counts": [{"value": 332.0, "count": 1}],
                "unexpected_list": [332.0],
                "unexpected_index_list": [1],
            },
            expectation_config={
                "expectation_type": "expect_column_value_z_scores_to_be_less_than",
                "kwargs": {
                    "column": "b",
                    "mostly": 1,
                    "threshold": 2,
                    "double_sided": True,
                },
                "meta": {},
            },
            exception_info=None,
        )
    ]


def test_validator_default_expectation_args__pandas(basic_datasource):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    my_validator = Validator(execution_engine=PandasExecutionEngine(), batches=[batch])

    print(my_validator.get_default_expectation_arguments())


def test_validator_default_expectation_args__sql(
    data_context_with_simple_sql_datasource_for_testing_get_batch,
):
    context = data_context_with_simple_sql_datasource_for_testing_get_batch

    my_validator = context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        batch_identifiers={"date": "2020-01-15"},
        create_expectation_suite_with_name="test_suite",
    )

    print(my_validator.get_default_expectation_arguments())

    with pytest.raises(ge_exceptions.InvalidDataContextKeyError):
        # expectation_suite_name is a number not str
        # noinspection PyUnusedLocal
        my_validator = context.get_validator(
            datasource_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            batch_identifiers={"date": "2020-01-15"},
            expectation_suite_name=1,
        )

    with pytest.raises(TypeError):
        # expectation_suite is a string not an ExpectationSuite
        # noinspection PyUnusedLocal
        my_validator = context.get_validator(
            datasource_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            batch_identifiers={"date": "2020-01-15"},
            expectation_suite="I_am_not_an_expectation_suite",
        )


def test_columns(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    batch_request: Dict[str, Union[str, Dict[str, Any]]] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }
    validator: Validator = data_context.get_validator(
        batch_request=BatchRequest(**batch_request),
        create_expectation_suite_with_name="warning",
    )
    columns: List[str] = validator.columns()

    expected: List[str] = [
        "Unnamed: 0",
        "Name",
        "PClass",
        "Age",
        "Sex",
        "Survived",
        "SexCode",
    ]
    assert columns == expected


def test_head(
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates
    batch_request: Dict[str, Union[str, Dict[str, Any]]] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }
    validator: Validator = data_context.get_validator(
        batch_request=BatchRequest(**batch_request),
        create_expectation_suite_with_name="warning",
    )
    head: pd.DataFrame = validator.head()

    expected: Dict[str, Dict[int, Union[int, str]]] = {
        "Unnamed: 0": {0: 1, 1: 2, 2: 3, 3: 4, 4: 5},
        "Name": {
            0: "Allen, Miss Elisabeth Walton",
            1: "Allison, Miss Helen Loraine",
            2: "Allison, Mr Hudson Joshua Creighton",
            3: "Allison, Mrs Hudson JC (Bessie Waldo Daniels)",
            4: "Allison, Master Hudson Trevor",
        },
        "PClass": {0: "1st", 1: "1st", 2: "1st", 3: "1st", 4: "1st"},
        "Age": {0: 29.0, 1: 2.0, 2: 30.0, 3: 25.0, 4: 0.92},
        "Sex": {0: "female", 1: "female", 2: "male", 3: "female", 4: "male"},
        "Survived": {0: 1, 1: 0, 2: 0, 3: 0, 4: 1},
        "SexCode": {0: 1, 1: 1, 2: 0, 3: 1, 4: 0},
    }
    assert head.to_dict() == expected


@pytest.fixture()
def multi_batch_taxi_validator(
    yellow_trip_pandas_data_context,
) -> Validator:
    context: DataContext = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    multi_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"year": "2019"}},
    )

    validator_multi_batch: Validator = context.get_validator(
        batch_request=multi_batch_request, expectation_suite=suite
    )

    return validator_multi_batch


def test_validator_can_instantiate_with_a_multi_batch_request(
    multi_batch_taxi_validator,
):
    assert len(multi_batch_taxi_validator.batches) == 3
    assert (
        multi_batch_taxi_validator.active_batch.batch_definition.batch_identifiers[
            "month"
        ]
        == "03"
    )

    validator_batch_identifiers_for_all_batches: List[str] = [
        i for i in multi_batch_taxi_validator.batches
    ]
    assert validator_batch_identifiers_for_all_batches == [
        "18653cbf8fb5baf5fbbc5ed95f9ee94d",
        "92bcffc67c34a1c9a67e0062ed4a9529",
        "021563e94d7866f395288f6e306aed9b",
    ]


def test_validator_batch_filter(
    multi_batch_taxi_validator,
):
    total_batch_definition_list: List[BatchDefinition] = [
        v.batch_definition for k, v in multi_batch_taxi_validator.batches.items()
    ]

    jan_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={"batch_filter_parameters": {"month": "01"}}
    )

    jan_batch_definition_list: List[
        BatchDefinition
    ] = jan_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )

    assert len(jan_batch_definition_list) == 1
    assert jan_batch_definition_list[0]["batch_identifiers"]["month"] == "01"
    assert jan_batch_definition_list[0]["id"] == "18653cbf8fb5baf5fbbc5ed95f9ee94d"

    feb_march_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={"index": slice(-1, 0, -1)}
    )

    feb_march_batch_definition_list: List[
        BatchDefinition
    ] = feb_march_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )

    for i in feb_march_batch_definition_list:
        print(i["batch_identifiers"])
    assert len(feb_march_batch_definition_list) == 2

    batch_definitions_months_set: Set[str] = {
        v.batch_identifiers["month"] for v in feb_march_batch_definition_list
    }
    assert batch_definitions_months_set == {"02", "03"}

    jan_march_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={
            "custom_filter_function": lambda batch_identifiers: batch_identifiers[
                "month"
            ]
            == "01"
            or batch_identifiers["month"] == "03"
        }
    )

    jan_march_batch_definition_list: List[
        BatchDefinition
    ] = jan_march_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )

    for i in jan_march_batch_definition_list:
        print(i["batch_identifiers"])
    assert len(jan_march_batch_definition_list) == 2

    batch_definitions_months_set: Set[str] = {
        v.batch_identifiers["month"] for v in jan_march_batch_definition_list
    }
    assert batch_definitions_months_set == {"01", "03"}


def test_custom_filter_function(
    multi_batch_taxi_validator,
):
    total_batch_definition_list: List[BatchDefinition] = [
        v.batch_definition for k, v in multi_batch_taxi_validator.batches.items()
    ]
    assert len(total_batch_definition_list) == 3

    # Filter to all batch_definitions prior to March
    jan_feb_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={
            "custom_filter_function": lambda batch_identifiers: int(
                batch_identifiers["month"]
            )
            < 3
        }
    )
    jan_feb_batch_definition_list: list = (
        jan_feb_batch_filter.select_from_data_connector_query(
            batch_definition_list=total_batch_definition_list
        )
    )
    assert len(jan_feb_batch_definition_list) == 2
    batch_definitions_months_set: Set[str] = {
        v.batch_identifiers["month"] for v in jan_feb_batch_definition_list
    }
    assert batch_definitions_months_set == {"01", "02"}


def test_validator_set_active_batch(
    multi_batch_taxi_validator,
):
    jan_min_date = "2019-01-01"
    mar_min_date = "2019-03-01"
    assert (
        multi_batch_taxi_validator.active_batch_id == "021563e94d7866f395288f6e306aed9b"
    )
    assert multi_batch_taxi_validator.expect_column_values_to_be_between(
        "pickup_datetime", min_value=mar_min_date, parse_strings_as_datetimes=True
    ).success

    multi_batch_taxi_validator.active_batch_id = "18653cbf8fb5baf5fbbc5ed95f9ee94d"

    assert (
        multi_batch_taxi_validator.active_batch_id == "18653cbf8fb5baf5fbbc5ed95f9ee94d"
    )
    assert not multi_batch_taxi_validator.expect_column_values_to_be_between(
        "pickup_datetime", min_value=mar_min_date, parse_strings_as_datetimes=True
    ).success
    assert multi_batch_taxi_validator.expect_column_values_to_be_between(
        "pickup_datetime", min_value=jan_min_date, parse_strings_as_datetimes=True
    ).success


def test_validator_load_additional_batch_to_validator(
    yellow_trip_pandas_data_context,
):
    context: DataContext = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    jan_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"month": "01"}},
    )

    validator: Validator = context.get_validator(
        batch_request=jan_batch_request, expectation_suite=suite
    )

    assert len(validator.batches) == 1
    assert validator.active_batch_id == "18653cbf8fb5baf5fbbc5ed95f9ee94d"

    first_batch_markers: BatchMarkers = validator.active_batch_markers
    assert (
        first_batch_markers["pandas_data_fingerprint"]
        == "c4f929e6d4fab001fedc9e075bf4b612"
    )

    feb_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"month": "02"}},
    )

    new_batch = context.get_batch_list(batch_request=feb_batch_request)
    validator.load_batch(batch_list=new_batch)

    updated_batch_markers: BatchMarkers = validator.active_batch_markers
    assert (
        updated_batch_markers["pandas_data_fingerprint"]
        == "88b447d903f05fb594b87b13de399e45"
    )

    assert len(validator.batches) == 2
    assert validator.active_batch_id == "92bcffc67c34a1c9a67e0062ed4a9529"
    assert first_batch_markers != updated_batch_markers


def test_instantiate_validator_with_a_list_of_batch_requests(
    yellow_trip_pandas_data_context,
):
    context: DataContext = yellow_trip_pandas_data_context
    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    jan_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"month": "01"}},
    )

    feb_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"month": "02"}},
    )

    validator_two_batch_requests_two_batches: Validator = context.get_validator(
        batch_request_list=[jan_batch_request, feb_batch_request],
        expectation_suite=suite,
    )

    # Instantiate a validator with a single BatchRequest yielding two batches for testing
    jan_feb_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"index": "0:2"},
    )

    validator_one_batch_request_two_batches: Validator = context.get_validator(
        batch_request=jan_feb_batch_request, expectation_suite=suite
    )

    assert (
        validator_one_batch_request_two_batches.batches.keys()
        == validator_two_batch_requests_two_batches.batches.keys()
    )
    assert (
        validator_one_batch_request_two_batches.active_batch_id
        == validator_two_batch_requests_two_batches.active_batch_id
    )

    with pytest.raises(ValueError) as ve:
        # noinspection PyUnusedLocal
        validator: Validator = context.get_validator(
            batch_request=jan_feb_batch_request,
            batch_request_list=[jan_batch_request, feb_batch_request],
            expectation_suite=suite,
        )
    assert ve.value.args == (
        "Only one of batch_request or batch_request_list may be specified",
    )
