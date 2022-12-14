import os
import shutil
from typing import Any, Dict, List, Set, Union
from unittest import mock
from uuid import UUID

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    BatchDefinition,
    BatchMarkers,
    BatchRequest,
    RuntimeBatchRequest,
)
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.render import RenderedAtomicContent
from great_expectations.validator.validation_graph import ValidationGraph
from great_expectations.validator.validator import Validator


@pytest.fixture()
def yellow_trip_pandas_data_context(
    tmp_path_factory,
    monkeypatch,
) -> DataContext:
    """
    Provides a data context with a data_connector for a pandas datasource which can connect to three months of
    yellow trip taxi data in csv form. This data connector enables access to all three months through a BatchRequest
    where the "year" in batch_filter_parameters is set to "2019", or to individual months if the "month" in
    batch_filter_parameters is set to "01", "02", or "03"
    """
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "integration",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-03.csv"
            )
        ),
    )

    context = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.fixture()
def multi_batch_taxi_validator_ge_cloud_mode(
    yellow_trip_pandas_data_context,
) -> Validator:
    context: DataContext = yellow_trip_pandas_data_context
    context._cloud_mode = True

    suite = ExpectationSuite(
        expectation_suite_name="validating_taxi_data",
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "passenger_count",
                    "min_value": 0,
                    "max_value": 99,
                    "result_format": "BASIC",
                },
                meta={"notes": "This is an expectation."},
                ge_cloud_id=UUID("0faf94a9-f53a-41fb-8e94-32f218d4a774"),
            )
        ],
        data_context=context,
        meta={"notes": "This is an expectation suite."},
    )

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


@mock.patch(
    "great_expectations.data_context.data_context.BaseDataContext.save_expectation_suite"
)
@mock.patch(
    "great_expectations.data_context.data_context.BaseDataContext.get_expectation_suite"
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.cloud
@pytest.mark.integration
def test_ge_cloud_validator_updates_self_suite_with_ge_cloud_ids_on_save(
    mock_emit,
    mock_context_get_suite,
    mock_context_save_suite,
    multi_batch_taxi_validator_ge_cloud_mode,
    empty_data_context_stats_enabled,
):
    """
    This checks that Validator in ge_cloud_mode properly updates underlying Expectation Suite on save.
    The multi_batch_taxi_validator_ge_cloud_mode fixture has a suite with a single expectation.
    :param mock_context_get_suite: Under normal circumstances, this would be ExpectationSuite object returned from GX Cloud
    :param mock_context_save_suite: Under normal circumstances, this would trigger post or patch to GX Cloud
    """
    context: DataContext = empty_data_context_stats_enabled
    mock_suite = ExpectationSuite(
        expectation_suite_name="validating_taxi_data",
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "passenger_count", "min_value": 0, "max_value": 99},
                meta={"notes": "This is an expectation."},
                ge_cloud_id=UUID("0faf94a9-f53a-41fb-8e94-32f218d4a774"),
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "trip_distance", "min_value": 11, "max_value": 22},
                meta={"notes": "This is an expectation."},
                ge_cloud_id=UUID("3e8eee33-b425-4b36-a831-6e9dd31ad5af"),
            ),
        ],
        data_context=context,
        meta={"notes": "This is an expectation suite."},
    )
    mock_context_save_suite.return_value = True
    mock_context_get_suite.return_value = mock_suite
    multi_batch_taxi_validator_ge_cloud_mode.expect_column_values_to_be_between(
        column="trip_distance", min_value=11, max_value=22
    )
    multi_batch_taxi_validator_ge_cloud_mode.save_expectation_suite()
    assert (
        multi_batch_taxi_validator_ge_cloud_mode.get_expectation_suite().to_json_dict()
        == mock_suite.to_json_dict()
    )

    # add_expectation() will not send usage_statistics event when called from a Validator
    assert mock_emit.call_count == 0
    assert mock_emit.call_args_list == []


@pytest.mark.integration
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
        "0327cfb13205ec8512e1c28e438ab43b",
        "0808e185a52825d22356de2fe00a8f5f",
        "90bb41c1fbd7c71c05dbc8695320af71",
    ]


@pytest.mark.integration
def test_validator_with_bad_batchrequest(
    yellow_trip_pandas_data_context,
):
    context: DataContext = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    multi_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="i_dont_exist",
        data_connector_query={"batch_filter_parameters": {"year": "2019"}},
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # noinspection PyUnusedLocal
        validator_multi_batch: Validator = context.get_validator(
            batch_request=multi_batch_request, expectation_suite=suite
        )


@pytest.mark.integration
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
    assert jan_batch_definition_list[0]["id"] == "0327cfb13205ec8512e1c28e438ab43b"

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

    # Filter using limit param
    limit_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={"limit": 2}
    )

    limit_batch_filter_definition_list: List[
        BatchDefinition
    ] = limit_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )

    assert len(limit_batch_filter_definition_list) == 2
    assert limit_batch_filter_definition_list[0]["batch_identifiers"]["month"] == "01"
    assert (
        limit_batch_filter_definition_list[0]["id"]
        == "0327cfb13205ec8512e1c28e438ab43b"
    )
    assert limit_batch_filter_definition_list[1]["batch_identifiers"]["month"] == "02"
    assert (
        limit_batch_filter_definition_list[1]["id"]
        == "0808e185a52825d22356de2fe00a8f5f"
    )


@pytest.mark.integration
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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.integration
def test_adding_expectation_to_validator_not_send_usage_message(
    mock_emit, multi_batch_taxi_validator
):
    """
    What does this test and why?

    When an Expectation is called using a Validator, it validates the dataset using the implementation of
    the Expectation. As part of the process, it also adds the Expectation to the active
    ExpectationSuite. This test ensures that this in-direct way of adding an Expectation to the ExpectationSuite
    (ie not calling add_expectations() directly) does not emit a usage_stats event.
    """
    multi_batch_taxi_validator.expect_column_values_to_be_between(
        column="trip_distance", min_value=11, max_value=22
    )
    assert mock_emit.call_count == 0
    assert mock_emit.call_args_list == []


@pytest.mark.integration
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
    assert validator.active_batch_id == "0327cfb13205ec8512e1c28e438ab43b"

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
    validator.load_batch_list(batch_list=new_batch)

    updated_batch_markers: BatchMarkers = validator.active_batch_markers
    assert (
        updated_batch_markers["pandas_data_fingerprint"]
        == "88b447d903f05fb594b87b13de399e45"
    )

    assert len(validator.batches) == 2
    assert validator.active_batch_id == "0808e185a52825d22356de2fe00a8f5f"
    assert first_batch_markers != updated_batch_markers


@pytest.mark.integration
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
        "No more than one of batch, batch_list, batch_request, or batch_request_list can be specified",
    )


@pytest.mark.integration
def test_graph_validate(in_memory_runtime_context, basic_datasource):
    in_memory_runtime_context.datasources["my_datasource"] = basic_datasource
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
        execution_engine=basic_datasource.execution_engine,
        data_context=in_memory_runtime_context,
        batches=[batch],
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


# Tests that runtime configuration actually works during graph validation
@pytest.mark.integration
def test_graph_validate_with_runtime_config(
    in_memory_runtime_context, basic_datasource
):
    in_memory_runtime_context.datasources["my_datasource"] = basic_datasource
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
        # noinspection PyTypeChecker
        result = Validator(
            execution_engine=basic_datasource.execution_engine,
            data_context=in_memory_runtime_context,
            batches=[batch],
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


@pytest.mark.integration
def test_graph_validate_with_exception(basic_datasource):
    # noinspection PyUnusedLocal
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

    execution_engine = PandasExecutionEngine()
    validator = Validator(execution_engine=execution_engine, batches=[batch])
    graph = ValidationGraph(execution_engine=execution_engine)
    graph.build_metric_dependency_graph = mock_error

    result = validator.graph_validate(configurations=[expectation_configuration])

    assert len(result) == 1
    assert result[0].expectation_config is not None


@pytest.mark.integration
def test_graph_validate_with_bad_config_catch_exceptions_false(
    in_memory_runtime_context, basic_datasource
):
    in_memory_runtime_context.datasources["my_datasource"] = basic_datasource
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
    with pytest.raises(
        tuple(
            [ge_exceptions.MetricResolutionError, ge_exceptions.ProfilerExecutionError]
        )
    ) as eee:
        # noinspection PyUnusedLocal
        result = Validator(
            execution_engine=basic_datasource.execution_engine,
            data_context=in_memory_runtime_context,
            batches=[batch],
        ).graph_validate(
            configurations=[expectation_configuration],
            runtime_configuration={
                "catch_exceptions": False,
                "result_format": {"result_format": "BASIC"},
            },
        )
    assert (
        str(eee.value)
        == 'Error: The column "not_in_table" in BatchData does not exist.'
    )


@pytest.mark.integration
def test_validate_expectation(multi_batch_taxi_validator):
    validator: Validator = multi_batch_taxi_validator
    expect_column_values_to_be_between_config = validator.validate_expectation(
        "expect_column_values_to_be_between"
    )("passenger_count", 0, 5).expectation_config.kwargs
    assert expect_column_values_to_be_between_config == {
        "column": "passenger_count",
        "min_value": 0,
        "max_value": 5,
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }

    expect_column_values_to_be_of_type_config = validator.validate_expectation(
        "expect_column_values_to_be_of_type"
    )("passenger_count", "int").expectation_config.kwargs

    assert expect_column_values_to_be_of_type_config == {
        "column": "passenger_count",
        "type_": "int",
        "batch_id": "90bb41c1fbd7c71c05dbc8695320af71",
    }


@pytest.mark.integration
def test_validator_docstrings(multi_batch_taxi_validator):
    expectation_impl = getattr(
        multi_batch_taxi_validator, "expect_column_values_to_be_in_set", None
    )
    assert expectation_impl.__doc__.startswith(
        "Expect each column value to be in a given set"
    )


@pytest.mark.integration
def test_validator_include_rendered_content(
    yellow_trip_pandas_data_context,
):
    context: DataContext = yellow_trip_pandas_data_context
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
    )
    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    column: str = "fare_amount"
    partition_object: Dict[str, List[float]] = {
        "values": [1.0, 2.0],
        "weights": [0.3, 0.7],
    }

    validator_exclude_rendered_content: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )
    validation_result: ExpectationValidationResult = (
        validator_exclude_rendered_content.expect_column_kl_divergence_to_be_less_than(
            column=column,
            partition_object=partition_object,
        )
    )
    assert validation_result.expectation_config.rendered_content is None
    assert validation_result.rendered_content is None

    validator_include_rendered_content: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
        include_rendered_content=True,
    )
    validation_result: ExpectationValidationResult = (
        validator_include_rendered_content.expect_column_kl_divergence_to_be_less_than(
            column=column,
            partition_object=partition_object,
        )
    )
    assert len(validation_result.expectation_config.rendered_content) == 1
    assert isinstance(
        validation_result.expectation_config.rendered_content[0], RenderedAtomicContent
    )
    assert len(validation_result.rendered_content) == 1
    assert isinstance(validation_result.rendered_content[0], RenderedAtomicContent)


@pytest.mark.integration
def test_validator_include_rendered_content_evaluation_parameters(
    yellow_trip_pandas_data_context,
):
    context: DataContext = yellow_trip_pandas_data_context
    batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
    )
    suite: ExpectationSuite = context.create_expectation_suite("validating_taxi_data")

    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
        include_rendered_content=True,
    )

    validator.set_evaluation_parameter("upstream_row_count", 10000)

    validation_result: ExpectationValidationResult = (
        validator.expect_table_row_count_to_equal(
            value={"$PARAMETER": "upstream_row_count"},
            result_format={"result_format": "BOOLEAN_ONLY"},
        )
    )

    assert (
        validation_result.expectation_config.rendered_content[0].value.params["value"][
            "value"
        ]
        == 10000
    )

    validator.set_evaluation_parameter("upstream_row_count", 8000)

    validation_result: ExpectationValidationResult = (
        validator.expect_table_row_count_to_equal(
            value={"$PARAMETER": "upstream_row_count"},
            result_format={"result_format": "BOOLEAN_ONLY"},
        )
    )

    assert (
        validation_result.expectation_config.rendered_content[0].value.params["value"][
            "value"
        ]
        == 8000
    )


@pytest.fixture
def validator_with_mock_execution_engine() -> Validator:
    execution_engine = mock.MagicMock()
    validator = Validator(execution_engine=execution_engine)
    return validator


@pytest.mark.unit
def test___dir___contains_expectation_impls(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    attrs = validator.__dir__()
    expectation_impls = list(filter(lambda a: a.startswith("expect_"), attrs))
    assert len(expectation_impls) > 0


@pytest.mark.unit
def test_show_progress_bars_property_and_setter(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    assert validator.metrics_calculator.show_progress_bars is True
    validator.metrics_calculator.show_progress_bars = False
    assert validator.metrics_calculator.show_progress_bars is False


@pytest.mark.unit
def test_expose_dataframe_methods_property_and_setter(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    assert validator.expose_dataframe_methods is False
    validator.expose_dataframe_methods = True
    assert validator.expose_dataframe_methods is True


@pytest.mark.unit
def test___get_attr___retrieves_existing_expectation(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    # Does not raise error if properly registered
    # Avoiding invocation to only test registration (and not actual expectation)
    validator.expect_column_max_to_be_between


@pytest.mark.unit
def test__get_attr___raises_attribute_error_with_invalid_attr(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    with pytest.raises(AttributeError) as e:
        validator.my_fake_attr

    assert "'Validator'  object has no attribute 'my_fake_attr'" in str(e.value)


@pytest.mark.unit
def test_list_available_expectation_types(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    available = validator.list_available_expectation_types()
    assert all(e.startswith("expect_") for e in available)
