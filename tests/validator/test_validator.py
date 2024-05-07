from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING, List, Set, Tuple
from unittest import mock
from unittest.mock import ANY, patch

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
import great_expectations.expectations as gxe
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    BatchMarkers,
    BatchRequest,
    LegacyBatchDefinition,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.data_context import get_context
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import CheckpointValidationDefinition
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasource
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

DATASOURCE_NAME = "my_datasource"
DATA_ASSET_NAME = "IN_MEMORY_DATA_ASSET"


@pytest.fixture
def basic_datasource() -> PandasDatasource:
    context = get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("my_datasource")
    return datasource


@pytest.fixture()
def yellow_trip_pandas_data_context(
    tmp_path_factory,
    monkeypatch,
) -> FileDataContext:
    """
    Provides a data context with a data_connector for a pandas datasource which can connect to three
    months of yellow trip taxi data in csv form. This data connector enables access to all three
    months through a BatchRequest where the "year" in batch_filter_parameters is set to "2019", or
    to individual months if the "month" in batch_filter_parameters is set to "01", "02", or "03"
    """
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(  # noqa: PTH118
        project_path, FileDataContext.GX_DIR
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"),  # noqa: PTH118
        exist_ok=True,
    )
    data_path: str = os.path.join(context_path, "..", "data")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH118, PTH103
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "integration",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                FileDataContext._LEGACY_GX_DIR,
                FileDataContext.GX_YML,
            ),
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "yellow_tripdata_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "yellow_tripdata_sample_2019-03.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


@pytest.mark.big
def test_validator_default_expectation_args__pandas(basic_datasource: PandasDatasource):
    asset = basic_datasource.add_dataframe_asset(
        "my_asset",
        dataframe=pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]}),
    )
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    my_validator = Validator(execution_engine=PandasExecutionEngine(), batches=[batch])

    assert my_validator.get_default_expectation_arguments() == {
        "catch_exceptions": False,
        "result_format": ResultFormat.BASIC,
    }


@pytest.mark.big
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

    with pytest.raises(gx_exceptions.InvalidDataContextKeyError):
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


@pytest.fixture()
def multi_batch_taxi_validator(
    yellow_trip_pandas_data_context,
) -> Validator:
    context = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.suites.add(ExpectationSuite(name="validating_taxi_data"))

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


@pytest.mark.big
def test_validator_convert_to_checkpoint_validations_list(multi_batch_taxi_validator):
    validator = multi_batch_taxi_validator

    actual = validator.convert_to_checkpoint_validations_list()
    expected_config = CheckpointValidationDefinition(
        expectation_suite_name="validating_taxi_data",
        expectation_suite_id=ANY,
        batch_request={
            "datasource_name": "taxi_pandas",
            "data_connector_name": "monthly",
            "data_asset_name": "my_reports",
            "data_connector_query": {"batch_filter_parameters": {"year": "2019"}},
            "batch_spec_passthrough": None,
            "limit": None,
        },
        id=None,
        name=None,
    )
    assert all(config.to_dict() == expected_config.to_dict() for config in actual)


@pytest.fixture()
def multi_batch_taxi_validator_ge_cloud_mode(
    yellow_trip_pandas_data_context,
) -> Validator:
    context = yellow_trip_pandas_data_context
    context._cloud_mode = True

    suite = ExpectationSuite(
        name="validating_taxi_data",
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
                id="0faf94a9-f53a-41fb-8e94-32f218d4a774",
            )
        ],
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


# TODO: There is something wrong with this test. It is trying to mock out cloud, but if I don't
#       unset the gx_env_variables (eg if i remove this fixture), this test will fail.
@mock.patch("great_expectations.data_context.store.ExpectationsStore.update")
@mock.patch("great_expectations.validator.validator.Validator.cloud_mode")
@pytest.mark.cloud
def test_ge_cloud_validator_updates_self_suite_with_ge_cloud_ids_on_save(
    mock_cloud_mode,
    mock_expectation_store_update,
    mock_context_get_suite,
    mock_context_save_suite,
    unset_gx_env_variables,
    multi_batch_taxi_validator_ge_cloud_mode,
):
    """
    This checks that Validator in ge_cloud_mode properly updates underlying Expectation Suite on save.
    The multi_batch_taxi_validator_ge_cloud_mode fixture has a suite with a single expectation.
    :param mock_context_get_suite: Under normal circumstances, this would be ExpectationSuite object returned from GX Cloud
    :param mock_context_save_suite: Under normal circumstances, this would trigger post or patch to GX Cloud
    """  # noqa: E501
    mock_suite = ExpectationSuite(
        name="validating_taxi_data",
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "passenger_count", "min_value": 0, "max_value": 99},
                meta={"notes": "This is an expectation."},
                id="0faf94a9-f53a-41fb-8e94-32f218d4a774",
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "trip_distance", "min_value": 11, "max_value": 22},
                meta={"notes": "This is an expectation."},
                id="3e8eee33-b425-4b36-a831-6e9dd31ad5af",
            ),
        ],
        meta={"notes": "This is an expectation suite."},
    )
    mock_context_get_suite.return_value = mock_suite
    mock_cloud_mode.return_value = True
    mock_context_save_suite.return_value = True

    multi_batch_taxi_validator_ge_cloud_mode.expect_column_values_to_be_between(
        column="trip_distance", min_value=11, max_value=22
    )
    multi_batch_taxi_validator_ge_cloud_mode.save_expectation_suite()

    expected = mock_suite.to_json_dict()
    actual = multi_batch_taxi_validator_ge_cloud_mode.get_expectation_suite().to_json_dict()
    assert expected == actual


@pytest.mark.big
def test_validator_can_instantiate_with_a_multi_batch_request(
    multi_batch_taxi_validator,
):
    assert len(multi_batch_taxi_validator.batches) == 3
    assert (
        multi_batch_taxi_validator.active_batch.batch_definition.batch_identifiers["month"] == "03"
    )

    validator_batch_identifiers_for_all_batches: List[str] = [
        i for i in multi_batch_taxi_validator.batches
    ]
    assert validator_batch_identifiers_for_all_batches == [
        "0327cfb13205ec8512e1c28e438ab43b",
        "0808e185a52825d22356de2fe00a8f5f",
        "90bb41c1fbd7c71c05dbc8695320af71",
    ]


@pytest.mark.big
def test_validator_with_bad_batchrequest(
    yellow_trip_pandas_data_context,
):
    context = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.suites.add(ExpectationSuite(name="validating_taxi_data"))

    multi_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="i_dont_exist",
        data_connector_query={"batch_filter_parameters": {"year": "2019"}},
    )
    with pytest.raises(gx_exceptions.InvalidBatchRequestError):
        # noinspection PyUnusedLocal
        _: Validator = context.get_validator(
            batch_request=multi_batch_request, expectation_suite=suite
        )


@pytest.mark.big
def test_validator_batch_filter(
    multi_batch_taxi_validator,
):
    total_batch_definition_list: List[LegacyBatchDefinition] = [
        v.batch_definition for k, v in multi_batch_taxi_validator.batches.items()
    ]

    jan_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={"batch_filter_parameters": {"month": "01"}}
    )

    jan_batch_definition_list: List[LegacyBatchDefinition] = (
        jan_batch_filter.select_from_data_connector_query(
            batch_definition_list=total_batch_definition_list
        )
    )

    assert len(jan_batch_definition_list) == 1
    assert jan_batch_definition_list[0]["batch_identifiers"]["month"] == "01"
    assert jan_batch_definition_list[0]["id"] == "0327cfb13205ec8512e1c28e438ab43b"

    feb_march_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={"index": slice(-1, 0, -1)}
    )

    feb_march_batch_definition_list: List[LegacyBatchDefinition] = (
        feb_march_batch_filter.select_from_data_connector_query(
            batch_definition_list=total_batch_definition_list
        )
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
            "custom_filter_function": lambda batch_identifiers: batch_identifiers["month"] == "01"
            or batch_identifiers["month"] == "03"
        }
    )

    jan_march_batch_definition_list: List[LegacyBatchDefinition] = (
        jan_march_batch_filter.select_from_data_connector_query(
            batch_definition_list=total_batch_definition_list
        )
    )

    for i in jan_march_batch_definition_list:
        print(i["batch_identifiers"])
    assert len(jan_march_batch_definition_list) == 2

    batch_definitions_months_set: Set[str] = {
        v.batch_identifiers["month"] for v in jan_march_batch_definition_list
    }
    assert batch_definitions_months_set == {"01", "03"}

    # Filter using limit param
    limit_batch_filter: BatchFilter = build_batch_filter(data_connector_query_dict={"limit": 2})

    limit_batch_filter_definition_list: List[LegacyBatchDefinition] = (
        limit_batch_filter.select_from_data_connector_query(
            batch_definition_list=total_batch_definition_list
        )
    )

    assert len(limit_batch_filter_definition_list) == 2
    assert limit_batch_filter_definition_list[0]["batch_identifiers"]["month"] == "01"
    assert limit_batch_filter_definition_list[0]["id"] == "0327cfb13205ec8512e1c28e438ab43b"
    assert limit_batch_filter_definition_list[1]["batch_identifiers"]["month"] == "02"
    assert limit_batch_filter_definition_list[1]["id"] == "0808e185a52825d22356de2fe00a8f5f"


@pytest.mark.big
def test_custom_filter_function(
    multi_batch_taxi_validator,
):
    total_batch_definition_list: List[LegacyBatchDefinition] = [
        v.batch_definition for k, v in multi_batch_taxi_validator.batches.items()
    ]
    assert len(total_batch_definition_list) == 3

    # Filter to all batch_definitions prior to March
    jan_feb_batch_filter: BatchFilter = build_batch_filter(
        data_connector_query_dict={
            "custom_filter_function": lambda batch_identifiers: int(batch_identifiers["month"]) < 3
        }
    )
    jan_feb_batch_definition_list: list = jan_feb_batch_filter.select_from_data_connector_query(
        batch_definition_list=total_batch_definition_list
    )
    assert len(jan_feb_batch_definition_list) == 2
    batch_definitions_months_set: Set[str] = {
        v.batch_identifiers["month"] for v in jan_feb_batch_definition_list
    }
    assert batch_definitions_months_set == {"01", "02"}


@pytest.mark.big
def test_validator_load_additional_batch_to_validator(
    yellow_trip_pandas_data_context,
):
    context = yellow_trip_pandas_data_context

    suite: ExpectationSuite = context.suites.add(ExpectationSuite(name="validating_taxi_data"))

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
    assert first_batch_markers["pandas_data_fingerprint"] == "c4f929e6d4fab001fedc9e075bf4b612"

    feb_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_pandas",
        data_connector_name="monthly",
        data_asset_name="my_reports",
        data_connector_query={"batch_filter_parameters": {"month": "02"}},
    )

    new_batch = context.get_batch_list(batch_request=feb_batch_request)
    validator.load_batch_list(batch_list=new_batch)

    updated_batch_markers: BatchMarkers = validator.active_batch_markers
    assert updated_batch_markers["pandas_data_fingerprint"] == "88b447d903f05fb594b87b13de399e45"

    assert len(validator.batches) == 2
    assert validator.active_batch_id == "0808e185a52825d22356de2fe00a8f5f"
    assert first_batch_markers != updated_batch_markers


@pytest.mark.big
def test_instantiate_validator_with_a_list_of_batch_requests(
    yellow_trip_pandas_data_context,
):
    context = yellow_trip_pandas_data_context
    suite: ExpectationSuite = context.suites.add(ExpectationSuite(name="validating_taxi_data"))

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
        _: Validator = context.get_validator(
            batch_request=jan_feb_batch_request,
            batch_request_list=[jan_batch_request, feb_batch_request],
            expectation_suite=suite,
        )

    assert ve.value.args == (
        "No more than one of batch, batch_list, batch_request, or batch_request_list can be specified",  # noqa: E501
    )


@pytest.mark.big
def test_graph_validate(in_memory_runtime_context, basic_datasource: PandasDatasource):
    asset = basic_datasource.add_dataframe_asset(
        "my_asset",
        dataframe=pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]}),
    )
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "b",
            "mostly": 0.9,
            "threshold": 4.0,
            "double_sided": True,
        },
    )
    result = Validator(
        execution_engine=PandasExecutionEngine(),
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
@pytest.mark.big
def test_graph_validate_with_runtime_config(
    in_memory_runtime_context, basic_datasource: PandasDatasource
):
    asset = basic_datasource.add_dataframe_asset(
        "my_asset",
        dataframe=pd.DataFrame(
            {"a": [1, 5, 22, 3, 5, 10, 2, 3], "b": [97, 332, 3, 4, 5, 6, 7, None]}
        ),
    )
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "b", "mostly": 1.0, "threshold": 2.0, "double_sided": True},
    )
    result = Validator(
        execution_engine=PandasExecutionEngine(),
        data_context=in_memory_runtime_context,
        batches=[batch],
    ).graph_validate(
        configurations=[expectation_configuration],
        runtime_configuration={"result_format": "COMPLETE"},
    )

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
            expectation_config=gxe.ExpectColumnValueZScoresToBeLessThan(
                column="b",
                mostly=1.0,
                threshold=2.0,
                double_sided=True,
            ).configuration,
            exception_info=None,
        )
    ]


@pytest.mark.big
def test_graph_validate_with_exception(basic_datasource: PandasDatasource, mocker: MockerFixture):
    # noinspection PyUnusedLocal
    def mock_error(*args, **kwargs):
        raise Exception("Mock Error")  # noqa: TRY002

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    asset = basic_datasource.add_dataframe_asset("my_asset", dataframe=df)
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

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

    # TODO: Convert this to actually mock an exception being thrown
    # graph = ValidationGraph(execution_engine=execution_engine)
    # graph.build_metric_dependency_graph = mock_error  # type: ignore[method-assign]

    result = validator.graph_validate(configurations=[expectation_configuration])

    assert len(result) == 1
    assert result[0].expectation_config is not None


@pytest.mark.big
def test_graph_validate_with_bad_config_catch_exceptions_false(
    in_memory_runtime_context, basic_datasource: PandasDatasource
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    asset = basic_datasource.add_dataframe_asset("my_asset", dataframe=df)
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={"column": "not_in_table", "min_value": 1, "max_value": 29},
    )
    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        _ = Validator(
            execution_engine=PandasExecutionEngine(),
            data_context=in_memory_runtime_context,
            batches=[batch],
        ).graph_validate(
            configurations=[expectation_configuration],
            runtime_configuration={
                "catch_exceptions": False,
                "result_format": {"result_format": "BASIC"},
            },
        )
    assert str(eee.value) == 'Error: The column "not_in_table" in BatchData does not exist.'


@pytest.mark.big
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


@pytest.mark.big
def test_validator_docstrings(multi_batch_taxi_validator):
    expectation_impl = getattr(
        multi_batch_taxi_validator, "expect_column_values_to_be_in_set", None
    )
    assert expectation_impl.__doc__.startswith("Expect each column value to be in a given set")


@pytest.mark.parametrize(
    "value_set, expected",
    [
        (list(range(2)), False),  # value set won't pass for the actual data
        (list(range(5)), True),  # value set will pass for the actual data
    ],
)
@pytest.mark.big
def test_validator_validate_substitutes_suite_parameters(
    value_set: list[int],
    expected: bool,
):
    """Integration test to ensure suite parameters are respected when validating.
    The setup here is to provide very simple data, and a variety of evaluation_parameter inputs,
    just checking for result.success as a proxy for the evaluation_parameter being respected.
    """

    # Arrange
    context = get_context(mode="ephemeral")
    suite_name = "my_suite"
    column_name = "my_column"
    datasource = context.data_sources.add_pandas(name="my_datasource")
    asset = datasource.add_dataframe_asset(
        "my_asset", dataframe=pd.DataFrame({column_name: [0, 1, 2, 3, 4]})
    )
    suite = context.suites.add(ExpectationSuite(suite_name))
    suite.add_expectation(
        gxe.ExpectColumnDistinctValuesToBeInSet(
            column=column_name, value_set={"$PARAMETER": "value_set"}
        )
    )
    validator = context.get_validator(
        batch_request=asset.build_batch_request(),
        expectation_suite_name=suite_name,
    )

    # Act
    result = validator.validate(suite_parameters={"value_set": value_set})
    assert isinstance(result, ExpectationSuiteValidationResult)
    suite_parameters_used = result.results[0]["expectation_config"]["kwargs"]["value_set"]

    # Assert
    assert suite_parameters_used == value_set
    assert result.success == expected


@pytest.mark.unit
@pytest.mark.parametrize("result_format", ["BOOLEAN_ONLY", {"result_format": "BOOLEAN_ONLY"}])
def test_rendered_content_bool_only_respected(result_format: str | dict):
    context = get_context()
    csv_asset = context.data_sources.pandas_default.add_dataframe_asset(
        "df",
        dataframe=pd.DataFrame(
            data=[1, 2, 3],
            columns=["numbers_i_can_count_to"],
        ),
    )
    batch_request = csv_asset.build_batch_request()
    expectation_suite_name = "test_result_format_suite"
    context.suites.add(ExpectationSuite(name=expectation_suite_name))

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    expectation_validation_result = validator.expect_column_max_to_be_between(
        column="numbers_i_can_count_to",
        min_value=1000,
        max_value=10000,
        result_format=result_format,
    )
    assert expectation_validation_result.result == {}


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
    _ = validator.expect_column_max_to_be_between


@pytest.mark.unit
def test__get_attr___raises_attribute_error_with_invalid_attr(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    with pytest.raises(AttributeError) as e:
        _ = validator.my_fake_attr

    assert "'Validator'  object has no attribute 'my_fake_attr'" in str(e.value)


@pytest.mark.unit
def test_list_available_expectation_types(
    validator_with_mock_execution_engine: Validator,
) -> None:
    validator = validator_with_mock_execution_engine

    available = validator.list_available_expectation_types()
    assert all(e.startswith("expect_") for e in available)


def _context_to_validator_and_expectation_sql(
    context: FileDataContext,
) -> Tuple[Validator, gxe.ExpectColumnValuesToBeInSet]:
    """
    Helper method used by sql tests in this suite. Takes in a Datacontext and returns a tuple of Validator and
    Expectation after building a BatchRequest and creating ExpectationSuite.
    Args:
        context (FileDataContext): DataContext to use
    """  # noqa: E501

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
        },
    )
    expectation: gxe.ExpectColumnValuesToBeInSet = gxe.ExpectColumnValuesToBeInSet(
        **expectation_configuration.kwargs
    )

    batch_request = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_sql_data_connector",
        data_asset_name="animals_names_asset",  # this is the name of the table you want to retrieve
    )
    context.suites.add(ExpectationSuite(name="test_suite"))
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    return validator, expectation


@pytest.mark.big
def test_validator_result_format_config_from_validator(
    data_context_with_connection_to_metrics_db,
):
    result_format_config: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }
    (validator, _) = _context_to_validator_and_expectation_sql(
        context=data_context_with_connection_to_metrics_db,
    )

    with pytest.warns(UserWarning) as config_warning:
        _: ExpectationValidationResult = validator.expect_column_values_to_be_in_set(
            column="animals",
            value_set=["cat", "fish", "dog"],
            result_format=result_format_config,
        )

    assert "`result_format` configured at the Validator-level will not be persisted." in str(
        config_warning.list[0].message
    )


@pytest.mark.big
def test_validator_result_format_config_from_expectation(
    data_context_with_connection_to_metrics_db,
):
    runtime_configuration: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    (validator, expectation) = _context_to_validator_and_expectation_sql(
        context=data_context_with_connection_to_metrics_db,
    )
    with pytest.warns(UserWarning) as config_warning:
        _: ExpectationValidationResult = expectation.validate_(
            validator=validator, runtime_configuration=runtime_configuration
        )

    assert "`result_format` configured at the Validator-level will not be persisted." in str(
        config_warning.list[0].message
    )


@pytest.mark.big
def test_graph_validate_with_two_expectations_and_first_expectation_without_additional_configuration(  # noqa: E501
    in_memory_runtime_context, basic_datasource: PandasDatasource
):
    in_memory_runtime_context.datasources["my_datasource"] = basic_datasource
    df = pd.DataFrame(
        [
            "A",
            "B",
            "B",
            "C",
            "C",
            "C",
            "D",
            "D",
            "D",
            "D",
            "E",
            "E",
            "E",
            "E",
            "E",
            "F",
            "F",
            "F",
            "F",
            "F",
            "F",
            "G",
            "G",
            "G",
            "G",
            "G",
            "G",
            "G",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
        ],
        columns=["var"],
    )
    asset = basic_datasource.add_dataframe_asset("my_asset", dataframe=df)
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    expectation_configuration_expect_column_values_to_be_null = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_null",
        kwargs={
            "column": "var",
        },
    )

    expectation_configuration_expect_column_values_to_be_in_set = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "var",
            "value_set": ["B", "C", "D", "F", "G", "H"],
            "result_format": {"result_format": "COMPLETE"},
        },
    )
    result = Validator(
        execution_engine=PandasExecutionEngine(),
        data_context=in_memory_runtime_context,
        batches=[batch],
    ).graph_validate(
        configurations=[
            expectation_configuration_expect_column_values_to_be_null,
            expectation_configuration_expect_column_values_to_be_in_set,
        ]
    )

    assert result == [
        ExpectationValidationResult(
            success=False,
            expectation_config=ExpectationConfiguration(
                "expect_column_values_to_be_null",
                kwargs={
                    "column": "var",
                },
            ),
            meta={},
            result={
                "element_count": 36,
                "unexpected_count": 36,
                "unexpected_percent": 100.0,
                "partial_unexpected_list": [
                    "A",
                    "B",
                    "B",
                    "C",
                    "C",
                    "C",
                    "D",
                    "D",
                    "D",
                    "D",
                    "E",
                    "E",
                    "E",
                    "E",
                    "E",
                    "F",
                    "F",
                    "F",
                    "F",
                    "F",
                ],
            },
            exception_info=None,
        ),
        ExpectationValidationResult(
            success=False,
            expectation_config=ExpectationConfiguration(
                "expect_column_values_to_be_in_set",
                kwargs={
                    "column": "var",
                    "value_set": ["B", "C", "D", "F", "G", "H"],
                    "result_format": {"result_format": "COMPLETE"},
                },
            ),
            meta={},
            result={
                "element_count": 36,
                "unexpected_count": 6,
                "unexpected_percent": 16.666666666666664,
                "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 16.666666666666664,
                "unexpected_percent_nonmissing": 16.666666666666664,
                "partial_unexpected_index_list": [0, 10, 11, 12, 13, 14],
                "partial_unexpected_counts": [
                    {"value": "E", "count": 5},
                    {"value": "A", "count": 1},
                ],
                "unexpected_list": ["A", "E", "E", "E", "E", "E"],
                "unexpected_index_list": [0, 10, 11, 12, 13, 14],
            },
            exception_info=None,
        ),
    ]


@pytest.mark.big
def test_graph_validate_with_two_expectations_and_first_expectation_with_result_format_complete(
    in_memory_runtime_context, basic_datasource: PandasDatasource
):
    in_memory_runtime_context.datasources["my_datasource"] = basic_datasource
    df = pd.DataFrame(
        [
            "A",
            "B",
            "B",
            "C",
            "C",
            "C",
            "D",
            "D",
            "D",
            "D",
            "E",
            "E",
            "E",
            "E",
            "E",
            "F",
            "F",
            "F",
            "F",
            "F",
            "F",
            "G",
            "G",
            "G",
            "G",
            "G",
            "G",
            "G",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
            "H",
        ],
        columns=["var"],
    )

    asset = basic_datasource.add_dataframe_asset("my_asset", dataframe=df)
    batch_definition = asset.add_batch_definition_whole_dataframe("my batch definition")
    batch = batch_definition.get_batch()

    expectation_configuration_expect_column_values_to_be_null = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_null",
        kwargs={
            "column": "var",
        },
    )

    expectation_configuration_expect_column_values_to_be_in_set = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "var",
            "value_set": ["B", "C", "D", "F", "G", "H"],
            "result_format": {"result_format": "COMPLETE"},
        },
    )
    result = Validator(
        execution_engine=PandasExecutionEngine(),
        data_context=in_memory_runtime_context,
        batches=[batch],
    ).graph_validate(
        configurations=[
            expectation_configuration_expect_column_values_to_be_in_set,
            expectation_configuration_expect_column_values_to_be_null,
        ]
    )

    assert result == [
        ExpectationValidationResult(
            success=False,
            expectation_config=ExpectationConfiguration(
                "expect_column_values_to_be_in_set",
                kwargs={
                    "column": "var",
                    "value_set": ["B", "C", "D", "F", "G", "H"],
                    "result_format": {"result_format": "COMPLETE"},
                },
            ),
            meta={},
            result={
                "element_count": 36,
                "unexpected_count": 6,
                "unexpected_percent": 16.666666666666664,
                "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 16.666666666666664,
                "unexpected_percent_nonmissing": 16.666666666666664,
                "partial_unexpected_index_list": [0, 10, 11, 12, 13, 14],
                "partial_unexpected_counts": [
                    {"value": "E", "count": 5},
                    {"value": "A", "count": 1},
                ],
                "unexpected_list": ["A", "E", "E", "E", "E", "E"],
                "unexpected_index_list": [0, 10, 11, 12, 13, 14],
            },
            exception_info=None,
        ),
        ExpectationValidationResult(
            success=False,
            expectation_config=ExpectationConfiguration(
                "expect_column_values_to_be_null",
                kwargs={
                    "column": "var",
                },
            ),
            meta={},
            result={
                "element_count": 36,
                "unexpected_count": 36,
                "unexpected_percent": 100.0,
                "partial_unexpected_list": [
                    "A",
                    "B",
                    "B",
                    "C",
                    "C",
                    "C",
                    "D",
                    "D",
                    "D",
                    "D",
                    "E",
                    "E",
                    "E",
                    "E",
                    "E",
                    "F",
                    "F",
                    "F",
                    "F",
                    "F",
                ],
            },
            exception_info=None,
        ),
    ]


@pytest.mark.unit
def test_validator_with_exception_info_in_result():
    get_context()
    validator = Validator(execution_engine=PandasExecutionEngine())

    mock_resolved_metrics = []

    metric_id = (
        "table.column_types",
        "a351bbf72b281f0b7a62dbbf3599ce5c",
        "include_nested=True",
    )
    exception_message = "Danger Will Robinson! Danger!"
    exception_traceback = 'Traceback (most recent call last):\n File "lostinspace.py", line 42, in <module>\n    raise Exception("Danger Will Robinson! Danger!")\nException: Danger Will Robinson! Danger!'  # noqa: E501

    mock_aborted_metrics_info = {
        metric_id: {
            "exception_info": ExceptionInfo(
                exception_traceback=exception_traceback,
                exception_message=exception_message,
            ),
        }
    }

    with patch.object(
        validator._metrics_calculator,
        "resolve_validation_graph",
        return_value=(mock_resolved_metrics, mock_aborted_metrics_info),
    ):
        result = validator.graph_validate(
            configurations=[
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={
                        "column": "animals",
                    },
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "animals",
                        "value_set": ["cat", "fish", "dog"],
                    },
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={
                        "column": "animals",
                    },
                ),
            ],
            runtime_configuration={"result_format": "COMPLETE"},
        )
        assert len(result) == 3  # One to one mapping of Expectations to results
        for evr in result:
            assert evr.exception_info is not None
            assert evr.exception_info[str(metric_id)].exception_traceback == exception_traceback
            assert evr.exception_info[str(metric_id)].exception_message == exception_message
