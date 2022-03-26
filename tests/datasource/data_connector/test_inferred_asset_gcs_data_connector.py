from unittest import mock

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest, IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import InferredAssetGCSDataConnector
from great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector import (
    storage,
)
from great_expectations.execution_engine import PandasExecutionEngine

yaml = YAML()


@pytest.fixture
def expected_config_dict():
    """Used to validate `self_check()` and `test_yaml_config()` outputs."""
    config = {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["directory", "path"],
        "data_assets": {
            "directory": {
                "batch_definition_count": 2,
                "example_data_references": ["directory/B-1.csv", "directory/B-2.csv"],
            },
            "path": {
                "batch_definition_count": 2,
                "example_data_references": ["path/A-100.csv", "path/A-101.csv"],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    return config


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_instantiation_without_args(
    mock_gcs_conn, mock_list_keys, expected_config_dict
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )
    assert my_data_connector.self_check() == expected_config_dict

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.service_account.Credentials.from_service_account_file"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_instantiation_with_filename_arg(
    mock_gcs_conn, mock_auth_method, mock_list_keys, expected_config_dict
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        gcs_options={
            "filename": "my_filename.json",
        },
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )
    assert my_data_connector.self_check() == expected_config_dict

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.service_account.Credentials.from_service_account_info"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_instantiation_with_info_arg(
    mock_gcs_conn, mock_auth_method, mock_list_keys, expected_config_dict
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        gcs_options={
            "info": "{ my_json: my_content }",
        },
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )
    assert my_data_connector.self_check() == expected_config_dict

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_get_batch_definition_list_from_batch_request_with_nonexistent_datasource_name_raises_error(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    my_data_connector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    # Raises error in `DataConnector._validate_batch_request()` due to `datasource_name` in BatchRequest not matching DataConnector `datasource_name`
    with pytest.raises(ValueError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="something",
                data_connector_name="my_data_connector",
                data_asset_name="something",
            )
        )


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_get_batch_definition_list_from_batch_request_with_unknown_data_connector_raises_error(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()

    # Raises error in `DataConnector._validate_batch_request()` due to `data-connector_name` in BatchRequest not matching DataConnector name
    with pytest.raises(ValueError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_DATASOURCE_NAME",
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "A-100.csv",
        "A-101.csv",
        "B-1.csv",
        "B-2.csv",
        "CCC.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_simple_regex_example_with_implicit_data_asset_names_self_check(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)-(\d+)\.csv",
            "group_names": [
                "data_asset_name",
                "number",
            ],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()

    self_check_report_object = my_data_connector.self_check()

    assert self_check_report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["A", "B"],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2,
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2,
            },
        },
        "example_unmatched_data_references": ["CCC.csv"],
        "unmatched_data_reference_count": 1,
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_complex_regex_example_with_implicit_data_asset_names(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="alpha",
                )
            )
        )
        == 3
    )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="beta",
                )
            )
        )
        == 4
    )

    assert my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            data_connector_query={
                "batch_filter_parameters": {
                    "year_dir": "2020",
                    "month_dir": "03",
                }
            },
        )
    ) == [
        BatchDefinition(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            batch_identifiers=IDDict(
                year_dir="2020",
                month_dir="03",
            ),
        )
    ]


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "A-100.csv",
        "A-101.csv",
        "B-1.csv",
        "B-2.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_self_check(mock_gcs_conn, mock_list_keys, mock_emit):
    my_data_connector: InferredAssetGCSDataConnector = InferredAssetGCSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "number"],
        },
        bucket_or_name="test_bucket",
        prefix="",
    )

    my_data_connector._refresh_data_references_cache()
    self_check_report_object = my_data_connector.self_check()

    assert self_check_report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["A", "B"],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2,
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_test_yaml_config(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: InferredAssetGCSDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR
bucket_or_name: test_bucket
prefix: ""
default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["alpha", "beta"],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "2020/01/alpha-*.csv",
                    "2020/02/alpha-*.csv",
                    "2020/03/alpha-*.csv",
                ],
                "batch_definition_count": 3,
            },
            "beta": {
                "example_data_references": [
                    "2020/01/beta-*.csv",
                    "2020/02/beta-*.csv",
                    "2020/03/beta-*.csv",
                ],
                "batch_definition_count": 4,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_instantiation_with_test_yaml_config_emits_proper_payload(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: InferredAssetGCSDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR
bucket_or_name: test_bucket
prefix: ""
default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert mock_emit.call_count == 1
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "InferredAssetGCSDataConnector",
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
        "gamma-202001.csv",
        "gamma-202002.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_yaml_config_excluding_non_regex_matching_files(
    mock_gcs_client, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: InferredAssetGCSDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR

bucket_or_name: test_bucket
prefix: ""

default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["alpha", "beta"],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "2020/01/alpha-*.csv",
                    "2020/02/alpha-*.csv",
                    "2020/03/alpha-*.csv",
                ],
                "batch_definition_count": 3,
            },
            "beta": {
                "example_data_references": [
                    "2020/01/beta-*.csv",
                    "2020/02/beta-*.csv",
                    "2020/03/beta-*.csv",
                ],
                "batch_definition_count": 4,
            },
        },
        "example_unmatched_data_references": ["gamma-202001.csv", "gamma-202002.csv"],
        "unmatched_data_reference_count": 2,
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "A/A-1.csv",
        "A/A-2.csv",
        "A/A-3.csv",
        "B/B-1.csv",
        "B/B-2.csv",
        "B/B-3.csv",
        "C/C-1.csv",
        "C/C-2.csv",
        "C/C-3.csv",
        "D/D-1.csv",
        "D/D-2.csv",
        "D/D-3.csv",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_nested_directory_data_asset_name_in_folder(
    mock_gcs_client, mock_list_keys, mock_emit, empty_data_context
):
    context = empty_data_context

    report_object = context.test_yaml_config(
        f"""
    module_name: great_expectations.datasource.data_connector
    class_name: InferredAssetGCSDataConnector
    datasource_name: FAKE_DATASOURCE
    name: TEST_DATA_CONNECTOR
    bucket_or_name: test_bucket
    prefix: ""
    default_regex:
        group_names:
            - data_asset_name
            - letter
            - number
        pattern: (\\w{{1}})\\/(\\w{{1}})-(\\d{{1}})\\.csv
        """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": ["A", "B", "C"],
        "data_assets": {
            "A": {
                "batch_definition_count": 3,
                "example_data_references": ["A/A-1.csv", "A/A-2.csv", "A/A-3.csv"],
            },
            "B": {
                "batch_definition_count": 3,
                "example_data_references": ["B/B-1.csv", "B/B-2.csv", "B/B-3.csv"],
            },
            "C": {
                "batch_definition_count": 3,
                "example_data_references": ["C/C-1.csv", "C/C-2.csv", "C/C-3.csv"],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz",
        "2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz",
        "2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz",
        "2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz",
        "2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz",
        "2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz",
        "2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_random_hash(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context
):
    context = empty_data_context

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                - year
                - month
                - day
                - data_asset_name
              pattern: (\\d{{4}})/(\\d{{2}})/(\\d{{2}})/(log_file)-.*\\.txt\\.gz
              """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["log_file"],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "2021/01/01/log_file-*.txt.gz",
                    "2021/01/02/log_file-*.txt.gz",
                    "2021/01/03/log_file-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "log_file-2021-01-01-035419.163324.txt.gz",
        "log_file-2021-01-02-035513.905752.txt.gz",
        "log_file-2021-01-03-035455.848839.txt.gz",
        "log_file-2021-01-04-035251.47582.txt.gz",
        "log_file-2021-01-05-033034.289789.txt.gz",
        "log_file-2021-01-06-034958.505688.txt.gz",
        "log_file-2021-01-07-033545.600898.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_timestamp(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context
):
    context = empty_data_context

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                - data_asset_name
                - year
                - month
                - day
              pattern: (log_file)-(\\d{{4}})-(\\d{{2}})-(\\d{{2}})-.*\\.*\\.txt\\.gz
      """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )
    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["log_file"],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "log_file-2021-01-01-*.txt.gz",
                    "log_file-2021-01-02-*.txt.gz",
                    "log_file-2021-01-03-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context
):
    context = empty_data_context

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-.*\\.txt\\.gz
              """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGCSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["some_bucket"],
        "data_assets": {
            "some_bucket": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "some_bucket/2021/01/01/log_file-*.txt.gz",
                    "some_bucket/2021/01/02/log_file-*.txt.gz",
                    "some_bucket/2021/01/03/log_file-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_sorted(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: full_date
          """,
    )

    my_data_connector: InferredAssetGCSDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_inferred_asset_filesystem_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="my_inferred_asset_filesystem_data_connector",
                data_asset_name="some_bucket",
            )
        )
    )

    expected = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "07", "full_date": "20210107"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "06", "full_date": "20210106"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "05", "full_date": "20210105"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "04", "full_date": "20210104"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "03", "full_date": "20210103"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "02", "full_date": "20210102"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            batch_identifiers=IDDict(
                {"year": "2021", "month": "01", "day": "01", "full_date": "20210101"}
            ),
        ),
    ]
    assert expected == sorted_batch_definition_list


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetGCSDataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket_or_name: test_bucket
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: not_matching_anything

          """,
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
    return_value=[
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
def test_redundant_information_in_naming_convention_bucket_too_many_sorters(
    mock_gcs_conn, mock_list_keys, mock_emit
):
    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetGCSDataConnector
        datasource_name: test_environment
        name: my_inferred_asset_filesystem_data_connector
        bucket_or_name: test_bucket
        prefix: ""
        default_regex:
            group_names:
                - data_asset_name
                - year
                - month
                - day
                - full_date
            pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
        sorters:
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price
          """,
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )


@pytest.mark.skipif(
    storage is None,
    reason="Could not import 'storage' from google.cloud in inferred_asset_gcs_data_connector.py",
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.storage.Client"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_gcs_data_connector.list_gcs_keys",
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_get_full_file_path(
    mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    yaml_string = f"""
class_name: InferredAssetGCSDataConnector
datasource_name: FAKE_DATASOURCE_NAME
bucket_or_name: my_bucket
prefix: my_base_directory/
default_regex:
   pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
   group_names:
       - data_asset_name
       - year_dir
       - month_dir
   """
    config = yaml.load(yaml_string)

    mock_list_keys.return_value = [
        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
        "my_base_directory/beta_here/beta-202001.txt",
        "my_base_directory/beta_here/beta-202002.txt",
        "my_base_directory/beta_here/beta-202003.txt",
        "my_base_directory/beta_here/beta-202004.txt",
        "my_base_directory/gamma-202001.csv",
        "my_base_directory/gamma-202002.csv",
        "my_base_directory/gamma-202003.csv",
        "my_base_directory/gamma-202004.csv",
        "my_base_directory/gamma-202005.csv",
    ]

    my_data_connector: InferredAssetGCSDataConnector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        runtime_environment={
            "name": "my_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
    )

    assert (
        my_data_connector._get_full_file_path(
            "my_base_directory/alpha/files/go/here/alpha-202001.csv", "alpha"
        )
        == "gs://my_bucket/my_base_directory/alpha/files/go/here/alpha-202001.csv"
    )
    assert (
        my_data_connector._get_full_file_path(
            "my_base_directory/beta_here/beta-202002.txt", "beta"
        )
        == "gs://my_bucket/my_base_directory/beta_here/beta-202002.txt"
    )
    assert (
        my_data_connector._get_full_file_path(
            "my_base_directory/gamma-202005.csv", "gamma"
        )
        == "gs://my_bucket/my_base_directory/gamma-202005.csv"
    )
