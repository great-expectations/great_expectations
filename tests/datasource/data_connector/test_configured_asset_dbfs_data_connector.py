from typing import List
from unittest import mock

import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.core.id_dict import BatchSpec
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import (
    ConfiguredAssetDBFSDataConnector,
)
from great_expectations.execution_engine import PandasExecutionEngine
from tests.test_utils import create_files_in_directory

yaml = YAML()


def test_basic_instantiation(tmp_path_factory):
    """
    What does this test and why?
    Parallels the same test for the parent class ConfiguredAssetFilesystemDataConnector to
    ensure there are no regressions in ConfiguredAssetDBFSDataConnector
    """
    base_directory = str(tmp_path_factory.mktemp("test_basic_instantiation"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    my_data_connector = ConfiguredAssetDBFSDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=base_directory,
        assets={"alpha": {}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetDBFSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "alpha-1.csv",
                    "alpha-2.csv",
                    "alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {},
    }

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(
            my_data_connector.get_batch_definition_list_from_batch_request(
                BatchRequest(
                    datasource_name="something",
                    data_connector_name="my_data_connector",
                    data_asset_name="something",
                )
            )
        )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_instantiation_from_a_config(
    mock_emit, empty_data_context_stats_enabled, tmp_path_factory
):
    """
    What does this test and why?
    Parallels the same test for the parent class ConfiguredAssetFilesystemDataConnector to
    ensure there are no regressions in ConfiguredAssetDBFSDataConnector
    """
    context: DataContext = empty_data_context_stats_enabled

    base_directory = str(tmp_path_factory.mktemp("test_instantiation_from_a_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: ConfiguredAssetDBFSDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}
# glob_directive: "*.csv"

default_regex:
    pattern: alpha-(.*)\\.csv
    group_names:
        - index

assets:
    alpha:
    """,
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetDBFSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "alpha-1.csv",
                    "alpha-2.csv",
                    "alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {},
    }
    assert mock_emit.call_count == 1
    # Substitute current anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "ConfiguredAssetDBFSDataConnector",
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list


def test__get_full_file_path_for_asset_pandas(fs):
    """
    What does this test and why?
    File paths in DBFS need to use the `dbfs:/` protocol base instead of `/dbfs/` when
    being read using the `spark.read` method in the ExecutionEngine. HOWEVER when using a
    PandasExecutionEngine the file semantic `/dbfs/` version must be used instead.
    This test verifies that a config using a `/dbfs/` path is NOT translated to `dbfs:/`
    when preparing the PathBatchSpec for the PandasExecutionEngine.
    """

    base_directory: str = "/dbfs/great_expectations"
    fs.create_dir(base_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetDBFSDataConnector
            datasource_name: BASE
            base_directory: {base_directory}/test_dir_0/A
            glob_directive: "*"
            default_regex:
              pattern: (.+)\\.csv
              group_names:
              - name

            assets:
              A:
                base_directory: B/C
                glob_directive: "log*.csv"
                pattern: (.+)_(\\d+)\\.csv
                group_names:
                - name
                - number
        """,
    )

    my_data_connector: ConfiguredAssetDBFSDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_configured_asset_filesystem_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    my_data_connector.data_context_root_directory = base_directory

    assert (
        my_data_connector._get_full_file_path_for_asset(
            path="bigfile_1.csv", asset=my_data_connector.assets["A"]
        )
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )
    self_check_report = my_data_connector.self_check()
    assert self_check_report == {
        "class_name": "ConfiguredAssetDBFSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["A"],
        "data_assets": {
            "A": {
                "batch_definition_count": 1,
                "example_data_references": ["logfile_0.csv"],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {},
    }

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="my_configured_asset_filesystem_data_connector",
        data_asset_name="A",
        data_connector_query=None,
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 1

    my_batch_definition = my_batch_definition_list[0]
    batch_spec: BatchSpec = my_data_connector.build_batch_spec(
        batch_definition=my_batch_definition
    )

    assert isinstance(batch_spec, PathBatchSpec)
    assert batch_spec.path == f"{base_directory}/test_dir_0/A/B/C/logfile_0.csv"


def test__get_full_file_path_for_asset_spark(basic_spark_df_execution_engine, fs):
    """
    What does this test and why?
    File paths in DBFS need to use the `dbfs:/` protocol base instead of `/dbfs/` when
    being read using the `spark.read` method in the ExecutionEngine. In the data connector
    config however, the `/dbfs` version must be used. This test verifies that a config
    using a `/dbfs/` path is translated to `dbfs:/` when preparing the PathBatchSpec for the
    SparkDFExecutionEngine.
    """

    base_directory: str = "/dbfs/great_expectations"
    base_directory_colon: str = "dbfs:/great_expectations"
    fs.create_dir(base_directory)

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetDBFSDataConnector
            datasource_name: BASE
            base_directory: {base_directory}/test_dir_0/A
            glob_directive: "*"
            default_regex:
              pattern: (.+)\\.csv
              group_names:
              - name

            assets:
              A:
                base_directory: B/C
                glob_directive: "log*.csv"
                pattern: (.+)_(\\d+)\\.csv
                group_names:
                - name
                - number
        """,
    )

    my_data_connector: ConfiguredAssetDBFSDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_configured_asset_filesystem_data_connector",
            "execution_engine": basic_spark_df_execution_engine,
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    my_data_connector.data_context_root_directory = base_directory

    assert (
        my_data_connector._get_full_file_path_for_asset(
            path="bigfile_1.csv", asset=my_data_connector.assets["A"]
        )
        == f"{base_directory_colon}/test_dir_0/A/B/C/bigfile_1.csv"
    )
    self_check_report = my_data_connector.self_check()
    assert self_check_report == {
        "class_name": "ConfiguredAssetDBFSDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["A"],
        "data_assets": {
            "A": {
                "batch_definition_count": 1,
                "example_data_references": ["logfile_0.csv"],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {},
    }

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="my_configured_asset_filesystem_data_connector",
        data_asset_name="A",
        data_connector_query=None,
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 1

    my_batch_definition = my_batch_definition_list[0]
    batch_spec: BatchSpec = my_data_connector.build_batch_spec(
        batch_definition=my_batch_definition
    )

    assert isinstance(batch_spec, PathBatchSpec)
    assert batch_spec.path == "dbfs:/great_expectations/test_dir_0/A/B/C/logfile_0.csv"
