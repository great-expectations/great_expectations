from great_expectations.execution_environment.data_connector import FilesDataConnector
from great_expectations.core.batch import BatchRequest
from tests.test_utils import (
    create_files_in_directory,
)


def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ]
    )

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=base_directory,
        assets={
            "alpha": {}
        }
    )

    assert my_data_connector.self_check() == {
        "class_name": "FilesDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
                "batch_definition_count": 3
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    my_data_connector.refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []

    print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="something",
        data_connector_name="my_data_connector",
        data_asset_name="something",
    )))


def test_instantiation_from_a_config(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: FilesDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}
# glob_directive: "*.csv"

default_regex:
    pattern: alpha-(.*)\\.csv
    group_names:
        - index

assets:
    alpha:
        partitioner_name: my_partitioner

    """, return_mode="return_object")

    assert return_object == {
        "class_name": "FilesDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
                "batch_definition_count": 3
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }