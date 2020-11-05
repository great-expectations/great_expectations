# TODO: <Alex>This module should be broken up -- please see suggestions below.</Alex>
import pytest
import yaml

from great_expectations.execution_environment.data_connector import FilesDataConnector
from tests.test_utils import (
    create_files_in_directory,
)
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment.data_connector import (
    FilesDataConnector,
)
from great_expectations.execution_environment.data_connector.util import batch_definition_matches_batch_request
from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture
def basic_data_connector(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )

    basic_data_connector = instantiate_class_from_config(yaml.load(
        f"""
class_name: FilesDataConnector
base_directory: {base_directory}
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT

default_regex:
    pattern: "(.*)"
    group_names:
        - file_name

assets:
    my_asset_name: {{}}
""", Loader=yaml.FullLoader
    ),
        runtime_environment={
            "name": "my_data_connector"
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )
    return basic_data_connector


# TODO: <Alex>This test should be moved to "tests/execution_environment/data_connector/test_files_data_connector.py".</Alex>
def test_basic_instantiation(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )

    # noinspection PyUnusedLocal
    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*.csv",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        default_regex={
            "pattern": "(.*)",
            "group_names": ["file_name"],
        },
        assets={
            "my_asset_name": {}
        }
    )


# TODO: <Alex>This test should be potentially moved to "tests/execution_environment/data_connector/test_files_data_connector.py".</Alex>
def test__get_instantiation_through_instantiate_class_from_config(basic_data_connector):
    # noinspection PyProtectedMember
    data_references: list = basic_data_connector._get_data_reference_list_from_cache_by_data_asset_name(
        data_asset_name="my_asset_name"
    )
    assert len(data_references) == 0
    assert data_references == []


# TODO: <Alex>This test should be renamed properly and moved to "tests/execution_environment/data_connector/test_files_data_connector.py".</Alex>
def test__file_object_caching_for_FileDataConnector(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "pretend/path/A-100.csv",
            "pretend/path/A-101.csv",
            "pretend/directory/B-1.csv",
            "pretend/directory/B-2.csv",
        ],
    )

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*/*/*.csv",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        default_regex={
            "pattern" : "(.*).csv",
            "group_names" : ["name"],
        },
        assets={
            "stuff": {}
        }
    )

    with pytest.raises(ValueError):
        my_data_connector.get_data_reference_list_count()

    with pytest.raises(ValueError):
        my_data_connector.get_unmatched_data_references()

    my_data_connector.refresh_data_references_cache()

    assert len(my_data_connector.get_unmatched_data_references()) == 0
    assert my_data_connector.get_data_reference_list_count() == 4


def test_get_batch_definition_list_from_batch_request():
    pass


def test_build_batch_spec_from_batch_definition():
    pass


def test_get_batch_data_and_metadata_from_batch_definition():
    pass


def test_convert_batch_data_to_batch():
    pass


def test_refresh_data_references_cache():
    pass


def test_get_unmatched_data_references():
    pass


def test_get_cached_data_reference_count():
    pass


def test_available_data_asset_names():
    pass


# TODO: <Alex>This test should be moved to the test module that is dedicated to BatchRequest and BatchDefinition testing.</Alex>
def test__batch_definition_matches_batch_request():
    # TODO: <Alex>We need to cleanup PyCharm warnings.</Alex>
    A = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition(
            {
               "id": "A",
            }
        )
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A"
        )
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="B"
        )
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
        )
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
        )
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="bbb",
        )
    )


    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
            partition_request={
                "partition_identifiers": {
                    "id": "B"
                },
            }
        )
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            partition_request={
                "partition_identifiers": {
                    "id": "A"
                },
            }
        )
    )

    assert batch_definition_matches_batch_request(
        batch_definition=BatchDefinition(**{
            "execution_environment_name": "FAKE_EXECUTION_ENVIRONMENT",
            "data_connector_name": "TEST_DATA_CONNECTOR",
            "data_asset_name": "DEFAULT_ASSET_NAME",
            "partition_definition": PartitionDefinition({
                "index": "3"
            })
        }),
        batch_request=BatchRequest(**{
            "execution_environment_name": "FAKE_EXECUTION_ENVIRONMENT",
            "data_connector_name": "TEST_DATA_CONNECTOR",
            "data_asset_name": "DEFAULT_ASSET_NAME",
            "partition_request": None
        })
    )
    # TODO : Test cases to exercise ranges, etc.
