import pytest
import pandas as pd
import yaml
import json

from great_expectations.execution_environment.data_connector import (
    FilesDataConnector,
    SinglePartitionDictDataConnector,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from tests.test_utils import (
    create_files_in_directory,
    create_fake_data_frame,
)

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)


@pytest.fixture
def basic_data_connector(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))

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
        }
    )
    return basic_data_connector


def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))

    # noinspection PyUnusedLocal
    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive='*.csv',
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        default_regex={
            "pattern": "(.*)",
            "group_names": ["file_name"],
        },
        assets={
            "my_asset_name": {}
        }
    )
    

def test__get_instantiation_through_instantiate_class_from_config(basic_data_connector):
    # noinspection PyProtectedMember
    data_references: list = basic_data_connector._get_data_reference_list(data_asset_name="my_asset_name")
    assert len(data_references) == 0
    assert data_references == []


def test__file_object_caching_for_FileDataConnector(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list= [
            "pretend/path/A-100.csv",
            "pretend/path/A-101.csv",
            "pretend/directory/B-1.csv",
            "pretend/directory/B-2.csv",            
        ]
    )

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive='*/*/*.csv',
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


def test_convert_in_memory_dataset_to_batch():
    pass


def test_refresh_data_references_cache():
    pass


def test_get_unmatched_data_references():
    pass


def test_get_cached_data_reference_count():
    pass


def test_available_data_asset_names():
    pass


def test__batch_definition_matches_batch_request():
    my_data_connector = SinglePartitionDictDataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        data_reference_dict={},
    )

    # TODO: <Alex>We need to cleanup PyCharm warnings.</Alex>
    A = BatchDefinition(
        execution_environment_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition(
            {
                "id": "A"
            }
        )
    )

    # noinspection PyProtectedMember
    assert my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A"
        )
    )

    # noinspection PyProtectedMember
    assert not my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="B"
        )
    )

    # noinspection PyProtectedMember
    assert my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
        )
    )

    # noinspection PyProtectedMember
    assert my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
        )
    )

    # noinspection PyProtectedMember
    assert not my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="bbb",
        )
    )

    # noinspection PyProtectedMember
    assert not my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            execution_environment_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
            partition_request={
                "id": "B"
            }
        )
    )

    # noinspection PyProtectedMember
    assert my_data_connector._batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            partition_request={
                "id": "A"
            }
        )
    )

    # noinspection PyProtectedMember
    assert my_data_connector._batch_definition_matches_batch_request(
        batch_definition=BatchDefinition(**{
            "execution_environment_name": "FAKE_EXECUTION_ENVIRONMENT",
            "data_connector_name": "TEST_DATA_CONNECTOR",
            "data_asset_name": "DEFAULT_ASSET_NAME",
            "partition_definition": PartitionDefinition(**{
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
