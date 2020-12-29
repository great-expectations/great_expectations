# TODO: <Alex>This module should be broken up -- please see suggestions below.</Alex>
import json
import os

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,
    InferredAssetFilesystemDataConnector,
)
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from tests.test_utils import create_files_in_directory

yaml = YAML()


@pytest.fixture
def basic_data_connector(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )

    basic_data_connector = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: ConfiguredAssetFilesystemDataConnector
base_directory: {base_directory}
datasource_name: FAKE_DATASOURCE

default_regex:
    pattern: "(.*)"
    group_names:
        - file_name

assets:
    my_asset_name: {{}}
""",
        ),
        runtime_environment={"name": "my_data_connector"},
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    return basic_data_connector


# TODO: <Alex>This test should be moved to "tests/datasource/data_connector/test_configured_asset_filesystem_data_connector.py".</Alex>
def test_basic_instantiation(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )

    # noinspection PyUnusedLocal
    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*.csv",
        datasource_name="FAKE_DATASOURCE",
        default_regex={
            "pattern": "(.*)",
            "group_names": ["file_name"],
        },
        assets={"my_asset_name": {}},
    )


# TODO: <Alex>This test should be potentially moved to "tests/datasource/data_connector/test_configured_asset_filesystem_data_connector.py".</Alex>
def test__get_instantiation_through_instantiate_class_from_config(basic_data_connector):
    # noinspection PyProtectedMember
    data_references: list = (
        basic_data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name="my_asset_name"
        )
    )
    assert len(data_references) == 0
    assert data_references == []


# TODO: <Alex>This test should be renamed properly and moved to "tests/datasource/data_connector/test_configured_asset_filesystem_data_connector.py".</Alex>
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

    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*/*/*.csv",
        datasource_name="FAKE_DATASOURCE",
        default_regex={
            "pattern": "(.*).csv",
            "group_names": ["name"],
        },
        assets={"stuff": {}},
    )

    with pytest.raises(ValueError):
        my_data_connector.get_data_reference_list_count()

    with pytest.raises(ValueError):
        my_data_connector.get_unmatched_data_references()

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert len(my_data_connector.get_unmatched_data_references()) == 0
    assert my_data_connector.get_data_reference_list_count() == 4


def test_get_batch_definition_list_from_batch_request():
    pass


def test_build_batch_spec_from_batch_definition():
    pass


def test_get_batch_data_and_metadata():
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
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        partition_definition=PartitionDefinition(
            {
                "id": "A",
            }
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A, batch_request=BatchRequest(datasource_name="A")
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A, batch_request=BatchRequest(datasource_name="B")
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            datasource_name="A",
            data_connector_name="a",
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            datasource_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
        ),
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            datasource_name="A",
            data_connector_name="a",
            data_asset_name="bbb",
        ),
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            datasource_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
            partition_request={
                "partition_identifiers": {"id": "B"},
            },
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequest(
            partition_request={
                "partition_identifiers": {"id": "A"},
            }
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=BatchDefinition(
            **{
                "datasource_name": "FAKE_DATASOURCE",
                "data_connector_name": "TEST_DATA_CONNECTOR",
                "data_asset_name": "DEFAULT_ASSET_NAME",
                "partition_definition": PartitionDefinition({"index": "3"}),
            }
        ),
        batch_request=BatchRequest(
            **{
                "datasource_name": "FAKE_DATASOURCE",
                "data_connector_name": "TEST_DATA_CONNECTOR",
                "data_asset_name": "DEFAULT_ASSET_NAME",
                "partition_request": None,
            }
        ),
    )
    # TODO : Test cases to exercise ranges, etc.


def test_for_self_check_using_InferredAssetFilesystemDataConnector_PandasExecutionEngine(
    tmp_path_factory,
):
    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alex_20201010_1000.csv",
            "abe_202011111_2000.csv",
            "will_20201212_3000.csv",
        ],
    )
    my_data_connector = InferredAssetFilesystemDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*.csv",
        datasource_name="FAKE_DATASOURCE",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "(.+)_(\\d+)_(\\d+)\\.csv",
            "group_names": ["data_asset_name", "timestamp", "size"],
        },
    )
    self_check_results = my_data_connector.self_check()
    assert self_check_results["data_asset_count"] == 3
    assert self_check_results["example_data_reference"]["n_rows"] == 2


def test_for_self_check_using_InferredAssetFilesystemDataConnector_SparkDFExecutionEngine(
    spark_session, tmp_path_factory
):
    base_directory = str(
        tmp_path_factory.mktemp(
            "basic_data_connector_inferred_asset_filesystem_data_connector"
        )
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alex_20201010_1000.csv",
            "abe_202011111_2000.csv",
            "will_20201212_3000.csv",
        ],
    )
    my_data_connector = InferredAssetFilesystemDataConnector(
        name="my_data_connector",
        base_directory=base_directory,
        glob_directive="*.csv",
        datasource_name="FAKE_DATASOURCE",
        execution_engine=SparkDFExecutionEngine(),
        default_regex={
            "pattern": "(.+)_(\\d+)_(\\d+)\\.csv",
            "group_names": ["data_asset_name", "timestamp", "size"],
        },
    )
    self_check_results = my_data_connector.self_check()
    assert self_check_results["data_asset_count"] == 3
    assert self_check_results["example_data_reference"]["n_rows"] == 3
