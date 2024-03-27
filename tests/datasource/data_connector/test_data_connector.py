# TODO: <Alex>This module should be broken up -- please see suggestions below.</Alex>
import pytest

from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    IDDict,
    LegacyBatchDefinition,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.execution_engine import PandasExecutionEngine

yaml = YAMLHandler()


@pytest.fixture
def basic_data_connector(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))

    basic_data_connector = instantiate_class_from_config(
        yaml.load(
            f"""
class_name: ConfiguredAssetFilesystemDataConnector
name: my_data_connector
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
        runtime_environment={
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    return basic_data_connector


# TODO: <Alex>This test should be moved to the test module that is dedicated to BatchRequest and BatchDefinition testing.</Alex>  # noqa: E501
@pytest.mark.unit
def test__batch_definition_matches_batch_request():
    # TODO: <Alex>We need to cleanup PyCharm warnings.</Alex>
    A = LegacyBatchDefinition(
        datasource_name="A",
        data_connector_name="a",
        data_asset_name="aaa",
        batch_identifiers=IDDict(
            {
                "id": "A",
            }
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="A", data_connector_name="", data_asset_name=""
        ),
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="B", data_connector_name="", data_asset_name=""
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="A", data_connector_name="a", data_asset_name=""
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="A", data_connector_name="a", data_asset_name="aaa"
        ),
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="A", data_connector_name="a", data_asset_name="bbb"
        ),
    )

    assert not batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="A",
            data_connector_name="a",
            data_asset_name="aaa",
            data_connector_query={
                "batch_filter_parameters": {"id": "B"},
            },
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=A,
        batch_request=BatchRequestBase(
            datasource_name="",
            data_connector_name="",
            data_asset_name="",
            data_connector_query={
                "batch_filter_parameters": {"id": "A"},
            },
        ),
    )

    assert batch_definition_matches_batch_request(
        batch_definition=LegacyBatchDefinition(
            **{
                "datasource_name": "FAKE_DATASOURCE",
                "data_connector_name": "TEST_DATA_CONNECTOR",
                "data_asset_name": "DEFAULT_ASSET_NAME",
                "batch_identifiers": IDDict({"index": "3"}),
            }
        ),
        batch_request=BatchRequest(
            **{
                "datasource_name": "FAKE_DATASOURCE",
                "data_connector_name": "TEST_DATA_CONNECTOR",
                "data_asset_name": "DEFAULT_ASSET_NAME",
                "data_connector_query": None,
            }
        ),
    )
    # TODO : Test cases to exercise ranges, etc.


@pytest.mark.filesystem
def test__get_instantiation_through_instantiate_class_from_config(basic_data_connector):
    # noinspection PyProtectedMember
    data_references: list = (
        basic_data_connector._get_data_reference_list_from_cache_by_data_asset_name(
            data_asset_name="my_asset_name"
        )
    )
    assert len(data_references) == 0
    assert data_references == []
