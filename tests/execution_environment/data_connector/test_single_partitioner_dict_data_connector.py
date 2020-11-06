import pytest
import yaml

from typing import List

from great_expectations.execution_environment.data_connector import SinglePartitionerDictDataConnector
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
from tests.test_utils import (
    create_fake_data_frame,
    create_files_in_directory,
)
import great_expectations.exceptions.exceptions as ge_exceptions


def test_basic_instantiation():
    data_reference_dict = {
        "path/A-100.csv": create_fake_data_frame(),
        "path/A-101.csv": create_fake_data_frame(),
        "directory/B-1.csv": create_fake_data_frame(),
        "directory/B-2.csv": create_fake_data_frame(),
    }

    my_data_connector: SinglePartitionerDictDataConnector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        default_regex={
            "pattern": "(.*)/(.+)-(\\d+)\\.csv",
            "group_names": [
                "data_asset_name",
                "letter",
                "number"
            ],
        },
        data_reference_dict=data_reference_dict,
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name="something",
                    data_connector_name="my_data_connector",
                    data_asset_name="something",
                )
            )
        )


# TODO: <Alex>Can we come up with an explicit way of figuring out the data_asset_name instead of using the implicit mechanism (via "group_names")?</Alex>
def test_example_with_implicit_data_asset_names():
    data_reference_dict = {
        data_reference: create_fake_data_frame
        for data_reference in [
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ]
    }

    yaml_string = """
class_name: SinglePartitionerDictDataConnector
base_directory: my_base_directory/
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT_NAME

default_regex:
    pattern: (\\d{4})/(\\d{2})/(.+)-\\d+\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """
    config = yaml.load(yaml_string, Loader=yaml.FullLoader)
    config["data_reference_dict"] = data_reference_dict
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
        runtime_environment={"name": "my_data_connector"},
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name="non_existent_execution_environment",
                    data_connector_name="my_data_connector",
                    data_asset_name="my_data_asset",
                )
            )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[BatchDefinition] = my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
                    data_connector_name="non_existent_data_connector",
                    data_asset_name="my_data_asset",
                )
            )

    assert len(
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
                data_connector_name="my_data_connector",
                data_asset_name="alpha",
            )
        )
    ) == 3

    assert len(
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                data_connector_name="my_data_connector",
                data_asset_name="alpha",
            )
        )
    ) == 3

    assert len(
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                data_connector_name="my_data_connector",
                data_asset_name="beta",
            )
        )
    ) == 4

    assert my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            partition_request={
                "partition_identifiers": {
                    "year_dir": "2020",
                    "month_dir": "03",
                }
            }
        )
    ) == [
        BatchDefinition(
            execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            partition_definition=PartitionDefinition(
                year_dir="2020",
                month_dir="03",
            )
        )
    ]


def test_self_check():
    data_reference_dict = {
        "A-100.csv": create_fake_data_frame(),
        "A-101.csv": create_fake_data_frame(),
        "B-1.csv": create_fake_data_frame(),
        "B-2.csv": create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        data_reference_dict=data_reference_dict,
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        default_regex={
            "pattern": "(.+)-(\\d+)\\.csv",
            # TODO: <Alex>Accommodating "data_asset_name" inside partition_definition (e.g., via "group_names") is problematic; idea: resurrect the Partition class.</Alex>
            "group_names": [
                "data_asset_name",
                "number"
            ]
        }
    )

    self_check_return_object = my_data_connector.self_check()

    assert self_check_return_object == {
        "class_name": "SinglePartitionerDictDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "A",
            "B"
        ],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2
            }
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


def test_that_needs_a_better_name():
    data_reference_dict = {
        "A-100.csv": create_fake_data_frame(),
        "A-101.csv": create_fake_data_frame(),
        "B-1.csv": create_fake_data_frame(),
        "B-2.csv": create_fake_data_frame(),
        "CCC.csv": create_fake_data_frame(),
    }

    my_data_connector = SinglePartitionerDictDataConnector(
        name="my_data_connector",
        data_reference_dict=data_reference_dict,
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT",
        default_regex={
            "pattern": "(.+)-(\\d+)\\.csv",
            "group_names": [
                "data_asset_name",
                "number"
            ]
        }
    )

    self_check_return_object = my_data_connector.self_check()

    assert self_check_return_object == {
        "class_name": "SinglePartitionerDictDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "A",
            "B"
        ],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2
            }
        },
        "example_unmatched_data_references": ["CCC.csv"],
        "unmatched_data_reference_count": 1,
    }
