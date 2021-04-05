import datetime
from typing import List

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import DataConnector
from tests.test_utils import create_files_in_directory

yaml = YAML()


@pytest.fixture()
def create_files_and_instantiate_data_connector(tmp_path_factory):

    base_directory = str(
        tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alex_20200809_1000.csv",
            "eugene_20200809_1500.csv",
            "james_20200811_1009.csv",
            "abe_20200809_1040.csv",
            "will_20200809_1002.csv",
            "james_20200713_1567.csv",
            "eugene_20201129_1900.csv",
            "will_20200810_1001.csv",
            "james_20200810_1003.csv",
            "alex_20200819_1300.csv",
        ],
    )

    my_data_connector_yaml = yaml.load(
        f"""
            class_name: ConfiguredAssetFilesystemDataConnector
            datasource_name: test_environment
            execution_engine:
                BASE_ENGINE:
                class_name: PandasExecutionEngine
            base_directory: {base_directory}
            glob_directive: '*.csv'
            assets:
                TestFiles:
            default_regex:
                pattern: (.+)_(.+)_(.+)\\.csv
                group_names:
                    - name
                    - timestamp
                    - price
            sorters:
                - orderby: asc
                  class_name: LexicographicSorter
                  name: name
                - datetime_format: '%Y%m%d'
                  orderby: desc
                  class_name: DateTimeSorter
                  name: timestamp
                - orderby: desc
                  class_name: NumericSorter
                  name: price

        """,
    )

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "datasource_name": "test_environment",
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    return my_data_connector


def test_data_connector_query_non_recognized_param(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Test 1: non valid_batch_identifiers_limit
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        sorted_batch_definition_list = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="test_environment",
                    data_connector_name="general_filesystem_data_connector",
                    data_asset_name="TestFiles",
                    data_connector_query={"fake": "I_wont_work"},
                )
            )
        )

    # Test 2: Unrecognized custom_filter is not a function
    with pytest.raises(ge_exceptions.BatchFilterError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={"custom_filter_function": "I_wont_work_either"},
            )
        )

    # Test 3: batch_identifiers is not dict
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        sorted_batch_definition_list = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="test_environment",
                    data_connector_name="general_filesystem_data_connector",
                    data_asset_name="TestFiles",
                    data_connector_query={"batch_filter_parameters": 1},
                )
            )
        )

    returned = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            data_connector_query={"batch_filter_parameters": {"name": "alex"}},
        )
    )
    assert len(returned) == 2


def test_data_connector_query_limit(create_files_and_instantiate_data_connector):
    my_data_connector = create_files_and_instantiate_data_connector
    # no limit
    sorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={"limit": None},
            )
        )
    )
    assert len(sorted_batch_definition_list) == 10

    # proper limit
    sorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={"limit": 3},
            )
        )
    )
    assert len(sorted_batch_definition_list) == 3

    # illegal limit
    with pytest.raises(ge_exceptions.BatchFilterError):
        sorted_batch_definition_list = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="test_environment",
                    data_connector_name="general_filesystem_data_connector",
                    data_asset_name="TestFiles",
                    data_connector_query={"limit": "apples"},
                )
            )
        )


def test_data_connector_query_illegal_index_and_limit_combination(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        sorted_batch_definition_list = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="test_environment",
                    data_connector_name="general_filesystem_data_connector",
                    data_asset_name="TestFiles",
                    data_connector_query={"index": 0, "limit": 1},
                )
            )
        )


def test_data_connector_query_sorted_filtered_by_custom_filter(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector
                },
            )
        )
    )

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_limit(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "limit": 4,
                },
            )
        )
    )

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_index_as_int(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": 0,
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 1

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_index_as_string(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": "-1",
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 1
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_slice_as_list(
    create_files_and_instantiate_data_connector,
):
    # <TODO> is this behavior correct?
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": [1, 3],
                },
            )
        )
    )

    assert len(returned_batch_definition_list) == 2

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_slice_as_tuple(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": (0, 4, 3),
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 2

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_slice_as_str(
    create_files_and_instantiate_data_connector,
):
    # <TODO> is this behavior correct?
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": "3:5",
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 2

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_sorted_filtered_by_custom_filter_with_slice_obj(
    create_files_and_instantiate_data_connector,
):
    # <TODO> is this behavior correct?
    my_data_connector = create_files_and_instantiate_data_connector

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            batch_identifiers["name"] in ["abe", "james", "eugene"]
            and datetime.datetime.strptime(
                batch_identifiers["timestamp"], "%Y%m%d"
            ).date()
            > datetime.datetime(2020, 7, 15).date()
        )

    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "custom_filter_function": my_custom_batch_selector,
                    "index": slice(3, 5, None),
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 2

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_data_connector_query_batch_identifiers_1_key(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector
    # no limit
    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "batch_filter_parameters": {"timestamp": "20200809"},
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 4

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "alex", "timestamp": "20200809", "price": "1000"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "will", "timestamp": "20200809", "price": "1002"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_data_connector_query_batch_identifiers_1_key_and_index(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector
    # no limit
    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "batch_filter_parameters": {"name": "james"},
                    "index": 0,
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 1

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_data_connector_query_batch_identifiers_2_key_name_timestamp(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector
    # no limit
    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
                data_connector_query={
                    "batch_filter_parameters": {
                        "timestamp": "20200809",
                        "name": "will",
                    },
                },
            )
        )
    )
    assert len(returned_batch_definition_list) == 1

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "will", "timestamp": "20200809", "price": "1002"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_for_data_asset_name(
    create_files_and_instantiate_data_connector,
):
    my_data_connector = create_files_and_instantiate_data_connector
    # no limit
    returned_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
            )
        )
    )
    assert len(returned_batch_definition_list) == 10

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "alex", "timestamp": "20200819", "price": "1300"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "alex", "timestamp": "20200809", "price": "1000"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "james", "timestamp": "20200713", "price": "1567"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "will", "timestamp": "20200810", "price": "1001"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict(
                {"name": "will", "timestamp": "20200809", "price": "1002"}
            ),
        ),
    ]
    assert returned_batch_definition_list == expected
