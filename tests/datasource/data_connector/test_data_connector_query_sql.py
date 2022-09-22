import datetime
import os
from typing import List

import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, IDDict
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.datasource import SimpleSqlalchemyDatasource


@pytest.fixture()
def create_db_and_instantiate_simple_sql_datasource():
    data_path: str = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "sqlite",
            "yellow_tripdata_2020.db",
        ),
    )
    datasource_config: dict = {
        "name": "taxi_multi_batch_sql_datasource",
        "module_name": "great_expectations.datasource",
        "class_name": "SimpleSqlalchemyDatasource",
        "connection_string": "sqlite:///" + data_path,
        "tables": {
            "yellow_tripdata_sample_2020_01": {
                "partitioners": {
                    "whole_table": {},
                    "by_vendor_id": {
                        "splitter_method": "split_on_divided_integer",
                        "splitter_kwargs": {"column_name": "vendor_id", "divisor": 1},
                    },
                    "by_pickup_date_time": {
                        "splitter_method": "split_on_converted_datetime",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                            "date_format_string": "%Y-%m-%d %H",
                        },
                    },
                },
                "yellow_tripdata_sample_2020_02": {
                    "partitioners": {
                        "whole_table": {},
                    },
                },
            },
        },
    }
    simple_sql_datasource: SimpleSqlalchemyDatasource = instantiate_class_from_config(
        config=datasource_config,
        runtime_environment={
            "name": "taxi_multi_batch_sql_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return simple_sql_datasource


def test_data_connector_query_non_recognized_param(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )

    # Test 1: non valid_batch_identifiers_limit
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        batch_definition_list = (
            my_sql_datasource.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="taxi_multi_batch_sql_datasource",
                    data_connector_name="by_vendor_id",
                    data_asset_name="yellow_tripdata_sample_2020_01",
                    data_connector_query={"fake": "I_wont_work"},
                )
            )
        )

    # Test 2: Unrecognized custom_filter is not a function
    with pytest.raises(ge_exceptions.BatchFilterError):
        my_sql_datasource.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="taxi_multi_batch_sql_datasource",
                data_connector_name="by_vendor_id",
                data_asset_name="yellow_tripdata_sample_2020_01",
                data_connector_query={"custom_filter_function": "I_wont_work_either"},
            )
        )

    # Test 3: batch_identifiers is not dict
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        batch_definition_list = (
            my_sql_datasource.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="taxi_multi_batch_sql_datasource",
                    data_connector_name="by_vendor_id",
                    data_asset_name="yellow_tripdata_sample_2020_01",
                    data_connector_query={"batch_filter_parameters": 1},
                )
            )
        )

    returned = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={"batch_filter_parameters": {"vendor_id": 1}},
        )
    )
    assert len(returned) == 1


def test_data_connector_query_limit(create_db_and_instantiate_simple_sql_datasource):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )

    # no limit
    batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={"limit": None},
        )
    )
    assert len(batch_definition_list) == 3
    # proper limit
    batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={"limit": 2},
        )
    )
    assert len(batch_definition_list) == 2

    # illegal limit
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="taxi_multi_batch_sql_datasource",
                data_connector_name="by_vendor_id",
                data_asset_name="yellow_tripdata_sample_2020_01",
                data_connector_query={"limit": "apples"},
            )
        )


def test_data_connector_query_illegal_index_and_limit_combination(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )
    with pytest.raises(ge_exceptions.BatchFilterError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="taxi_multi_batch_sql_datasource",
                data_connector_name="by_vendor_id",
                data_asset_name="yellow_tripdata_sample_2020_01",
                data_connector_query={"index": 0, "limit": 1},
            )
        )


def test_data_connector_query_sorted_filtered_by_custom_filter(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            datetime.datetime.strptime(
                batch_identifiers["pickup_datetime"], "%Y-%m-%d %H"
            ).date()
            == datetime.datetime(2020, 1, 1).date()
        )

    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_pickup_date_time",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={"custom_filter_function": my_custom_batch_selector},
        )
    )
    assert len(returned_batch_definition_list) == 24
    expected_batch_definition = BatchDefinition(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="by_pickup_date_time",
        data_asset_name="yellow_tripdata_sample_2020_01",
        batch_identifiers=IDDict({"pickup_datetime": "2020-01-01 10"}),
    )
    assert expected_batch_definition in returned_batch_definition_list


def test_data_connector_query_sorted_filtered_by_custom_filter_with_index(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            datetime.datetime.strptime(
                batch_identifiers["pickup_datetime"], "%Y-%m-%d %H"
            ).date()
            == datetime.datetime(2020, 1, 1).date()
        )

    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_pickup_date_time",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={
                "custom_filter_function": my_custom_batch_selector,
                "index": "-1",
            },
        )
    )
    assert len(returned_batch_definition_list) == 1

    expected_batch_definition = BatchDefinition(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="by_pickup_date_time",
        data_asset_name="yellow_tripdata_sample_2020_01",
        batch_identifiers=IDDict({"pickup_datetime": "2020-01-01 23"}),
    )
    assert expected_batch_definition == returned_batch_definition_list[0]


def test_data_connector_query_sorted_filtered_by_custom_filter_with_index_as_slice_via_string_left_right_step(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )

    # Note that both a function and a lambda Callable types are acceptable as the definition of a custom filter.
    def my_custom_batch_selector(batch_identifiers: dict) -> bool:
        return (
            datetime.datetime.strptime(
                batch_identifiers["pickup_datetime"], "%Y-%m-%d %H"
            ).date()
            == datetime.datetime(2020, 1, 1).date()
        )

    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_pickup_date_time",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={
                "custom_filter_function": my_custom_batch_selector,
                "index": "0:4:3",
            },
        )
    )
    assert len(returned_batch_definition_list) == 2
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_pickup_date_time",
            data_asset_name="yellow_tripdata_sample_2020_01",
            batch_identifiers=IDDict({"pickup_datetime": "2020-01-01 00"}),
        ),
        BatchDefinition(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_pickup_date_time",
            data_asset_name="yellow_tripdata_sample_2020_01",
            batch_identifiers=IDDict({"pickup_datetime": "2020-01-01 03"}),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_data_connector_query_batch_identifiers_1_key(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_data_connector: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )
    # no limit
    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={
                "batch_filter_parameters": {"vendor_id": 1},
            },
        )
    )
    assert len(returned_batch_definition_list) == 1
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            batch_identifiers=IDDict({"vendor_id": 1}),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_data_connector_query_batch_identifiers_1_key_and_index(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )
    # no limit
    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            data_connector_query={
                "batch_filter_parameters": {"vendor_id": 2},
                "index": 0,
            },
        )
    )
    assert len(returned_batch_definition_list) == 1

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
            batch_identifiers=IDDict({"vendor_id": 2}),
        ),
    ]
    assert returned_batch_definition_list == expected


def test_data_connector_query_for_data_asset_name(
    create_db_and_instantiate_simple_sql_datasource,
):
    my_sql_datasource: SimpleSqlalchemyDatasource = (
        create_db_and_instantiate_simple_sql_datasource
    )
    # no limit
    returned_batch_definition_list: List[
        BatchDefinition
    ] = my_sql_datasource.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="taxi_multi_batch_sql_datasource",
            data_connector_name="by_vendor_id",
            data_asset_name="yellow_tripdata_sample_2020_01",
        )
    )
    assert len(returned_batch_definition_list) == 3
