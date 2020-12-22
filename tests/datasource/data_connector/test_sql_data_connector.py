import json
import random

import pytest
from ruamel.yaml import YAML

from great_expectations.core.batch import BatchRequest, BatchSpec
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector

yaml = YAML()


def test_basic_self_check(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_date_column__A:
            #table_name: events # If table_name is omitted, then the table_name defaults to the asset name
            splitter_method: _split_on_column_value
            splitter_kwargs:
                column_name: date
    """,
    )
    config["execution_engine"] = execution_engine

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_date_column__A"],
        "data_assets": {
            "table_partitioned_by_date_column__A": {
                "batch_definition_count": 30,
                "example_data_references": [
                    {"date": "2020-01-01"},
                    {"date": "2020-01-02"},
                    {"date": "2020-01-03"},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 8,
            "batch_spec": {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {"date": "2020-01-02"},
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "date"},
            },
        },
    }


def test_get_batch_definition_list_from_batch_request(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_date_column__A:
            splitter_method: _split_on_column_value
            splitter_kwargs:
                column_name: date

    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)
    my_data_connector._refresh_data_references_cache()

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={"partition_identifiers": {"date": "2020-01-01"}},
            )
        )
    )
    assert len(batch_definition_list) == 1

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={"partition_identifiers": {}},
            )
        )
    )
    assert len(batch_definition_list) == 30

    # Note: Abe 20201109: It would be nice to put in safeguards for mistakes like this.
    # In this case, "date" should go inside "partition_identifiers".
    # Currently, the method ignores "date" entirely, and matches on too many partitions.
    # I don't think this is unique to ConfiguredAssetSqlDataConnector.
    # with pytest.raises(DataConnectorError) as e:
    #     batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
    #         batch_request=BatchRequest(
    #             datasource_name="FAKE_Datasource_NAME",
    #             data_connector_name="my_sql_data_connector",
    #             data_asset_name="table_partitioned_by_date_column__A",
    #             partition_request={
    #                 "partition_identifiers" : {},
    #                 "date" : "2020-01-01",
    #             }
    #     ))
    # assert "Unmatched key" in e.value.message

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )
    )
    assert len(batch_definition_list) == 30

    with pytest.raises(KeyError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
            )
        )

    with pytest.raises(KeyError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
            )
        )

    with pytest.raises(KeyError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest()
        )


def test_example_A(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_date_column__A:
            splitter_method: _split_on_column_value
            splitter_kwargs:
                column_name: date

    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_date_column__A"],
        "data_assets": {
            "table_partitioned_by_date_column__A": {
                "batch_definition_count": 30,
                "example_data_references": [
                    {"date": "2020-01-01"},
                    {"date": "2020-01-02"},
                    {"date": "2020-01-03"},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 8,
            "batch_spec": {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {"date": "2020-01-02"},
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "date"},
            },
        },
    }


def test_example_B(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_timestamp_column__B:
            splitter_method: _split_on_converted_datetime
            splitter_kwargs:
                column_name: timestamp
    """
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_timestamp_column__B"],
        "data_assets": {
            "table_partitioned_by_timestamp_column__B": {
                "batch_definition_count": 30,
                "example_data_references": [
                    {"timestamp": "2020-01-01"},
                    {"timestamp": "2020-01-02"},
                    {"timestamp": "2020-01-03"},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 8,
            "batch_spec": {
                "table_name": "table_partitioned_by_timestamp_column__B",
                "partition_definition": {"timestamp": "2020-01-02"},
                "splitter_method": "_split_on_converted_datetime",
                "splitter_kwargs": {"column_name": "timestamp"},
            },
        },
    }


def test_example_C(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_regularly_spaced_incrementing_id_column__C:
            splitter_method: _split_on_divided_integer
            splitter_kwargs:
                column_name: id
                divisor: 10
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C"
        ],
        "data_assets": {
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C": {
                "batch_definition_count": 12,
                "example_data_references": [
                    {"id": 0},
                    {"id": 1},
                    {"id": 2},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 10,
            "batch_spec": {
                "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
                "partition_definition": {"id": 1},
                "splitter_method": "_split_on_divided_integer",
                "splitter_kwargs": {"column_name": "id", "divisor": 10},
            },
        },
    }


def test_example_E(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_incrementing_batch_id__E:
            splitter_method: _split_on_column_value
            splitter_kwargs:
                column_name: batch_id
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_incrementing_batch_id__E"],
        "data_assets": {
            "table_partitioned_by_incrementing_batch_id__E": {
                "batch_definition_count": 11,
                "example_data_references": [
                    {"batch_id": 0},
                    {"batch_id": 1},
                    {"batch_id": 2},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 9,
            "batch_spec": {
                "table_name": "table_partitioned_by_incrementing_batch_id__E",
                "partition_definition": {"batch_id": 1},
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "batch_id"},
            },
        },
    }


def test_example_F(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_foreign_key__F:
            splitter_method: _split_on_column_value
            splitter_kwargs:
                column_name: session_id
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_foreign_key__F"],
        "data_assets": {
            "table_partitioned_by_foreign_key__F": {
                "batch_definition_count": 49,
                # TODO Abe 20201029 : These values should be sorted
                "example_data_references": [
                    {"session_id": 3},
                    {"session_id": 2},
                    {"session_id": 4},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 2,
            "batch_spec": {
                "table_name": "table_partitioned_by_foreign_key__F",
                "partition_definition": {"session_id": 2},
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "session_id"},
            },
        },
    }


def test_example_G(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_multiple_columns__G:
            splitter_method: _split_on_multi_column_values
            splitter_kwargs:
                column_names:
                    - y
                    - m
                    - d
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["table_partitioned_by_multiple_columns__G"],
        "data_assets": {
            "table_partitioned_by_multiple_columns__G": {
                "batch_definition_count": 30,
                # TODO Abe 20201029 : These values should be sorted
                "example_data_references": [
                    {"y": 2020, "m": 1, "d": 1},
                    {"y": 2020, "m": 1, "d": 2},
                    {"y": 2020, "m": 1, "d": 3},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "n_rows": 8,
            "batch_spec": {
                "table_name": "table_partitioned_by_multiple_columns__G",
                "partition_definition": {
                    "y": 2020,
                    "m": 1,
                    "d": 2,
                },
                "splitter_method": "_split_on_multi_column_values",
                "splitter_kwargs": {"column_names": ["y", "m", "d"]},
            },
        },
    }


def test_example_H(test_cases_for_sql_data_connector_sqlite_execution_engine):
    return

    # Leaving this test commented for now, since sqlite doesn't support MD5.
    # Later, we'll want to add a more thorough test harness, including other databases.

    # db = test_cases_for_sql_data_connector_sqlite_execution_engine

    # config = yaml.load("""
    # name: my_sql_data_connector
    # datasource_name: FAKE_Datasource_NAME

    # data_assets:
    #     table_that_should_be_partitioned_by_random_hash__H:
    #         splitter_method: _split_on_hashed_column
    #         splitter_kwargs:
    #             column_name: id
    #             hash_digits: 1
    # """)
    # config["execution_engine"] = db

    # my_data_connector = ConfiguredAssetSqlDataConnector(**config)

    # report = my_data_connector.self_check()
    # print(json.dumps(report, indent=2))

    # # TODO: Flesh this out once the implementation actually works to this point
    # assert report == {
    #     "class_name": "ConfiguredAssetSqlDataConnector",
    #     "data_asset_count": 1,
    #     "example_data_asset_names": [
    #         "table_that_should_be_partitioned_by_random_hash__H"
    #     ],
    #     "data_assets": {
    #         "table_that_should_be_partitioned_by_random_hash__H": {
    #             "batch_definition_count": 16,
    #             "example_data_references": [
    #                 0,
    #                 1,
    #                 2,
    #             ]
    #         }
    #     },
    #     "unmatched_data_reference_count": 0,
    #     "example_unmatched_data_references": []
    # }


#  'table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D',
#  'table_containing_id_spacers_for_D',
#  'table_that_should_be_partitioned_by_random_hash__H']


def test_sampling_method__limit(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_limit",
                "sampling_kwargs": {"n": 20},
            }
        )
    )
    assert len(batch_data.head(fetch_all=True)) == 20

    # TODO: Implement this test once get_batch_data_and_markers is returning a proper SqlAlchemyBatchData
    # batch_data.expect_column_values_to_be_in_set("date", values=["2020-01-02"])


def test_sampling_method__random(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_random",
                "sampling_kwargs": {"p": 1.0},
            }
        )
    )

    # random.seed() is no good here: the random number generator is in the database, not python
    # assert len(batch_data.head(fetch_all=True)) == 63
    pass


def test_sampling_method__mod(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
            }
        )
    )

    assert len(batch_data.head(fetch_all=True)) == 12


def test_sampling_method__a_list(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_a_list",
                "sampling_kwargs": {
                    "column_name": "id",
                    "value_list": [10, 20, 30, 40],
                },
            }
        )
    )

    assert len(batch_data.head(fetch_all=True)) == 4


def test_sampling_method__md5(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    # SQlite doesn't support MD5
    # batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
    #     batch_spec=BatchSpec({
    #         "table_name": "table_partitioned_by_date_column__A",
    #         "partition_definition": {},
    #         "splitter_method": "_split_on_whole_table",
    #         "splitter_kwargs": {},
    #         "sampling_method": "_sample_using_md5",
    #         "sampling_kwargs": {
    #             "column_name": "index",
    #         }
    #     })
    # )


def test_to_make_sure_splitter_and_sampler_methods_are_optional(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "sampling_method": "_sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
            }
        )
    )

    assert len(batch_data.head(fetch_all=True)) == 12

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
            }
        )
    )

    assert len(batch_data.head(fetch_all=True)) == 120

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=BatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "partition_definition": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
            }
        )
    )

    assert len(batch_data.head(fetch_all=True)) == 120


def test_default_behavior_with_no_splitter(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_date_column__A: {}
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)
    report_object = my_data_connector.self_check()
    print(json.dumps(report_object, indent=2))

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={"partition_identifiers": {}},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}


def test_behavior_with_whole_table_splitter(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    data_assets:
        table_partitioned_by_date_column__A:
            splitter_method : "_split_on_whole_table"
            splitter_kwargs : {}
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)
    report_object = my_data_connector.self_check()
    print(json.dumps(report_object, indent=2))

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request={"partition_identifiers": {}},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["partition_definition"] == {}
