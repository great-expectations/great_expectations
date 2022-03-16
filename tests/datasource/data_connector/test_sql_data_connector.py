import json
import random
from typing import Any

import pytest
from ruamel.yaml import YAML

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None
import great_expectations.exceptions as ge_exceptions
from great_expectations.validator.validator import Validator

yaml = YAML(typ="safe")


# TODO: <Alex>ALEX -- Some methods in this module are misplaced and/or provide no action; this must be repaired.</Alex>
def test_basic_self_check(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 8,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_date_column__A",
        #         "data_asset_name": "table_partitioned_by_date_column__A",
        #         "batch_identifiers": {"date": "2020-01-02"},
        #         "splitter_method": "_split_on_column_value",
        #         "splitter_kwargs": {"column_name": "date"},
        #     },
        # },
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

    assets:
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
                data_connector_query={
                    "batch_filter_parameters": {"date": "2020-01-01"}
                },
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
                data_connector_query={"batch_filter_parameters": {}},
            )
        )
    )
    assert len(batch_definition_list) == 30

    # Note: Abe 20201109: It would be nice to put in safeguards for mistakes like this.
    # In this case, "date" should go inside "batch_identifiers".
    # Currently, the method ignores "date" entirely, and matches on too many partitions.
    # I don't think this is unique to ConfiguredAssetSqlDataConnector.
    # with pytest.raises(ge_exceptions.DataConnectorError) as e:
    #     batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
    #         batch_request=BatchRequest(
    #             datasource_name="FAKE_Datasource_NAME",
    #             data_connector_name="my_sql_data_connector",
    #             data_asset_name="table_partitioned_by_date_column__A",
    #             data_connector_query={
    #                 "batch_filter_parameters": {},
    #                 "date" : "2020-01-01",
    #             }
    #         )
    #     )
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

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
            )
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(datasource_name="FAKE_Datasource_NAME")
        )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
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

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 8,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_date_column__A",
        #         "data_asset_name": "table_partitioned_by_date_column__A",
        #         "batch_identifiers": {"date": "2020-01-02"},
        #         "splitter_method": "_split_on_column_value",
        #         "splitter_kwargs": {"column_name": "date"},
        #     },
        # },
    }


def test_example_B(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 8,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_timestamp_column__B",
        #         "data_asset_name": "table_partitioned_by_timestamp_column__B",
        #         "batch_identifiers": {"timestamp": "2020-01-02"},
        #         "splitter_method": "_split_on_converted_datetime",
        #         "splitter_kwargs": {"column_name": "timestamp"},
        #     },
        # },
    }


def test_example_C(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 10,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
        #         "data_asset_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
        #         "batch_identifiers": {"id": 1},
        #         "splitter_method": "_split_on_divided_integer",
        #         "splitter_kwargs": {"column_name": "id", "divisor": 10},
        #     },
        # },
    }


def test_example_E(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 9,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_incrementing_batch_id__E",
        #         "data_asset_name": "table_partitioned_by_incrementing_batch_id__E",
        #         "batch_identifiers": {"batch_id": 1},
        #         "splitter_method": "_split_on_column_value",
        #         "splitter_kwargs": {"column_name": "batch_id"},
        #     },
        # },
    }


def test_example_F(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
                    {"session_id": 2},
                    {"session_id": 3},
                    {"session_id": 4},
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 2,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_foreign_key__F",
        #         "data_asset_name": "table_partitioned_by_foreign_key__F",
        #         "batch_identifiers": {"session_id": 2},
        #         "splitter_method": "_split_on_column_value",
        #         "splitter_kwargs": {"column_name": "session_id"},
        #     },
        # },
    }


def test_example_G(test_cases_for_sql_data_connector_sqlite_execution_engine):
    random.seed(0)
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "n_rows": 8,
        #     "batch_spec": {
        #         "table_name": "table_partitioned_by_multiple_columns__G",
        #         "data_asset_name": "table_partitioned_by_multiple_columns__G",
        #         "batch_identifiers": {
        #             "y": 2020,
        #             "m": 1,
        #             "d": 2,
        #         },
        #         "splitter_method": "_split_on_multi_column_values",
        #         "splitter_kwargs": {"column_names": ["y", "m", "d"]},
        #     },
        # },
    }


def test_example_H(test_cases_for_sql_data_connector_sqlite_execution_engine):
    return

    # Leaving this test commented for now, since sqlite doesn't support MD5.
    # Later, we'll want to add a more thorough test harness, including other databases.

    # db = test_cases_for_sql_data_connector_sqlite_execution_engine

    # config = yaml.load("""
    # name: my_sql_data_connector
    # datasource_name: FAKE_Datasource_NAME

    # assets:
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


#  ['table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D',
#  'table_containing_id_spacers_for_D',
#  'table_that_should_be_partitioned_by_random_hash__H']


def test_sampling_method__limit(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
                "sampling_method": "_sample_using_limit",
                "sampling_kwargs": {"n": 20},
            }
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])
    assert len(validator.head(fetch_all=True)) == 20

    assert not validator.expect_column_values_to_be_in_set(
        "date", value_set=["2020-01-02"]
    ).success


def test_sampling_method__random(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    # noinspection PyUnusedLocal
    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
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
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
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
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 12


def test_sampling_method__a_list(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
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
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 4


def test_sampling_method__md5(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    # noinspection PyUnusedLocal
    execution_engine = test_cases_for_sql_data_connector_sqlite_execution_engine

    # SQlite doesn't support MD5
    # batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
    #     batch_spec=SqlAlchemyDatasourceBatchSpec({
    #         "table_name": "table_partitioned_by_date_column__A",
    #         "batch_identifiers": {},
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
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "sampling_method": "_sample_using_mod",
                "sampling_kwargs": {
                    "column_name": "id",
                    "mod": 10,
                    "value": 8,
                },
            }
        )
    )
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 12

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
            }
        )
    )
    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 120

    batch_data, batch_markers = execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            {
                "table_name": "table_partitioned_by_date_column__A",
                "batch_identifiers": {},
                "splitter_method": "_split_on_whole_table",
                "splitter_kwargs": {},
            }
        )
    )

    execution_engine.load_batch_data("__", batch_data)
    validator = Validator(execution_engine)
    assert len(validator.head(fetch_all=True)) == 120


def test_default_behavior_with_no_splitter(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
        table_partitioned_by_date_column__A: {}
    """,
    )
    config["execution_engine"] = db

    my_data_connector = ConfiguredAssetSqlDataConnector(**config)
    report_object = my_data_connector.self_check()
    print(json.dumps(report_object, indent=2))

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query={},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query={"batch_filter_parameters": {}},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}


def test_behavior_with_whole_table_splitter(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    db = test_cases_for_sql_data_connector_sqlite_execution_engine

    config = yaml.load(
        """
    name: my_sql_data_connector
    datasource_name: FAKE_Datasource_NAME

    assets:
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
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query={},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_Datasource_NAME",
                data_connector_name="my_sql_data_connector",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query={"batch_filter_parameters": {}},
            )
        )
    )
    assert len(batch_definition_list) == 1
    assert batch_definition_list[0]["batch_identifiers"] == {}


def test_basic_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_prefix": "prexif__",
            "data_asset_name_suffix": "__xiffus",
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    report_object = my_data_connector.self_check()
    # print(json.dumps(report_object, indent=4))
    assert report_object == {
        "class_name": "InferredAssetSqlDataConnector",
        "data_asset_count": 21,
        "example_data_asset_names": [
            "prexif__table_containing_id_spacers_for_D__xiffus",
            "prexif__table_full__I__xiffus",
            "prexif__table_partitioned_by_date_column__A__xiffus",
        ],
        "data_assets": {
            "prexif__table_containing_id_spacers_for_D__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prexif__table_full__I__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prexif__table_partitioned_by_date_column__A__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "batch_spec": {
        #         "schema_name": "main",
        #         "table_name": "table_containing_id_spacers_for_D",
        #         "data_asset_name": "prexif__table_containing_id_spacers_for_D__xiffus",
        #         "batch_identifiers": {},
        #     },
        #     "n_rows": 30,
        # },
    }

    assert my_data_connector.get_available_data_asset_names() == [
        "prexif__table_containing_id_spacers_for_D__xiffus",
        "prexif__table_full__I__xiffus",
        "prexif__table_partitioned_by_date_column__A__xiffus",
        "prexif__table_partitioned_by_foreign_key__F__xiffus",
        "prexif__table_partitioned_by_incrementing_batch_id__E__xiffus",
        "prexif__table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__table_partitioned_by_multiple_columns__G__xiffus",
        "prexif__table_partitioned_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__table_partitioned_by_timestamp_column__B__xiffus",
        "prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__table_with_fk_reference_from_F__xiffus",
        "prexif__view_by_date_column__A__xiffus",
        "prexif__view_by_incrementing_batch_id__E__xiffus",
        "prexif__view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__view_by_multiple_columns__G__xiffus",
        "prexif__view_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__view_by_timestamp_column__B__xiffus",
        "prexif__view_containing_id_spacers_for_D__xiffus",
        "prexif__view_partitioned_by_foreign_key__F__xiffus",
        "prexif__view_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__view_with_fk_reference_from_F__xiffus",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        )
    )
    assert len(batch_definition_list) == 1


def test_more_complex_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_suffix": "__whole",
            "include_schema_name": True,
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    report_object = my_data_connector.self_check()

    assert report_object == {
        "class_name": "InferredAssetSqlDataConnector",
        "data_asset_count": 21,
        "data_assets": {
            "main.table_containing_id_spacers_for_D__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "main.table_full__I__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "main.table_partitioned_by_date_column__A__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "example_data_asset_names": [
            "main.table_containing_id_spacers_for_D__whole",
            "main.table_full__I__whole",
            "main.table_partitioned_by_date_column__A__whole",
        ],
        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
        # "example_data_reference": {
        #     "batch_spec": {
        #         "batch_identifiers": {},
        #         "schema_name": "main",
        #         "table_name": "table_containing_id_spacers_for_D",
        #         "data_asset_name": "main.table_containing_id_spacers_for_D__whole",
        #     },
        #     "n_rows": 30,
        # },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    assert my_data_connector.get_available_data_asset_names() == [
        "main.table_containing_id_spacers_for_D__whole",
        "main.table_full__I__whole",
        "main.table_partitioned_by_date_column__A__whole",
        "main.table_partitioned_by_foreign_key__F__whole",
        "main.table_partitioned_by_incrementing_batch_id__E__whole",
        "main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.table_partitioned_by_multiple_columns__G__whole",
        "main.table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.table_partitioned_by_timestamp_column__B__whole",
        "main.table_that_should_be_partitioned_by_random_hash__H__whole",
        "main.table_with_fk_reference_from_F__whole",
        "main.view_by_date_column__A__whole",
        "main.view_by_incrementing_batch_id__E__whole",
        "main.view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.view_by_multiple_columns__G__whole",
        "main.view_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.view_by_timestamp_column__B__whole",
        "main.view_containing_id_spacers_for_D__whole",
        "main.view_partitioned_by_foreign_key__F__whole",
        "main.view_that_should_be_partitioned_by_random_hash__H__whole",
        "main.view_with_fk_reference_from_F__whole",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="main.table_that_should_be_partitioned_by_random_hash__H__whole",
        )
    )
    assert len(batch_definition_list) == 1


def test_basic_instantiation_of_ConfiguredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "ConfiguredAssetSqlDataConnector",
            "name": "my_sql_data_connector",
            "assets": {"main.table_full__I__whole": {}},
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    report_object = my_data_connector.self_check()

    assert report_object == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["main.table_full__I__whole"],
        "data_assets": {
            "main.table_full__I__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "ConfiguredAssetSqlDataConnector",
            "name": "my_sql_data_connector",
            "assets": {
                "main.table_partitioned_by_date_column__A": {
                    "splitter_method": "_split_on_column_value",
                    "splitter_kwargs": {"column_name": "date"},
                },
            },
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    report_object = my_data_connector.self_check()
    assert report_object == {
        "class_name": "ConfiguredAssetSqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["main.table_partitioned_by_date_column__A"],
        "data_assets": {
            "main.table_partitioned_by_date_column__A": {
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
    }


def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine="test_cases_for_sql_data_connector_sqlite_execution_engine",
        assets={
            "table_partitioned_by_date_column__A": {
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
            },
        },
    )
    assert "main.table_partitioned_by_date_column__A" in my_data_connector.assets

    # schema_name given but include_schema_name is set to False
    with pytest.raises(ge_exceptions.DataConnectorError) as e:
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine="test_cases_for_sql_data_connector_sqlite_execution_engine",
            assets={
                "table_partitioned_by_date_column__A": {
                    "splitter_method": "_split_on_column_value",
                    "splitter_kwargs": {"column_name": "date"},
                    "include_schema_name": False,
                    "schema_name": "main",
                },
            },
        )

    assert (
        e.value.message
        == "ConfiguredAssetSqlDataConnector ran into an error while initializing Asset names. Schema main was specified, but 'include_schema_name' flag was set to False."
    )


def test_more_complex_instantiation_of_ConfiguredAssetSqlDataConnector_include_schema_name_prefix_suffix(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name="my_sql_data_connector",
        datasource_name="my_test_datasource",
        execution_engine="test_cases_for_sql_data_connector_sqlite_execution_engine",
        assets={
            "table_partitioned_by_date_column__A": {
                "splitter_method": "_split_on_column_value",
                "splitter_kwargs": {"column_name": "date"},
                "include_schema_name": True,
                "schema_name": "main",
                "data_asset_name_prefix": "taxi__",
                "data_asset_name_suffix": "__asset",
            },
        },
    )
    assert (
        "taxi__main.table_partitioned_by_date_column__A__asset"
        in my_data_connector.assets
    )

    # schema_name provided, but include_schema_name is set to False
    with pytest.raises(ge_exceptions.DataConnectorError) as e:
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine="test_cases_for_sql_data_connector_sqlite_execution_engine",
            assets={
                "table_partitioned_by_date_column__A": {
                    "splitter_method": "_split_on_column_value",
                    "splitter_kwargs": {"column_name": "date"},
                    "include_schema_name": False,
                    "schema_name": "main",
                    "data_asset_name_prefix": "taxi__",
                    "data_asset_name_suffix": "__asset",
                },
            },
        )
    assert (
        e.value.message
        == "ConfiguredAssetSqlDataConnector ran into an error while initializing Asset names. Schema main was specified, but 'include_schema_name' flag was set to False."
    )


# TODO
def test_ConfiguredAssetSqlDataConnector_with_sorting(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    pass


def test_update_configured_asset_sql_data_connector_missing_data_asset(
    sqlalchemy_missing_data_asset_data_context,
):
    context: DataContext = sqlalchemy_missing_data_asset_data_context

    data_asset_name: str = "table_containing_id_spacers_for_D"

    batch_request: dict[str, Any] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "default_configured_data_connector",
        "data_asset_name": data_asset_name,
        "limit": 1000,
    }

    expectation_suite_name: str = "test"

    context.create_expectation_suite(expectation_suite_name=expectation_suite_name)

    batch_request: BatchRequest = BatchRequest(**batch_request)

    validator: Validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )

    assert (
        validator.batches[validator.active_batch_id].batch_definition.data_asset_name
        == data_asset_name
    )
