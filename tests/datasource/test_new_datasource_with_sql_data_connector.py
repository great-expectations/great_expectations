from __future__ import annotations

import logging
import os
import random
from typing import Optional, Union

import pandas as pd
import pytest
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.datasource import (
    BaseDatasource,
    LegacyDatasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.exceptions.exceptions import ExecutionEngineError
from great_expectations.validator.validator import Validator

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as sqla_bigquery,
)

yaml = YAMLHandler()


logger = logging.getLogger(__name__)


@pytest.fixture
def fake_private_key() -> bytes:
    # We pick the smallest public_exponent and key_size to do the least amount of work in tests.
    key = rsa.generate_private_key(public_exponent=3, key_size=512)
    return key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption(),
    )


@pytest.fixture
def data_context_with_sql_data_connectors_including_schema_for_testing_get_batch(
    sa,
    empty_data_context,
    test_db_connection_string,
):
    context: FileDataContext = empty_data_context

    sqlite_engine: sa.engine.base.Engine = sa.create_engine(test_db_connection_string)
    # noinspection PyUnusedLocal
    conn: sa.engine.base.Connection = sqlite_engine.connect()  # noqa: F841
    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: SqlAlchemyExecutionEngine
            connection_string: {test_db_connection_string}

        data_connectors:
            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
            my_inferred_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: InferredAssetSqlDataConnector
                include_schema_name: true
            my_configured_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: ConfiguredAssetSqlDataConnector
                assets:
                    my_first_data_asset:
                        table_name: table_1
                    my_second_data_asset:
                        schema_name: main
                        table_name: table_2
                    table_1: {{}}
                    table_2:
                        include_schema_name: True
                        schema_name: main
    """

    try:
        # noinspection PyUnusedLocal
        my_sql_datasource: Optional[  # noqa: F841
            Union[SimpleSqlalchemyDatasource, LegacyDatasource]
        ] = context.add_datasource(
            "test_sqlite_db_datasource", **yaml.load(datasource_config)
        )
    except AttributeError:
        pytest.skip("SQL Database tests require sqlalchemy to be installed.")

    return context


@pytest.mark.sqlite
def test_basic_instantiation_with_ConfiguredAssetSqlDataConnector_splitting(sa):
    random.seed(0)

    db_file = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    # This is a basic integration test demonstrating a Datasource containing a SQL data_connector
    # It also shows how to instantiate a SQLite SqlAlchemyExecutionEngine
    config = yaml.load(
        f"""
class_name: Datasource

execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: sqlite:///{db_file}

data_connectors:
    my_sqlite_db:
        class_name: ConfiguredAssetSqlDataConnector

        assets:
            table_partitioned_by_date_column__A:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
    """,
    )

    my_data_source = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_sql_datasource"},
    )

    report = my_data_source.self_check()
    report["execution_engine"].pop("connection_string")
    assert report == {
        "execution_engine": {
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["table_partitioned_by_date_column__A"],
                "data_assets": {
                    "table_partitioned_by_date_column__A": {
                        "batch_definition_count": 9,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
                # "example_data_reference": {
                #     "batch_spec": {
                #         "table_name": "table_partitioned_by_date_column__A",
                #         "data_asset_name": "table_partitioned_by_date_column__A",
                #         "batch_identifiers": {"date": "2020-01"},
                #         "splitter_method": "_split_on_converted_datetime",
                #         "splitter_kwargs": {
                #             "column_name": "date",
                #             "date_format_string": "%Y-%W",
                #         },
                #     },
                #     "n_rows": 24,
                # },
            },
        },
    }


@pytest.mark.sqlite
def test_instantiation_with_ConfiguredAssetSqlDataConnector_round_trip_to_config_splitting_and_sampling(
    sa, empty_data_context
):
    # This is a basic integration test demonstrating a Datasource containing a SQL data_connector.
    # It tests that splitter configurations can be saved and loaded to great_expectations.yml by performing a
    # round-trip to the configuration.
    context: FileDataContext = empty_data_context
    db_file: Union[bytes, str] = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    config: str = f"""
    name: my_datasource
    class_name: Datasource

    execution_engine:
        class_name: SqlAlchemyExecutionEngine
        connection_string: sqlite:///{db_file}

    data_connectors:
        my_sqlite_db:
            class_name: ConfiguredAssetSqlDataConnector

            assets:
                table_partitioned_by_date_column__A:
                    splitter_method: _split_on_converted_datetime
                    splitter_kwargs:
                        column_name: date
                        date_format_string: "%Y-%W"
                    sampling_method: sample_using_limit
                    sampling_kwargs:
                        n: 10
    """
    context.add_datasource(**yaml.load(config))
    datasource: Union[LegacyDatasource, BaseDatasource, None] = context.get_datasource(
        datasource_name="my_datasource"
    )
    report: dict = datasource.self_check()
    report["execution_engine"].pop("connection_string")
    assert report == {
        "execution_engine": {
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["table_partitioned_by_date_column__A"],
                "data_assets": {
                    "table_partitioned_by_date_column__A": {
                        "batch_definition_count": 9,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
        },
    }


@pytest.mark.sqlite
def test_basic_instantiation_with_InferredAssetSqlDataConnector_splitting(sa):
    # This is a basic integration test demonstrating a Datasource containing a SQL data_connector.
    # It tests that splitter configurations can be saved and loaded to great_expectations.yml by performing a
    # round-trip to the configuration.
    random.seed(0)

    db_file: Union[bytes, str] = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    # This is a basic integration test demonstrating an Datasource containing a SQL data_connector
    # It also shows how to instantiate a SQLite SqlAlchemyExecutionEngine

    config = yaml.load(
        f"""
class_name: Datasource

execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: sqlite:///{db_file}

data_connectors:
    my_sqlite_db:
        class_name: InferredAssetSqlDataConnector
        name: whole_table
        data_asset_name_prefix: prefix__
        data_asset_name_suffix: __xiffus
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%W"
    """,
    )

    my_data_source = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_sql_datasource"},
    )
    report = my_data_source.self_check()
    report["execution_engine"].pop("connection_string")

    assert report == {
        "execution_engine": {
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "InferredAssetSqlDataConnector",
                "data_asset_count": 6,
                "example_data_asset_names": [
                    "prefix__table_containing_id_spacers_for_D__xiffus",
                    "prefix__table_full__I__xiffus",
                    "prefix__table_partitioned_by_date_column__A__xiffus",
                ],
                "data_assets": {
                    "prefix__table_containing_id_spacers_for_D__xiffus": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                    "prefix__table_full__I__xiffus": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                    "prefix__table_partitioned_by_date_column__A__xiffus": {
                        "batch_definition_count": 9,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
        },
    }


@pytest.mark.sqlite
def test_instantiation_with_InferredAssetSqlDataConnector_round_trip_to_config_splitting_and_sampling(
    sa, empty_data_context
):
    # This is a basic integration test demonstrating a Datasource containing a SQL data_connector.
    # It tests that splitter configurations can be saved and loaded to great_expectations.yml by performing a
    # round-trip to the configuration.
    context: FileDataContext = empty_data_context
    db_file: Union[bytes, str] = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    config: str = f"""
    name: my_datasource
    class_name: Datasource

    execution_engine:
        class_name: SqlAlchemyExecutionEngine
        connection_string: sqlite:///{db_file}

    data_connectors:
        my_sqlite_db:
            class_name: InferredAssetSqlDataConnector
            name: whole_table
            data_asset_name_prefix: prefix__
            data_asset_name_suffix: __xiffus
            splitter_method: _split_on_converted_datetime
            splitter_kwargs:
                column_name: date
                date_format_string: "%Y-%W"
            sampling_method: sample_using_limit
            sampling_kwargs:
                n: 10
        """
    context.add_datasource(**yaml.load(config))
    datasource: Union[LegacyDatasource, BaseDatasource, None] = context.get_datasource(
        datasource_name="my_datasource"
    )
    report: dict = datasource.self_check()
    report["execution_engine"].pop("connection_string")
    assert report == {
        "execution_engine": {
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "InferredAssetSqlDataConnector",
                "data_asset_count": 6,
                "example_data_asset_names": [
                    "prefix__table_containing_id_spacers_for_D__xiffus",
                    "prefix__table_full__I__xiffus",
                    "prefix__table_partitioned_by_date_column__A__xiffus",
                ],
                "data_assets": {
                    "prefix__table_containing_id_spacers_for_D__xiffus": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                    "prefix__table_full__I__xiffus": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                    "prefix__table_partitioned_by_date_column__A__xiffus": {
                        "batch_definition_count": 9,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    },
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
        },
    }


@pytest.mark.sqlite
def test_SimpleSqlalchemyDatasource(empty_data_context):
    context = empty_data_context
    # This test mirrors the likely path to configure a SimpleSqlalchemyDatasource

    db_file = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )

    # Absolutely minimal starting config
    datasource_with_minimum_config = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table: {}
"""
    )

    assert datasource_with_minimum_config.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_partitioned_by_foreign_key__F",
            "table_partitioned_by_incrementing_batch_id__E",
            "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "table_partitioned_by_multiple_columns__G",
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "table_partitioned_by_timestamp_column__B",
            "table_that_should_be_partitioned_by_random_hash__H",
            "table_with_fk_reference_from_F",
            "view_by_date_column__A",
            "view_by_incrementing_batch_id__E",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "view_by_multiple_columns__G",
            "view_by_regularly_spaced_incrementing_id_column__C",
            "view_by_timestamp_column__B",
            "view_containing_id_spacers_for_D",
            "view_partitioned_by_foreign_key__F",
            "view_that_should_be_partitioned_by_random_hash__H",
            "view_with_fk_reference_from_F",
        ]
    }

    assert datasource_with_minimum_config.get_available_data_asset_names_and_types() == {
        "whole_table": [
            ("table_containing_id_spacers_for_D", "table"),
            ("table_full__I", "table"),
            ("table_partitioned_by_date_column__A", "table"),
            ("table_partitioned_by_foreign_key__F", "table"),
            ("table_partitioned_by_incrementing_batch_id__E", "table"),
            (
                "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "table",
            ),
            ("table_partitioned_by_multiple_columns__G", "table"),
            (
                "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
                "table",
            ),
            ("table_partitioned_by_timestamp_column__B", "table"),
            ("table_that_should_be_partitioned_by_random_hash__H", "table"),
            ("table_with_fk_reference_from_F", "table"),
            ("view_by_date_column__A", "view"),
            ("view_by_incrementing_batch_id__E", "view"),
            (
                "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "view",
            ),
            ("view_by_multiple_columns__G", "view"),
            ("view_by_regularly_spaced_incrementing_id_column__C", "view"),
            ("view_by_timestamp_column__B", "view"),
            ("view_containing_id_spacers_for_D", "view"),
            ("view_partitioned_by_foreign_key__F", "view"),
            ("view_that_should_be_partitioned_by_random_hash__H", "view"),
            ("view_with_fk_reference_from_F", "view"),
        ]
    }

    # Here we should test getting a batch

    # Very thin starting config
    datasource_with_name_suffix = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table:
        data_asset_name_suffix: __whole_table
        introspection_directives: {}
"""
    )

    assert datasource_with_name_suffix.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D__whole_table",
            "table_full__I__whole_table",
            "table_partitioned_by_date_column__A__whole_table",
            "table_partitioned_by_foreign_key__F__whole_table",
            "table_partitioned_by_incrementing_batch_id__E__whole_table",
            "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole_table",
            "table_partitioned_by_multiple_columns__G__whole_table",
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole_table",
            "table_partitioned_by_timestamp_column__B__whole_table",
            "table_that_should_be_partitioned_by_random_hash__H__whole_table",
            "table_with_fk_reference_from_F__whole_table",
            "view_by_date_column__A__whole_table",
            "view_by_incrementing_batch_id__E__whole_table",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole_table",
            "view_by_multiple_columns__G__whole_table",
            "view_by_regularly_spaced_incrementing_id_column__C__whole_table",
            "view_by_timestamp_column__B__whole_table",
            "view_containing_id_spacers_for_D__whole_table",
            "view_partitioned_by_foreign_key__F__whole_table",
            "view_that_should_be_partitioned_by_random_hash__H__whole_table",
            "view_with_fk_reference_from_F__whole_table",
        ]
    }

    assert datasource_with_name_suffix.get_available_data_asset_names_and_types() == {
        "whole_table": [
            ("table_containing_id_spacers_for_D", "table"),
            ("table_full__I", "table"),
            ("table_partitioned_by_date_column__A", "table"),
            ("table_partitioned_by_foreign_key__F", "table"),
            ("table_partitioned_by_incrementing_batch_id__E", "table"),
            (
                "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "table",
            ),
            ("table_partitioned_by_multiple_columns__G", "table"),
            (
                "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
                "table",
            ),
            ("table_partitioned_by_timestamp_column__B", "table"),
            ("table_that_should_be_partitioned_by_random_hash__H", "table"),
            ("table_with_fk_reference_from_F", "table"),
            ("view_by_date_column__A", "view"),
            ("view_by_incrementing_batch_id__E", "view"),
            (
                "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "view",
            ),
            ("view_by_multiple_columns__G", "view"),
            ("view_by_regularly_spaced_incrementing_id_column__C", "view"),
            ("view_by_timestamp_column__B", "view"),
            ("view_containing_id_spacers_for_D", "view"),
            ("view_partitioned_by_foreign_key__F", "view"),
            ("view_that_should_be_partitioned_by_random_hash__H", "view"),
            ("view_with_fk_reference_from_F", "view"),
        ]
    }

    # Here we should test getting a batch

    # Add some manually configured tables...
    datasource_manually_configured = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}

introspection:
    whole_table:
        excluded_tables:
            - main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D
            - main.table_partitioned_by_multiple_columns__G
            - main.table_partitioned_by_regularly_spaced_incrementing_id_column__C
            - main.table_partitioned_by_timestamp_column__B
            - main.table_that_should_be_partitioned_by_random_hash__H
            - main.table_with_fk_reference_from_F

    hourly:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: timestamp
            date_format_string: "%Y-%m-%d:%H"
        included_tables:
            - main.table_partitioned_by_timestamp_column__B
        introspection_directives:
            include_views: true


tables:
    table_partitioned_by_date_column__A:
        partitioners:
            daily:
                data_asset_name_suffix: __daily
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%m-%d"
            weekly:
                include_schema_name: False
                data_asset_name_prefix: some_string__
                data_asset_name_suffix: __some_other_string
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
            by_id_dozens:
                include_schema_name: True
                # Note: no data_asset_name_suffix
                splitter_method: _split_on_divided_integer
                splitter_kwargs:
                    column_name: id
                    divisor: 12
"""
    )

    assert datasource_manually_configured.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_partitioned_by_foreign_key__F",
            "table_partitioned_by_incrementing_batch_id__E",
            "view_by_date_column__A",
            "view_by_incrementing_batch_id__E",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "view_by_multiple_columns__G",
            "view_by_regularly_spaced_incrementing_id_column__C",
            "view_by_timestamp_column__B",
            "view_containing_id_spacers_for_D",
            "view_partitioned_by_foreign_key__F",
            "view_that_should_be_partitioned_by_random_hash__H",
            "view_with_fk_reference_from_F",
        ],
        "hourly": [
            "table_partitioned_by_timestamp_column__B",
        ],
        "daily": [
            "table_partitioned_by_date_column__A__daily",
        ],
        "weekly": [
            "some_string__table_partitioned_by_date_column__A__some_other_string",
        ],
        "by_id_dozens": [
            "table_partitioned_by_date_column__A",
        ],
    }

    # Here we should test getting another batch

    # Drop the introspection...
    datasource_without_introspection = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
tables:
    table_partitioned_by_date_column__A:
        partitioners:
            whole_table: {}
            daily:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%m-%d"
            weekly:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
            by_id_dozens:
                splitter_method: _split_on_divided_integer
                splitter_kwargs:
                    column_name: id
                    divisor: 12
"""
    )

    assert datasource_without_introspection.get_available_data_asset_names() == {
        "whole_table": [
            "table_partitioned_by_date_column__A",
        ],
        "daily": [
            "table_partitioned_by_date_column__A",
        ],
        "weekly": [
            "table_partitioned_by_date_column__A",
        ],
        "by_id_dozens": [
            "table_partitioned_by_date_column__A",
        ],
    }

    # Here we should test getting another batch


@pytest.mark.skipif(
    not sqla_bigquery,
    reason="sqlalchemy_bigquery is not installed",
)
@pytest.mark.big
def test_basic_instantiation_with_bigquery_creds(sa, fake_private_key):
    # bigquery driver is invoked upon datasource instantiation, and validates credentials_info
    instantiate_class_from_config(
        config={
            "connection_string": "bigquery://project-1353/dataset",
            "credentials_info": {
                "type": "service_account",
                "project_id": "project-1353",
                "private_key_id": "df87033061fd7c27dcc953e235fe099a7017f9c4",
                "private_key": fake_private_key,
                "client_email": "testme@project-1353.iam.gserviceaccount.com",
                "client_id": "100945395817716260007",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40project-1353.iam.gserviceaccount.com",
            },
        },
        config_defaults={
            "module_name": "great_expectations.datasource",
            "class_name": "SimpleSqlalchemyDatasource",
        },
        runtime_environment={"name": "my_sql_datasource"},
    )


@pytest.mark.big
def test_basic_instantiation_with_bigquery_creds_failure_pkey(sa):
    with pytest.raises(ExecutionEngineError):
        instantiate_class_from_config(
            config={
                "connection_string": "bigquery://project-1353/dataset",
                "credentials_info": {
                    "type": "service_account",
                    "project_id": "project-1353",
                    "private_key_id": "df87033061fd7c27dcc953e235fe099a7017f9c4",
                    "private_key": "bad_pkey",
                    "client_email": "testme@project-1353.iam.gserviceaccount.com",
                    "client_id": "100945395817716260007",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40project-1353.iam.gserviceaccount.com",
                },
            },
            config_defaults={
                "module_name": "great_expectations.datasource",
                "class_name": "SimpleSqlalchemyDatasource",
            },
            runtime_environment={"name": "my_sql_datasource"},
        )


@pytest.mark.sqlite
def test_skip_inapplicable_tables(empty_data_context):
    context = empty_data_context
    # This test mirrors the likely path to configure a SimpleSqlalchemyDatasource

    db_file = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )

    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
introspection:
    daily:
        skip_inapplicable_tables: true
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"
"""
    )
    assert my_sql_datasource.get_available_data_asset_names() == {
        "daily": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_with_fk_reference_from_F",
            "view_by_date_column__A",
            "view_with_fk_reference_from_F",
        ]
    }

    with pytest.raises(gx_exceptions.DatasourceInitializationError):
        # noinspection PyUnusedLocal
        my_sql_datasource = context.test_yaml_config(
            f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
introspection:
    daily:
        skip_inapplicable_tables: false
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"
    """
        )


@pytest.mark.big
def test_batch_request_sql_with_schema(
    data_context_with_sql_data_connectors_including_schema_for_testing_get_batch,
):
    context: FileDataContext = (
        data_context_with_sql_data_connectors_including_schema_for_testing_get_batch
    )

    df_table_expected_my_first_data_asset: pd.DataFrame = pd.DataFrame(
        {"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]}
    )
    df_table_expected_my_second_data_asset: pd.DataFrame = pd.DataFrame(
        {"col_1": [0, 1, 2, 3, 4], "col_2": ["b", "c", "d", "e", "f"]}
    )

    batch_request: dict
    validator: Validator
    df_table_actual: pd.DataFrame

    # Exercise RuntimeDataConnector using SQL query against database table with empty schema name.
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_runtime_data_connector",
        "data_asset_name": "test_asset",
        "runtime_parameters": {"query": "SELECT * FROM table_1"},
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
        },
    }
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise RuntimeDataConnector using SQL query against database table with non-empty ("main") schema name.
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_runtime_data_connector",
        "data_asset_name": "test_asset",
        "runtime_parameters": {"query": "SELECT * FROM main.table_2"},
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567891,
        },
    }
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise InferredAssetSqlDataConnector using data_asset_name introspected with schema from table, named "table_1".
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_inferred_data_connector",
        "data_asset_name": "main.table_1",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise InferredAssetSqlDataConnector using data_asset_name introspected with schema from table, named "table_2".
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_inferred_data_connector",
        "data_asset_name": "main.table_2",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "my_first_data_asset" (implicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "my_first_data_asset",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "my_second_data_asset" (implicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "my_second_data_asset",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "table_1" (explicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "table_1",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "main.table_2" (explicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "main.table_2",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)
