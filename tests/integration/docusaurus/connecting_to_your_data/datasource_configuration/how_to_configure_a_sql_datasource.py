"""Example Script: How to configure a SQL Datasource

This example script is intended for use in documentation on how to configure a SQL Datasource.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource

To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_configure_a_sql_datasource" tests/integration/test_script_runner.py
```

To validate the snippets in this file, use the following console command:
```
yarn snippet-check ./tests/integration/docusaurus/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource.py
```
"""
from ruamel import yaml

import great_expectations as gx
from tests.integration.docusaurus.connecting_to_your_data.datasource_configuration.datasource_configuration_test_utilities import (
    is_subset,
)
from tests.integration.docusaurus.connecting_to_your_data.datasource_configuration.full_datasource_configurations import (
    get_full_config_sql_configured_datasource,
    get_full_config_sql_inferred_datasource__single_and_multi_batch,
    get_full_config_sql_inferred_datasource__single_batch_only,
    get_full_config_sql_runtime_datasource,
    get_partial_config_universal_datasource_config_elements,
)

CONNECTION_STRING = "sqlite:///data/yellow_tripdata_sample_2020_all_months_combined.db"
data_context: gx.DataContext = gx.get_context()


def validate_universal_config_elements():
    """Validates that the 'universal' configuration keys and values are in fact identical to the keys and values
    in all the full Spark configurations.
    """
    universal_elements = get_partial_config_universal_datasource_config_elements()
    is_subset(
        universal_elements,
        get_full_config_sql_inferred_datasource__single_and_multi_batch(),
    )
    is_subset(
        universal_elements, get_full_config_sql_inferred_datasource__single_batch_only()
    )
    is_subset(universal_elements, get_full_config_sql_configured_datasource())
    is_subset(universal_elements, get_full_config_sql_runtime_datasource())


def section_5_add_the_sqlalchemy_execution_engine_to_your_datasource_configuration():
    datasource_config: dict = {
        # <snippet name="sql datasource define execution_engine class_name and module_name">
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        # </snippet>
    }
    for full_config in (
        get_full_config_sql_configured_datasource(),
        get_full_config_sql_inferred_datasource__single_batch_only(),
        get_full_config_sql_inferred_datasource__single_and_multi_batch(),
        get_full_config_sql_runtime_datasource(),
    ):
        is_subset(datasource_config, full_config)

    # <snippet name="sql datasource configuration post execution engine defined">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            # <snippet name="sql datasource define CONNECTION_STRING">
            "connection_string": CONNECTION_STRING,
            # </snippet>
        },
    }
    # </snippet>
    for full_config in (
        get_full_config_sql_configured_datasource(),
        get_full_config_sql_inferred_datasource__single_batch_only(),
        get_full_config_sql_inferred_datasource__single_and_multi_batch(),
        get_full_config_sql_runtime_datasource(),
    ):
        is_subset(datasource_config, full_config)


def section_6_add_a_dictionary_as_the_value_of_the_data_connectors_key():
    # <snippet name="sql datasource configuration post execution engine defined with empty data_connectors">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {},
    }
    # </snippet>
    for full_config in (
        get_full_config_sql_configured_datasource(),
        get_full_config_sql_inferred_datasource__single_batch_only(),
        get_full_config_sql_inferred_datasource__single_and_multi_batch(),
        get_full_config_sql_runtime_datasource(),
    ):
        is_subset(datasource_config, full_config)


def section_7_configure_your_individual_data_connectors__inferred():
    # <snippet name="sql datasource configuration with empty inferred sql data_connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {},
        },
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_sql_inferred_datasource__single_batch_only()
    )

    # <snippet name="inferred sql datasource configuration with data_connector class_name defined">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="define data_connector class_name for inferred sql datasource">
                "class_name": "InferredAssetSqlDataConnector",
                # </snippet>
            },
        },
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_sql_inferred_datasource__single_batch_only()
    )


def section_7_configure_your_individual_data_connectors__configured():
    # <snippet name="sql datasource configuration with empty configured sql data_connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {},
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_sql_configured_datasource())

    # <snippet name="configured sql datasource configuration with data_connector class_name defined">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                # <snippet name="define data_connector class_name for configured sql datasource">
                "class_name": "ConfiguredAssetSqlDataConnector",
                # </snippet>
                "assets": {},
            },
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_sql_configured_datasource())


def section_7_configure_your_individual_data_connectors__runtime():
    # <snippet name="sql datasource configuration with empty runtime sql data_connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {},
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_sql_runtime_datasource())

    # <snippet name="runtime sql datasource configuration with data_connector class_name defined">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                # <snippet name="define data_connector class_name for runtime sql datasource">
                "class_name": "RuntimeDataConnector",
                # </snippet>
                "batch_identifiers": {},
            },
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_sql_runtime_datasource())


def section_8_configure_your_data_connectors_data_assets__inferred():
    # <snippet name="full configuration for sql inferred Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            # <snippet name="inferred sql data asset single batch">
            "inferred_data_connector_single_batch_asset": {
                "class_name": "InferredAssetSqlDataConnector",
            },
            # </snippet>
            # <snippet name="inferred sql data asset multi batch">
            "inferred_data_connector_multi_batch_asset_split_on_date_time": {
                "class_name": "InferredAssetSqlDataConnector",
                "splitter_method": "split_on_year_and_month",
                "splitter_kwargs": {
                    "column_name": "pickup_datetime",
                },
            },
            # </snippet>
        },
    }
    # </snippet>
    is_subset(
        datasource_config,
        get_full_config_sql_inferred_datasource__single_and_multi_batch(),
    )


def section_8_configure_your_data_connectors_data_assets__configured():
    # <snippet name="full configuration for sql configured Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    # <snippet name="configured sql data asset single batch">
                    "yellow_tripdata_sample_2020_full": {
                        "table_name": "yellow_tripdata_sample_2020",
                        "schema_name": "main",
                    },
                    # </snippet>
                    # <snippet name="configured sql data asset multi batch">
                    "yellow_tripdata_sample_2020_by_year_and_month": {
                        "table_name": "yellow_tripdata_sample_2020",
                        "schema_name": "main",
                        "splitter_method": "split_on_year_and_month",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                    # </snippet>
                },
            },
        },
    }
    # </snippet>
    is_subset(
        datasource_config,
        get_full_config_sql_configured_datasource(),
    )


def section_8_configure_your_data_connectors_data_assets__runtime():
    # <snippet name="full configuration for sql runtime Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                # <snippet name="runtime sql data asset define batch_identifiers">
                "batch_identifiers": ["batch_timestamp"],
                # </snippet>
            },
        },
    }
    # </snippet>
    is_subset(
        datasource_config,
        get_full_config_sql_runtime_datasource(),
    )


def section_9_test_your_configuration__inferred_and_configured():
    for datasource_config, connector_name, asset_count, asset_name, batch_count in (
        (
            get_full_config_sql_inferred_datasource__single_and_multi_batch(),
            "inferred_data_connector_single_batch_asset",
            1,
            "yellow_tripdata_sample_2020",
            1,
        ),
        (
            get_full_config_sql_inferred_datasource__single_and_multi_batch(),
            "inferred_data_connector_multi_batch_asset_split_on_date_time",
            1,
            "yellow_tripdata_sample_2020",
            12,
        ),
        (
            get_full_config_sql_configured_datasource(),
            "name_of_my_configured_data_connector",
            2,
            "yellow_tripdata_sample_2020_full",
            1,
        ),
        (
            get_full_config_sql_configured_datasource(),
            "name_of_my_configured_data_connector",
            2,
            "yellow_tripdata_sample_2020_by_year_and_month",
            12,
        ),
    ):

        test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
        datasource_check = test_result.self_check(max_examples=12)
        assert (
            datasource_check["data_connectors"][connector_name]["data_asset_count"]
            == asset_count
        ), f"{connector_name} {asset_count} != {datasource_check['data_connectors'][connector_name]['data_asset_count']}"
        assert (
            datasource_check["data_connectors"][connector_name]["data_assets"][
                asset_name
            ]["batch_definition_count"]
            == batch_count
        ), f"{connector_name} {batch_count} batches != {datasource_check['data_connectors'][connector_name]['data_assets'][asset_name]['batch_definition_count']}"


def section_9_test_your_configuration__runtime():
    # Testing the runtime data connector (there will be no data assets, those are passed in by the RuntimeBatchRequest.)
    datasource_config = get_full_config_sql_runtime_datasource()
    connector_name = "name_of_my_runtime_data_connector"
    asset_count = 0

    test_result = (
        # <snippet name="test your sql datasource_config with test_yaml_config">
        data_context.test_yaml_config(yaml.dump(datasource_config))
        # </snippet>
    )
    datasource_check = test_result.self_check(max_examples=12)
    assert (
        datasource_check["data_connectors"][connector_name]["data_asset_count"]
        == asset_count
    ), f"{connector_name} {asset_count} != {datasource_check['data_connectors'][connector_name]['data_asset_count']}"


def run_tests():
    # Test to verify that the universal config elements are consistent in each of the SQL config examples.
    validate_universal_config_elements()

    # Test to verify that the sql specific config examples are consistent in each of the SQL config examples.
    section_5_add_the_sqlalchemy_execution_engine_to_your_datasource_configuration()
    section_6_add_a_dictionary_as_the_value_of_the_data_connectors_key()

    # Test to verify that the inferred config examples are consistent with each other.
    section_7_configure_your_individual_data_connectors__inferred()
    section_8_configure_your_data_connectors_data_assets__inferred()

    # Test to verify that the configured config examples are consistent with each other.
    section_7_configure_your_individual_data_connectors__configured()
    section_8_configure_your_data_connectors_data_assets__configured()

    # Test to verify that the runtime config examples are consistent with each other.
    section_7_configure_your_individual_data_connectors__runtime()
    section_8_configure_your_data_connectors_data_assets__runtime()

    # Test to verify that the full configuration examples are functional.
    section_9_test_your_configuration__inferred_and_configured()
    section_9_test_your_configuration__runtime()


run_tests()
