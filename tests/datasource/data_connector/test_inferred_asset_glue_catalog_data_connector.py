import random

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import (
    InferredAssetAWSGlueDataCatalogDataConnector,
)
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()


def test_basic_instantiation(glue_titanic_catalog):
    random.seed(0)

    config = yaml.load(
        """
    name: my_data_connector
    datasource_name: FAKE_DATASOURCE_NAME
    execution_engine: execution_engine
    """,
    )
    my_data_connector = InferredAssetAWSGlueDataCatalogDataConnector(**config)

    # noinspection PyProtectedMember
    asset_names = my_data_connector.get_available_data_asset_names()

    assert len(asset_names) == 2
    assert (
        "db_test.tb_titanic_with_partitions" in asset_names
        and "db_test.tb_titanic_without_partitions" in asset_names
    )
    assert my_data_connector.get_unmatched_data_references() == []

    report = my_data_connector.self_check()
    assert report == {
        "class_name": "InferredAssetAWSGlueDataCatalogDataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "db_test.tb_titanic_with_partitions",
            "db_test.tb_titanic_without_partitions",
        ],
        "data_assets": {
            "db_test.tb_titanic_with_partitions": {
                "batch_definition_count": 6,
                "example_data_references": [
                    {"PClass": "1st", "SexCode": "0"},
                    {"PClass": "1st", "SexCode": "1"},
                    {"PClass": "2nd", "SexCode": "0"},
                ],
            },
            "db_test.tb_titanic_without_partitions": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }
    assert "db_test.tb_titanic_with_partitions" in my_data_connector.assets
    assert my_data_connector.assets["db_test.tb_titanic_with_partitions"] == {
        "database_name": "db_test",
        "table_name": "tb_titanic_with_partitions",
        "partitions": ["PClass", "SexCode"],
        "data_asset_name_prefix": "",
        "data_asset_name_suffix": "",
    }
    assert "db_test.tb_titanic_without_partitions" in my_data_connector.assets
    assert my_data_connector.assets["db_test.tb_titanic_without_partitions"] == {
        "database_name": "db_test",
        "table_name": "tb_titanic_without_partitions",
        "partitions": [],
        "data_asset_name_prefix": "",
        "data_asset_name_suffix": "",
    }


def test_instantiation_from_a_config(
    glue_titanic_catalog, empty_data_context_stats_enabled
):
    random.seed(0)
    report_object = empty_data_context_stats_enabled.test_yaml_config(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetAWSGlueDataCatalogDataConnector
        name: my_data_connector
        datasource_name: FAKE_Datasource_NAME
        data_asset_name_prefix: prefix__
        data_asset_name_suffix: __suffix
        excluded_tables:
            - db_test.tb_titanic_without_partitions
        glue_introspection_directives:
            database_name: db_test
        """,  # noqa: F541
        runtime_environment={
            "execution_engine": "execution_engine",
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetAWSGlueDataCatalogDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "prefix__db_test.tb_titanic_with_partitions__suffix",
        ],
        "data_assets": {
            "prefix__db_test.tb_titanic_with_partitions__suffix": {
                "batch_definition_count": 6,
                "example_data_references": [
                    {"PClass": "1st", "SexCode": "0"},
                    {"PClass": "1st", "SexCode": "1"},
                    {"PClass": "2nd", "SexCode": "0"},
                ],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_instantiation_with_included_tables(glue_titanic_catalog):
    my_data_connector = InferredAssetAWSGlueDataCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine="execution_engine",
        catalog_id="catalog_A",
        included_tables=["db_test.tb_titanic_with_partitions"],
    )

    assert len(my_data_connector.assets) == 1
    assert "db_test.tb_titanic_with_partitions" in my_data_connector.assets


def test_instantiation_with_invalid_database_name(glue_titanic_catalog):
    with pytest.raises(gx_exceptions.DataConnectorError):
        InferredAssetAWSGlueDataCatalogDataConnector(
            name="my_data_connector",
            datasource_name="FAKE_DATASOURCE_NAME",
            execution_engine="execution_engine",
            catalog_id="catalog_A",
            glue_introspection_directives={"database_name": "FAKE_DATABASE"},
        )


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_without_partitions(
    splitter_method_name_prefix,
    glue_titanic_catalog,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    my_data_connector = InferredAssetAWSGlueDataCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
        catalog_id="catalog_A",
        data_asset_name_prefix="prefix__",
        data_asset_name_suffix="__suffix",
    )
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="prefix__db_test.tb_titanic_without_partitions__suffix",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "splitter_method": f"{splitter_method_name_prefix}split_on_hashed_column",
                "splitter_kwargs": {
                    "column_name": "Name",
                    "hash_digits": 1,
                    "hash_function_name": "md5",
                    "batch_identifiers": {"hash_value": "f"},
                },
            },
        )
    )

    batch = Batch(data=batch_data)

    validator = Validator(execution_engine, batches=[batch])

    assert len(validator.head(fetch_all=True)) == 77


def test_get_batch_data_and_metadata_with_partitions(
    in_memory_runtime_context,
    glue_titanic_catalog,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
):
    execution_engine = (
        test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    )
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"] = Datasource(
        name="FAKE_Datasource_NAME",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine=execution_engine.config,
        data_connectors={
            "my_data_connector": {
                "class_name": "InferredAssetAWSGlueDataCatalogDataConnector",
                "catalog_id": "catalog_A",
                "data_asset_name_prefix": "prefix__",
                "data_asset_name_suffix": "__suffix",
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    in_memory_runtime_context.datasources["FAKE_Datasource_NAME"]._execution_engine = execution_engine  # type: ignore[union-attr]

    my_data_connector = in_memory_runtime_context.datasources[
        "FAKE_Datasource_NAME"
    ].data_connectors["my_data_connector"]

    batch_definition = BatchDefinition(
        datasource_name="FAKE_Datasource_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="prefix__db_test.tb_titanic_with_partitions__suffix",
        batch_identifiers=IDDict({"PClass": "1st", "SexCode": "0"}),
    )
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=batch_definition
    )

    batch = Batch(data=batch_data, batch_definition=batch_definition)

    validator = Validator(
        execution_engine, batches=[batch], data_context=in_memory_runtime_context
    )

    assert validator.expect_column_values_to_be_in_set("PClass", ["1st"]).success
    assert validator.expect_column_values_to_be_in_set("SexCode", ["0"]).success
