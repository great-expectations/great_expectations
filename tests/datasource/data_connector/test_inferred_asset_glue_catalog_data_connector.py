import random

import boto3
import pytest
from moto import mock_glue
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import (
    InferredAssetGlueCatalogDataConnector,
)
from great_expectations.validator.validator import Validator

yaml = YAML(typ="safe")


@pytest.fixture
def glue():
    with mock_glue():
        region_name: str = "us-east-1"
        client = boto3.client("glue", region_name=region_name)

        ## Database A
        client.create_database(
            CatalogId="catalog_A", DatabaseInput={"Name": "database_A"}
        )
        for i in range(110):
            client.create_table(
                CatalogId="catalog_A",
                DatabaseName="database_A",
                TableInput={"Name": f"table_{i}"},
            )
        client.create_database(
            CatalogId="catalog_A", DatabaseInput={"Name": "database_B"}
        )
        for i in range(110):
            client.create_table(
                CatalogId="catalog_A",
                DatabaseName="database_B",
                TableInput={"Name": f"table_{i}"},
            )
        yield client


@pytest.fixture
def glue_titanic_catalog():
    with mock_glue():
        region_name: str = "us-east-1"
        client = boto3.client("glue", region_name=region_name)

        ## Database A
        client.create_database(CatalogId="catalog_A", DatabaseInput={"Name": "db_test"})
        client.create_table(
            CatalogId="catalog_A",
            DatabaseName="db_test",
            TableInput={"Name": "tb_titanic"},
        )
        yield client


def test_basic_instantiation(glue):
    random.seed(0)

    config = yaml.load(
        """
    name: my_data_connector
    datasource_name: FAKE_DATASOURCE_NAME
    execution_engine: execution_engine
    """,
    )
    my_data_connector = InferredAssetGlueCatalogDataConnector(**config)

    # noinspection PyProtectedMember
    asset_names_and_types = my_data_connector.get_available_data_asset_names_and_types()
    asset_names, asset_types = zip(*asset_names_and_types)

    assert len(asset_names) == 220
    assert "table_0" in asset_names and "table_99" in asset_names
    assert all(t == "table" for t in asset_types)
    assert my_data_connector.get_unmatched_data_references() == []

    report = my_data_connector.self_check()
    assert report == {
        "class_name": "InferredAssetGlueCatalogDataConnector",
        "data_asset_count": 220,
        "example_data_asset_names": [
            "database_A.table_0",
            "database_A.table_1",
            "database_A.table_10",
        ],
        "data_assets": {
            "database_A.table_0": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "database_A.table_1": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "database_A.table_10": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_instantiation_from_a_config(glue, empty_data_context_stats_enabled):
    random.seed(0)
    report_object = empty_data_context_stats_enabled.test_yaml_config(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetGlueCatalogDataConnector
        name: my_data_connector
        datasource_name: FAKE_Datasource_NAME
        data_asset_name_prefix: prefix__
        data_asset_name_suffix: __suffix
        excluded_tables:
            - database_A.table_99
        glue_introspection_directives:
            database_name: database_A
        """,
        runtime_environment={
            "execution_engine": "execution_engine",
        },
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetGlueCatalogDataConnector",
        "data_asset_count": 109,
        "example_data_asset_names": [
            "prefix__database_A.table_0__suffix",
            "prefix__database_A.table_100__suffix",
            "prefix__database_A.table_101__suffix",
        ],
        "data_assets": {
            "prefix__database_A.table_0__suffix": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prefix__database_A.table_100__suffix": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prefix__database_A.table_101__suffix": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
    }


def test_instantiation_with_prefix_and_suffix(glue):
    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetGlueCatalogDataConnector
        datasource_name: FAKE_DATASOURCE_NAME
        name: my_data_connector
        data_asset_name_prefix: prefix__
        data_asset_name_suffix: __suffix
        """,
    )
    my_data_connector: InferredAssetGlueCatalogDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "execution_engine": "execution_engine",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    assert len(my_data_connector.assets) == 220
    assert "prefix__database_A.table_0__suffix" in my_data_connector.assets
    db_A_tb_0 = my_data_connector.assets["prefix__database_A.table_0__suffix"]
    assert (
        db_A_tb_0["database_name"] == "database_A"
        and db_A_tb_0["table_name"] == "table_0"
    )


def test_instantiation_with_excluded_tables(glue):
    my_data_connector = InferredAssetGlueCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine="execution_engine",
        catalog_id="catalog_A",
        excluded_tables=["database_A.table_99", "database_B.table_0"],
    )

    assert len(my_data_connector.assets) == 218
    assert "database_B.table_0" not in my_data_connector.assets
    assert "database_A.table_99" not in my_data_connector.assets


def test_instantiation_with_included_tables(glue):
    my_data_connector = InferredAssetGlueCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine="execution_engine",
        catalog_id="catalog_A",
        included_tables=["database_A.table_0"],
    )

    assert len(my_data_connector.assets) == 1
    assert "database_A.table_0" in my_data_connector.assets


def test_instantiation_with_introspection_directives(glue):
    my_data_connector = InferredAssetGlueCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine="execution_engine",
        catalog_id="catalog_A",
        glue_introspection_directives={"database_name": "database_B"},
    )

    assert len(my_data_connector.assets) == 110
    assert "database_B.table_0" in my_data_connector.assets
    assert "database_A.table_0" not in my_data_connector.assets


def test_instantiation_with_invalid_database_name(glue):
    with pytest.raises(ge_exceptions.DataConnectorError):
        InferredAssetGlueCatalogDataConnector(
            name="my_data_connector",
            datasource_name="FAKE_DATASOURCE_NAME",
            execution_engine="execution_engine",
            catalog_id="catalog_A",
            glue_introspection_directives={"database_name": "FAKE_DATABASE"},
        )


@pytest.mark.parametrize("splitter_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata(
    splitter_method_name_prefix,
    glue_titanic_catalog,
    test_cases_for_glue_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_glue_catalog_data_connector_spark_execution_engine
    my_data_connector = InferredAssetGlueCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=test_cases_for_glue_catalog_data_connector_spark_execution_engine,
        catalog_id="catalog_A",
        data_asset_name_prefix="prefix__",
        data_asset_name_suffix="__suffix",
    )
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=BatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="prefix__db_test.tb_titanic__suffix",
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
