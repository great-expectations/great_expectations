import random

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import Batch, LegacyBatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.data_connector import (
    InferredAssetAWSGlueDataCatalogDataConnector,
)
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()

# module level markers
pytestmark = pytest.mark.big


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


@pytest.mark.parametrize("partitioner_method_name_prefix", ["_", ""])
def test_get_batch_data_and_metadata_without_partitions(
    partitioner_method_name_prefix,
    glue_titanic_catalog,
    test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
):
    execution_engine = test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine
    my_data_connector = InferredAssetAWSGlueDataCatalogDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_Datasource_NAME",
        execution_engine=test_cases_for_aws_glue_data_catalog_data_connector_spark_execution_engine,
        catalog_id="catalog_A",
        data_asset_name_prefix="prefix__",
        data_asset_name_suffix="__suffix",
    )
    batch_data, _, __ = my_data_connector.get_batch_data_and_metadata(
        batch_definition=LegacyBatchDefinition(
            datasource_name="FAKE_Datasource_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="prefix__db_test.tb_titanic_without_partitions__suffix",
            batch_identifiers=IDDict(),
            batch_spec_passthrough={
                "partitioner_method": f"{partitioner_method_name_prefix}partition_on_hashed_column",
                "partitioner_kwargs": {
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
