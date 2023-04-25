from typing import Union

import pytest

from great_expectations import DataContext
from great_expectations.compatibility import pyarrow, pyspark
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource import BaseDatasource, LegacyDatasource

yaml = YAMLHandler()


@pytest.fixture
def data_source_config_with_aws_glue_catalog_data_connectors():
    config: str = f"""
    class_name: Datasource
    execution_engine:
        class_name: SparkDFExecutionEngine
    data_connectors:
        glue_configured_connector:
            class_name: ConfiguredAssetAWSGlueDataCatalogDataConnector
            partitions:
                - PClass
                - SexCode
            assets:
                db_test.tb_titanic:
                    table_name: tb_titanic_with_partitions
                    database_name: db_test
                    splitter_method: split_on_column_value
                    splitter_kwargs:
                        column_name: PClass
                    partitions:
                        - SexCode
                asset2:
                    table_name: tb_titanic_with_partitions
                    database_name: db_test
                    sampling_method: _sample_using_random
                    sampling_kwargs:
                        p: 0.5
                        seed: 0
                asset3:
                    table_name: tb_titanic_without_partitions
                    database_name: db_test
        glue_inferred_connector:
            class_name: InferredAssetAWSGlueDataCatalogDataConnector
    """  # noqa: F541
    return yaml.load(config)


@pytest.mark.integration
@pytest.mark.skipif(
    not pyspark.pyspark,
    reason='Could not import "pyspark"',
)
@pytest.mark.skipif(
    not pyarrow.pyarrow,
    reason='Could not import "pyarrow"',
)
def test_instantiation_from_config(
    glue_titanic_catalog, data_source_config_with_aws_glue_catalog_data_connectors
):
    my_data_source = instantiate_class_from_config(
        data_source_config_with_aws_glue_catalog_data_connectors,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_spark_datasource"},
    )

    report: dict = my_data_source.self_check()
    report["execution_engine"].pop("spark_config")
    assert report == {
        "execution_engine": {
            "caching": True,
            "module_name": "great_expectations.execution_engine.sparkdf_execution_engine",
            "class_name": "SparkDFExecutionEngine",
            "persist": True,
            "azure_options": {},
        },
        "data_connectors": {
            "count": 2,
            "glue_configured_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "data_asset_count": 3,
                "example_data_asset_names": ["asset2", "asset3", "db_test.tb_titanic"],
                "data_assets": {
                    "asset2": {
                        "batch_definition_count": 6,
                        "example_data_references": [
                            {"PClass": "1st", "SexCode": "0"},
                            {"PClass": "1st", "SexCode": "1"},
                            {"PClass": "2nd", "SexCode": "0"},
                        ],
                    },
                    "asset3": {
                        "batch_definition_count": 1,
                        "example_data_references": [{}],
                    },
                    "db_test.tb_titanic": {
                        "batch_definition_count": 2,
                        "example_data_references": [
                            {"SexCode": "0"},
                            {"SexCode": "1"},
                        ],
                    },
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
            "glue_inferred_connector": {
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
            },
        },
    }


@pytest.mark.integration
@pytest.mark.skipif(
    not pyspark.pyspark,
    reason='Could not import "pyspark"',
)
@pytest.mark.skipif(
    not pyarrow.pyarrow,
    reason='Could not import "pyarrow"',
)
def test_instantiation_from_datasource(
    glue_titanic_catalog,
    empty_data_context,
    data_source_config_with_aws_glue_catalog_data_connectors,
):
    # This is a basic integration test demonstrating a Datasource containing a SQL data_connector.
    # It tests that splitter configurations can be saved and loaded to great_expectations.yml by performing a
    # round-trip to the configuration.
    context: DataContext = empty_data_context

    context.add_datasource(
        name="my_datasource",
        **data_source_config_with_aws_glue_catalog_data_connectors,
    )
    datasource: Union[LegacyDatasource, BaseDatasource, None] = context.get_datasource(
        datasource_name="my_datasource"
    )
    report: dict = datasource.self_check()
    report["execution_engine"].pop("spark_config")
    assert report == {
        "execution_engine": {
            "caching": True,
            "module_name": "great_expectations.execution_engine.sparkdf_execution_engine",
            "class_name": "SparkDFExecutionEngine",
            "persist": True,
            "azure_options": {},
        },
        "data_connectors": {
            "count": 2,
            "glue_configured_connector": {
                "class_name": "ConfiguredAssetAWSGlueDataCatalogDataConnector",
                "data_asset_count": 3,
                "example_data_asset_names": ["asset2", "asset3", "db_test.tb_titanic"],
                "data_assets": {
                    "asset2": {
                        "batch_definition_count": 6,
                        "example_data_references": [
                            {"PClass": "1st", "SexCode": "0"},
                            {"PClass": "1st", "SexCode": "1"},
                            {"PClass": "2nd", "SexCode": "0"},
                        ],
                    },
                    "asset3": {
                        "batch_definition_count": 1,
                        "example_data_references": [{}],
                    },
                    "db_test.tb_titanic": {
                        "batch_definition_count": 2,
                        "example_data_references": [
                            {"SexCode": "0"},
                            {"SexCode": "1"},
                        ],
                    },
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
            },
            "glue_inferred_connector": {
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
            },
        },
    }
