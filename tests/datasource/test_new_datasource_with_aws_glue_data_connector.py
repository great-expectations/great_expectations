from typing import Union

import pytest

from great_expectations.compatibility import pyarrow, pyspark
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource import BaseDatasource, LegacyDatasource

pytestmark = pytest.mark.filesystem


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
                    partitioner_method: partition_on_column_value
                    partitioner_kwargs:
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
    # It tests that partitioner configurations can be saved and loaded to great_expectations.yml by performing a  # noqa: E501
    # round-trip to the configuration.
    context = empty_data_context

    context.add_datasource(
        name="my_datasource",
        **data_source_config_with_aws_glue_catalog_data_connectors,
    )
    _: Union[LegacyDatasource, BaseDatasource, None] = context.get_datasource(
        datasource_name="my_datasource"
    )
