---
title: How to instantiate a Data Context without a yml file
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '/docs/term_tags/_tag.mdx';

This guide will help you instantiate a <TechnicalTag tag="data_context" text="Data Context" /> without a yml file, aka configure a Data Context in code. If you are working in an environment without easy access to a local filesystem (e.g. AWS Spark EMR, Databricks, etc.) you may wish to configure your Data Context in code, within your notebook or workflow tool (e.g. Airflow DAG node).

<Prerequisites>

</Prerequisites>

:::note
- See also our companion video for this guide: [Data Contexts In Code](https://youtu.be/4VMOYpjHNhM).
:::

## Steps

### 1. **Create a DataContextConfig**

The `DataContextConfig` holds all of the associated configuration parameters to build a Data Context. There are defaults set for you to minimize configuration in typical cases, but please note that every parameter is configurable and all defaults are overridable. Also note that `DatasourceConfig` also has defaults which can be overridden.

Here we will show a few examples of common configurations, using the ``store_backend_defaults`` parameter. Note that you can use the existing API without defaults by omitting that parameter, and you can override all of the parameters as shown in the last example. A parameter set in ``DataContextConfig`` will override a parameter set in ``store_backend_defaults`` if both are used.

The following ``store_backend_defaults`` are currently available:
- `S3StoreBackendDefaults`
- `GCSStoreBackendDefaults`
- `DatabaseStoreBackendDefaults`
- `FilesystemStoreBackendDefaults`

The following example shows a Data Context configuration with an SQLAlchemy <TechnicalTag relative="../../../" tag="datasource" text="Datasource" /> and an AWS S3 bucket for all metadata <TechnicalTag relative="../../../" tag="store" text="Stores" />, using default prefixes. Note that you can still substitute environment variables as in the YAML based configuration to keep sensitive credentials out of your code.

```python
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, S3StoreBackendDefaults

data_context_config = DataContextConfig(
    datasources={
        "sql_warehouse": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SqlAlchemyExecutionEngine",
                "credentials": {
                    "drivername": "postgresql+psycopg2",
                    "host": "localhost",
                    "port": "5432",
                    "username": "postgres",
                    "password": "postgres",
                    "database": "postgres",
                },
            },
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": "whole_table",
                },
            }
        )
    },
    store_backend_defaults=S3StoreBackendDefaults(default_bucket_name="my_default_bucket"),
    )
```

The following example shows a Data Context configuration with a Pandas datasource and local filesystem defaults for metadata stores. Note: imports are omitted in the following examples. Note: You may add an optional root_directory parameter to set the base location for the Store Backends.

```python
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults

data_context_config = DataContextConfig(
    datasources={
        "pandas": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "PandasExecutionEngine"
            },
            data_connectors={
                "tripdata_monthly_configured": {
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "base_directory": "/path/to/trip_data",
                    "assets": {
                        "yellow": {
                            "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                            "group_names": ["year", "month"],
                        }
                    },
                }
            },
        )
    },
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/path/to/store/location"),
)
```

The following example shows a Data Context configuration with an SQLAlchemy datasource and two GCS buckets for metadata Stores, using some custom and some default prefixes. Note that you can still substitute environment variables as in the YAML based configuration to keep sensitive credentials out of your code. `default_bucket_name`, `default_project_name` sets the default value for all stores that are not specified individually.

The resulting `DataContextConfig` from the following example creates an <TechnicalTag tag="expectation_store" text="Expectations Store" /> and <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> using the `my_default_bucket` and `my_default_project` parameters since their bucket and project is not specified explicitly. The <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is created using the explicitly specified `my_validations_bucket` and `my_validations_project`. Further, the prefixes are set for the Expectations Store and Validation Results Store, while Data Docs use the default `data_docs` prefix.

```python
data_context_config = DataContextConfig(
    datasources={
        "sql_warehouse": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SqlAlchemyExecutionEngine",
                "credentials": {
                    "drivername": "postgresql+psycopg2",
                    "host": "localhost",
                    "port": "5432",
                    "username": "postgres",
                    "password": "postgres",
                    "database": "postgres",
                },
            },
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": "whole_table",
                },
            }
        )
    },
    store_backend_defaults=GCSStoreBackendDefaults(
        default_bucket_name="my_default_bucket",
        default_project_name="my_default_project",
        validations_store_bucket_name="my_validations_bucket",
        validations_store_project_name="my_validations_project",
        validations_store_prefix="my_validations_store_prefix",
        expectations_store_prefix="my_expectations_store_prefix",
    ),
)
```

The following example sets overrides for many of the parameters available to you when creating a `DataContextConfig` and a Datasource.

```python
data_context_config = DataContextConfig(
    config_version=2,
    plugins_directory=None,
    config_variables_file_path=None,
    datasources={
        "my_spark_datasource": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SparkDFExecutionEngine"
            },
            data_connectors={
                "tripdata_monthly_configured": {
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "base_directory": "/path/to/trip_data",
                    "assets": {
                        "yellow": {
                            "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                            "group_names": ["year", "month"],
                        }
                    },
                }
            },
        )
    },
    stores={
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "my_expectations_store_bucket",
                "prefix": "my_expectations_store_prefix",
            },
        },
        "validations_S3_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "my_validations_store_bucket",
                "prefix": "my_validations_store_prefix",
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_S3_store",
    validations_store_name="validations_S3_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    data_docs_sites={
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket":  "my_data_docs_bucket",
                "prefix":  "my_optional_data_docs_prefix",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
        }
    },
    validation_operators={
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    },
    anonymous_usage_statistics={
      "enabled": True
    }
)
```


### 2. Pass this DataContextConfig as a project_config to BaseDataContext

```python
from great_expectations.data_context.types.base import BaseDataContext
context = BaseDataContext(project_config=data_context_config)
```

### 3. Use this BaseDataContext instance as your DataContext

If you are using Airflow, you may wish to pass this Data Context to your GreatExpectationsOperator as a parameter. See the following guide for more details:

- [Deploying Great Expectations with Airflow](../../../../docs/intro.md)


Additional resources
--------------------

- [How to instantiate a Data Context on an EMR Spark cluster](../../../deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster.md)
- [How to use Great Expectations in Databricks](/docs/deployment_patterns/how_to_use_great_expectations_in_databricks)
