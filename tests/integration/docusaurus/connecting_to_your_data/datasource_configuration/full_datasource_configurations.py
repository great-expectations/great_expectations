### Universal datasource configuration elements:

CONNECTION_STRING = "sqlite:///data/yellow_tripdata_sample_2020_all_months_combined.db"


def get_partial_config_universal_datasource_config_elements() -> dict:
    """Creates a dictionary containing the keys and values that are universally defined in
    Spark, Pandas, and SQL Datasource configurations.

    Returns:
         a dictionary containing a partial configuration for a Datasource
    """
    # <snippet name="testerific">
    datasource_config: dict = {
        # <snippet name="test">
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        # </snippet>
    }
    # </snippet>
    return datasource_config


### Spark datasource configurations:


def get_full_config_spark_inferred_datasource_single_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that only returns single item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {
                    "pattern": "(.*)\\.csv",
                    "group_names": ["data_asset_name"],
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
        },
    }
    return datasource_config


def get_full_config_spark_inferred_datasource_multi_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that can returns multiple item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {
                    "pattern": "(yellow_tripdata_sample_2020)-(\\d.*)\\.csv",
                    "group_names": ["data_asset_name", "month"],
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
        },
    }
    return datasource_config


def get_full_config_spark_configured_datasource_single_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that only returns single item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    "yellow_tripdata_jan": {
                        "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                        "group_names": ["month"],
                    }
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
        },
    }
    return datasource_config


def get_full_config_spark_configured_datasource_multi_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using a
     configured data connector that can return multiple item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    "yellow_tripdata_2020": {
                        "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                        "group_names": ["month"],
                    }
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
        },
    }
    return datasource_config


def get_full_config_spark_runtime_datasource() -> dict:
    """Creates a dictionary configuration for a spark Datasource using a
     runtime data connector.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
                "batch_identifiers": ["batch_timestamp"],
            }
        },
    }
    return datasource_config


def get_full_config_sql_inferred_datasource__single_batch_only() -> dict:
    """Creates a dictionary configuration for a SQL Datasource using a
     configured data connector that can only return a single batch.

    Returns:
         a dictionary containing a full configuration for a SQL Datasource
    """
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
                "class_name": "InferredAssetSqlDataConnector",
            },
        },
    }
    return datasource_config


def get_full_config_sql_inferred_datasource__single_and_multi_batch() -> dict:
    """Creates a dictionary configuration for a SQL Datasource using a
     configured data connector that can return multiple item batches and
     a configured data connector that can only return a single batch.

    Returns:
         a dictionary containing a full configuration for a SQL Datasource
    """
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
            "inferred_data_connector_single_batch_asset": {
                "class_name": "InferredAssetSqlDataConnector",
            },
            "inferred_data_connector_multi_batch_asset_partition_on_date_time": {
                "class_name": "InferredAssetSqlDataConnector",
                "partitioner_method": "partition_on_year_and_month",
                "partitioner_kwargs": {
                    "column_name": "pickup_datetime",
                },
            },
        },
    }
    return datasource_config


def get_full_config_sql_configured_datasource() -> dict:
    """Creates a dictionary configuration for a SQL Datasource using a
     inferred data connector that can return multiple item batches and
     a configured data connector that can only return a single batch.

    Returns:
         a dictionary containing a full configuration for a SQL Datasource
    """
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
                    "yellow_tripdata_sample_2020_full": {
                        "table_name": "yellow_tripdata_sample_2020",
                        "schema_name": "main",
                    },
                    "yellow_tripdata_sample_2020_by_year_and_month": {
                        "table_name": "yellow_tripdata_sample_2020",
                        "schema_name": "main",
                        "partitioner_method": "partition_on_year_and_month",
                        "partitioner_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                },
            },
        },
    }
    return datasource_config


def get_full_config_sql_runtime_datasource() -> dict:
    """Creates a dictionary configuration for a SQL Datasource using a
     runtime data connector.

    Returns:
         a dictionary containing a full configuration for a SQL Datasource
    """
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
                "batch_identifiers": ["batch_timestamp"],
            }
        },
    }
    return datasource_config
