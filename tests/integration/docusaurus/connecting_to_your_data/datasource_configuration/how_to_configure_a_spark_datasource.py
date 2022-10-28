"""Example Script: How to configure a Spark Datasource

This example script is intended for use in documentation on how to configure a Spark Datasource.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource
"""

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.

from datasource_configuration_test_utilities import (
    get_full_universal_datasource_config_elements,
    is_subset,
)

import great_expectations as gx

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.


def get_full_spark_inferred_datasource_single_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that only returns single item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    # <snippet name="full datasource_config for spark inferred singlebatch Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkExecutionEngine",
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
    # </snippet>
    return datasource_config


def get_full_spark_inferred_datasource_multi_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that can returns multiple item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    # <snippet name="full datasource_config for spark inferred multibatch Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkExecutionEngine",
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
    # </snippet>
    return datasource_config


def get_full_spark_configured_datasource_single_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using an
     inferred data connector that only returns single item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    # <snippet name="full datasource_config for spark configured singlebatch Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkExecutionEngine",
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
    # </snippet>
    return datasource_config


def get_full_spark_configured_datasource_multi_batch() -> dict:
    """Creates a dictionary configuration for a spark Datasource using a
     configured data connector that can return multiple item batches.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    # <snippet name="full datasource_config for spark configured multibatch Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkExecutionEngine",
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
    # </snippet>
    return datasource_config


def get_full_spark_runtime_datasource() -> dict:
    """Creates a dictionary configuration for a spark Datasource using a
     runtime data connector.

    Returns:
         a dictionary containing a full configuration for a Spark Datasource
    """
    # <snippet name="full datasource_config for spark runtime Datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkExecutionEngine",
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
    # </snippet>
    return datasource_config


def validate_universal_config_elements():
    """Validates that the 'universal' configuration keys and values are in fact identical to the keys and values
    in all the full Spark configurations.
    """
    universal_elements = get_full_universal_datasource_config_elements()
    is_subset(universal_elements, get_full_spark_configured_datasource_single_batch())
    is_subset(universal_elements, get_full_spark_configured_datasource_multi_batch())
    is_subset(universal_elements, get_full_spark_inferred_datasource_single_batch())
    is_subset(universal_elements, get_full_spark_inferred_datasource_multi_batch())
    is_subset(universal_elements, get_full_spark_runtime_datasource())
