"""Example Script: How to configure a Spark Datasource

This example script is intended for use in documentation on how to configure a Spark Datasource.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource

To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_configure_a_spark_datasource" tests/integration/test_script_runner.py
```

To validate the snippets in this file, use the following console command:
```
yarn snippet-check ./tests/integration/docusaurus/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource.py
```
"""

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.
from ruamel import yaml

import great_expectations as gx
from tests.integration.docusaurus.connecting_to_your_data.datasource_configuration.datasource_configuration_test_utilities import (
    is_subset,
)
from tests.integration.docusaurus.connecting_to_your_data.datasource_configuration.full_datasource_configurations import (
    get_full_config_spark_configured_datasource_multi_batch,
    get_full_config_spark_configured_datasource_single_batch,
    get_full_config_spark_inferred_datasource_multi_batch,
    get_full_config_spark_inferred_datasource_single_batch,
    get_full_config_spark_runtime_datasource,
    get_partial_config_universal_datasource_config_elements,
)

data_context: gx.DataContext = gx.get_context()


def validate_universal_config_elements():
    """Validates that the 'universal' configuration keys and values are in fact identical to the keys and values
    in all the full Spark configurations.
    """
    universal_elements = get_partial_config_universal_datasource_config_elements()
    is_subset(
        universal_elements, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        universal_elements, get_full_config_spark_configured_datasource_multi_batch()
    )
    is_subset(
        universal_elements, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        universal_elements, get_full_config_spark_inferred_datasource_multi_batch()
    )
    is_subset(universal_elements, get_full_config_spark_runtime_datasource())


# The following methods correspond to the section headings in the how-to guide linked in the module docstring.


def section_5_add_the_spark_execution_engine_to_your_datasource_configuration():
    """Provides and tests the snippets for section 5 of the Spark Datasource configuration guide."""

    # <snippet name="spark datasource_config up to definition of Spark execution engine">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
    }
    # </snippet>

    execution_engine_snippet: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        # <snippet name="define spark execution engine">
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        }
        # </snippet>
    }

    assert datasource_config == execution_engine_snippet
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )
    is_subset(datasource_config, get_full_config_spark_runtime_datasource())


def section_6_add_a_dictionary_as_the_value_of_the_data_connectors_key():
    """Provides and tests the snippets for section 6 of the Spark Datasource configuration guide."""

    # <snippet name="spark datasource_config up to adding an empty data_connectors dictionary">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {},
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )
    is_subset(datasource_config, get_full_config_spark_runtime_datasource())


def section_7_configure_your_individual_data_connectors__inferred():
    """Provides and tests the InferredAssetFilesystemDataConnector snippets for section 7 of the
    Spark Datasource configuration guide.

    """
    # <snippet name="spark Datasource configuration up to adding an empty inferred data connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_inferred_data_connector": {}},
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )

    # <snippet name="inferred spark datasource_config up to adding empty default_regex and batch_spec_passthrough dictionaries">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="inferred data connector define class_name as InferredAssetFilesystemDataConnector">
                "class_name": "InferredAssetFilesystemDataConnector",
                # </snippet>
                "base_directory": "../data",
                "default_regex": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    # </snippet>

    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )

    glob_directive: dict = {
        "name": "my_datasource_name",
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
                # <snippet name="define glob_directive">
                "glob_directive": "*/*",
                # </snippet>
                "default_regex": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    is_subset(datasource_config, glob_directive)

    # <snippet name="spark inferred datasource_config post glob_directive definition">
    datasource_config: dict = {
        "name": "my_datasource_name",
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
                "glob_directive": "*/*",
                "default_regex": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    # </snippet>
    assert datasource_config == glob_directive


def section_7_configure_your_individual_data_connectors__configured():
    """Provides and tests the ConfiguredAssetFilesystemDataConnector snippets for section 7 of the
    Spark Datasource configuration guide.

    """
    # <snippet name="spark Datasource configuration up to adding an empty configured data connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_configured_data_connector": {}},
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )

    # <snippet name="configured spark datasource_config up to adding empty assets and batch_spec_passthrough dictionaries">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                # <snippet name="configured data connector define class_name as ConfiguredAssetFilesystemDataConnector">
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                # </snippet>
                "base_directory": "../data",
                "assets": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    # </snippet>

    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )


def section_7_configure_your_individual_data_connectors__runtime():
    """Provides and tests the RuntimeDataConnector snippets for section 7 of the
    Spark Datasource configuration guide.

    """
    # <snippet name="spark Datasource configuration up to adding an empty runtime data connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_runtime_data_connector": {}},
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_spark_runtime_datasource())

    # <snippet name="runtime spark datasource_config up to adding empty batch_identifiers and batch_spec_passthrough dictionaries">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                # <snippet name="runtime data connector define class_name as RuntimeDataConnector">
                "class_name": "RuntimeDataConnector",
                # </snippet>
                "batch_spec_passthrough": {},
                "batch_identifiers": [],
            }
        },
    }
    # </snippet>

    is_subset(datasource_config, get_full_config_spark_runtime_datasource())


def section_8_configure_the_values_for_batch_spec_passthrough__universal():
    """Provides and tests the RuntimeDataConnector snippets for section 7 of the
    Spark Datasource configuration guide.

    """
    empty_batch_spec_passthrough: dict = {
        # <snippet name="populate batch_spec_passthrough with empty reader_method and reader_options">
        "batch_spec_passthrough": {"reader_method": "", "reader_options": {}}
        # </snippet>
    }
    smaller_snippets: dict = {
        "batch_spec_passthrough": {
            # <snippet name="populate reader_method with csv">
            "reader_method": "csv",
            # </snippet>
            # <snippet name = "populate reader_options with empty header and inferSchema keys">
            "reader_options": {"header": "", "inferSchema": ""}
            # </snippet>
        }
    }
    even_smaller_snippets: dict = {
        "batch_spec_passthrough": {
            "reader_method": "csv",
            "reader_options": {
                # <snippet name="define header as True for reader_options">
                "header": True,
                # </snippet>
                # <snippet name="define inferSchema as True for reader_options">
                "inferSchema": True
                # </snippet>
            },
        }
    }
    is_subset(empty_batch_spec_passthrough, even_smaller_snippets)
    is_subset(smaller_snippets, even_smaller_snippets)
    is_subset(
        even_smaller_snippets,
        get_full_config_spark_configured_datasource_single_batch()["data_connectors"][
            "name_of_my_configured_data_connector"
        ],
    )
    is_subset(
        even_smaller_snippets,
        get_full_config_spark_configured_datasource_multi_batch()["data_connectors"][
            "name_of_my_configured_data_connector"
        ],
    )
    is_subset(
        even_smaller_snippets,
        get_full_config_spark_inferred_datasource_single_batch()["data_connectors"][
            "name_of_my_inferred_data_connector"
        ],
    )
    is_subset(
        even_smaller_snippets,
        get_full_config_spark_inferred_datasource_multi_batch()["data_connectors"][
            "name_of_my_inferred_data_connector"
        ],
    )
    is_subset(
        even_smaller_snippets,
        get_full_config_spark_runtime_datasource()["data_connectors"][
            "name_of_my_runtime_data_connector"
        ],
    )
    assert (
        even_smaller_snippets["batch_spec_passthrough"]
        == get_full_config_spark_configured_datasource_single_batch()[
            "data_connectors"
        ]["name_of_my_configured_data_connector"]["batch_spec_passthrough"]
    )
    assert (
        even_smaller_snippets["batch_spec_passthrough"]
        == get_full_config_spark_configured_datasource_multi_batch()["data_connectors"][
            "name_of_my_configured_data_connector"
        ]["batch_spec_passthrough"]
    )
    assert (
        even_smaller_snippets["batch_spec_passthrough"]
        == get_full_config_spark_inferred_datasource_single_batch()["data_connectors"][
            "name_of_my_inferred_data_connector"
        ]["batch_spec_passthrough"]
    )
    assert (
        even_smaller_snippets["batch_spec_passthrough"]
        == get_full_config_spark_inferred_datasource_multi_batch()["data_connectors"][
            "name_of_my_inferred_data_connector"
        ]["batch_spec_passthrough"]
    )
    assert (
        even_smaller_snippets["batch_spec_passthrough"]
        == get_full_config_spark_runtime_datasource()["data_connectors"][
            "name_of_my_runtime_data_connector"
        ]["batch_spec_passthrough"]
    )


def section_8_configure_the_values_for_batch_spec_passthrough__inferred():
    # <snippet name="inferred spark datasource_config post batch_spec_passthrough definition">
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
                "default_regex": {},
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
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )


def section_8_configure_the_values_for_batch_spec_passthrough__configured():
    # <snippet name="configured spark datasource_config post batch_spec_passthrough definition">
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
                "assets": {},
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
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )


def section_8_configure_the_values_for_batch_spec_passthrough__runtime():
    # <snippet name="runtime spark datasource_config post batch_spec_passthrough definition">
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
                "batch_identifiers": [],
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_spark_runtime_datasource())


def section_9_configure_your_data_connectors_data_assets__inferred__single_batch():
    single_line_snippets: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "default_regex": {
                    # <snippet name="inferred spark single batch pattern">
                    "pattern": "(.*)\\.csv",
                    # </snippet>
                    # <snippet name="inferred spark single batch group_names">
                    "group_names": ["data_asset_name"]
                    # </snippet>
                },
            },
        },
    }
    is_subset(
        single_line_snippets, get_full_config_spark_inferred_datasource_single_batch()
    )

    data_connector_snippet: dict = {
        "data_connectors": {
            # <snippet name="inferred spark single batch data connector config">
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {
                    "pattern": "(.*)\\.csv",
                    "group_names": ["data_asset_name"],
                },
            },
            # </snippet>
        },
        # </snippet>
    }
    is_subset(
        data_connector_snippet, get_full_config_spark_inferred_datasource_single_batch()
    )

    # <snippet name="full datasource_config for inferred spark single batch datasource">
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
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_single_batch()
    )


def section_9_configure_your_data_connectors_data_assets__inferred__multi_batch():
    # <snippet name="full datasource_config for inferred spark multi batch datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            # <snippet name="inferred spark multi batch data connector config">
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
            # </snippet>
        },
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_inferred_datasource_multi_batch()
    )


def section_9_configure_your_data_connectors_data_assets__configured__single_batch():
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="empty asset for configured spark single batch datasource">
                    "yellow_tripdata_jan": {}
                    # </snippet>
                }
            }
        }
    }
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )

    # <snippet name="full datasource_config for configured spark single batch datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            # <snippet name="configured spark single batch data connector config">
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    # <snippet name="configured spark single batch asset">
                    "yellow_tripdata_jan": {
                        # <snippet name="configured spark single batch pattern">
                        "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                        # </snippet>
                        # <snippet name="configured spark single batch group_names">
                        "group_names": ["month"],
                        # </snippet>
                    },
                    # </snippet>
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
            # </snippet>
        },
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_single_batch()
    )


def section_9_configure_your_data_connectors_data_assets__configured__multi_batch():
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="empty asset for configured spark multi batch datasource">
                    "yellow_tripdata_2020": {}
                    # </snippet>
                }
            }
        }
    }
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )

    # <snippet name="full datasource_config for configured spark multi batch datasource">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            # <snippet name="configured spark multi batch data connector config">
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    # <snippet name="configured spark multi batch asset">
                    "yellow_tripdata_2020": {
                        # <snippet name="configured spark multi batch pattern">
                        "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                        # </snippet>
                        # <snippet name="configured spark multi batch group_names">
                        "group_names": ["month"],
                        # </snippet>
                    },
                    # </snippet>
                },
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
            }
            # </snippet>
        },
    }
    # </snippet>
    is_subset(
        datasource_config, get_full_config_spark_configured_datasource_multi_batch()
    )


def section_9_configure_your_data_connectors_data_assets__runtime():
    # <snippet name="full datasource_config for runtime spark datasource">
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
                # <snippet name="runtime spark batch_identifiers">
                "batch_identifiers": ["batch_timestamp"],
                # </snippet>
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, get_full_config_spark_runtime_datasource())


def section_10_test_your_configuration_with_test_yaml_config():
    for datasource_config, connector_name, asset_count in (
        (
            get_full_config_spark_runtime_datasource(),
            "name_of_my_runtime_data_connector",
            0,
        ),
        (
            get_full_config_spark_inferred_datasource_single_batch(),
            "name_of_my_inferred_data_connector",
            12,
        ),
        (
            get_full_config_spark_inferred_datasource_multi_batch(),
            "name_of_my_inferred_data_connector",
            1,
        ),
        (
            get_full_config_spark_configured_datasource_single_batch(),
            "name_of_my_configured_data_connector",
            1,
        ),
        (
            get_full_config_spark_configured_datasource_multi_batch(),
            "name_of_my_configured_data_connector",
            1,
        ),
    ):

        test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
        datasource_check = test_result.self_check(max_examples=12)

        # NOTE: The following code is only for testing and can be ignored by users.
        # Assert that there are no data sets -- those get defined in a Batch Request.
        assert (
            datasource_check["data_connectors"][connector_name]["data_asset_count"]
            == asset_count
        ), f"{connector_name} {asset_count} != {datasource_check['data_connectors'][connector_name]['data_asset_count']}"


def section_12_add_your_new_datasource_to_your_datacontext():
    for datasource_config, datasource_name in (
        (
            get_full_config_spark_runtime_datasource(),
            "name_of_my_runtime_data_connector",
        ),
        (
            get_full_config_spark_inferred_datasource_single_batch(),
            "name_of_my_inferred_data_connector",
        ),
        (
            get_full_config_spark_inferred_datasource_multi_batch(),
            "name_of_my_inferred_data_connector",
        ),
        (
            get_full_config_spark_configured_datasource_single_batch(),
            "name_of_my_configured_data_connector",
        ),
        (
            get_full_config_spark_configured_datasource_multi_batch(),
            "name_of_my_configured_data_connector",
        ),
    ):
        # <snippet name="test your spark datasource config with test_yaml_config">
        data_context.test_yaml_config(yaml.dump(datasource_config))
        # </snippet>

        # <snippet name="add your spark datasource to your data_context">
        data_context.add_datasource(**datasource_config)
        # </snippet>

        # <snippet name="add your spark datasource to your data_context only if it does not already exit">
        # add_datasource only if it doesn't already exist in your Data Context
        try:
            data_context.get_datasource(datasource_config["name"])
        except ValueError:
            data_context.add_datasource(**datasource_config)
        else:
            print(
                f"The datasource {datasource_config['name']} already exists in your Data Context!"
            )
        # </snippet>

        assert (
            datasource_name
            in data_context.list_datasources()[0]["data_connectors"].keys()
        ), f"{datasource_name} not in {data_context.list_datasources()}"
        data_context.delete_datasource(name="my_datasource_name")


validate_universal_config_elements()

section_5_add_the_spark_execution_engine_to_your_datasource_configuration()

section_6_add_a_dictionary_as_the_value_of_the_data_connectors_key()

section_7_configure_your_individual_data_connectors__inferred()
section_7_configure_your_individual_data_connectors__configured()
section_7_configure_your_individual_data_connectors__runtime()

section_8_configure_the_values_for_batch_spec_passthrough__universal()
section_8_configure_the_values_for_batch_spec_passthrough__inferred()
section_8_configure_the_values_for_batch_spec_passthrough__configured()
section_8_configure_the_values_for_batch_spec_passthrough__runtime()

section_9_configure_your_data_connectors_data_assets__inferred__single_batch()
section_9_configure_your_data_connectors_data_assets__inferred__multi_batch()
section_9_configure_your_data_connectors_data_assets__configured__single_batch()
section_9_configure_your_data_connectors_data_assets__configured__multi_batch()
section_9_configure_your_data_connectors_data_assets__runtime()

section_10_test_your_configuration_with_test_yaml_config()

section_12_add_your_new_datasource_to_your_datacontext()
