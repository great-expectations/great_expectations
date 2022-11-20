"""Example Script: How to configure a Pandas Datasource

This example script is intended for use in documentation on how to configure a Pandas Datasource.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource
"""

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.

import operator
from functools import reduce
from typing import List, Tuple

# Import the necessary modules for the examples, and initialize a Data Context.
# <snippet name="filesystem_datasource_config_gx_imports">
from ruamel import yaml

import great_expectations as gx

data_context: gx.DataContext = gx.get_context()
# </snippet>

# The following methods are used to verify that the example configuration snippets are consistent with each other.
# Users can disregard them.


def get_by_path(root_dictionary: dict, keys: Tuple[str]) -> Tuple:
    return "/".join(keys), reduce(operator.getitem, keys, root_dictionary)


def gather_key_paths(
    target_dict: dict, current_path: List[str] = None
) -> List[Tuple[str]]:
    key_paths: List[Tuple[str]] = []
    for key, value in target_dict.items():
        if isinstance(value, dict):
            if current_path:
                next_path = current_path[:]
                next_path.append(key)
            else:
                next_path = [key]
            if value:
                key_paths.extend(gather_key_paths(value, next_path))
            else:
                # If this is an empty dictionary, then there will be no further nested keys to gather.
                key_paths.append(tuple(next_path))
        else:
            if current_path:
                next_path = current_path[:]
                next_path.append(key)
                key_paths.append(tuple(next_path))
            else:
                key_paths.append((key,))
    return key_paths


def is_subset(subset, superset):
    key_paths = gather_key_paths(subset)
    subset_items = [get_by_path(subset, key_path) for key_path in key_paths]
    # If the last value in key_paths leads to an empty placeholder, remove that entry from subset_items
    subset_items = [x for x in subset_items if x[1]]
    # if not subset_items[-1][1]:
    #     subset_items.pop(-1)
    # if not get_by_path(subset, key_paths[-1])[1]:
    #     subset_items.pop(-1)

    superset_items = [get_by_path(superset, key_path) for key_path in key_paths]
    assert all(
        item in superset_items for item in subset_items
    ), f"\n{subset_items} is not a subset of \n{superset_items}"


# The following methods return the full configurations used in the documentation.
# They are defined here to ensure that the partial examples are all consistent with the
# final configuration.


def get_full_pandas_inferred_datasource_single_batch():
    # <snippet name="pandas_inferred_datasource_single_batch_full_snippet">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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


def get_full_pandas_inferred_datasource_multi_batch():
    # <snippet name="pandas_inferred_datasource_multi_batch_full_snippet">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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


def get_full_pandas_configured_datasource_single_batch():
    # <snippet name="pandas_configured_datasource_single_batch_full_snippet">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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


def get_full_pandas_configured_datasource_multi_batch():
    # <snippet name="pandas_configured_datasource_multi_batch_full_snippet">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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


def get_full_pandas_runtime_datasource():
    # <snippet name="pandas_runtime_datasource_full_snippet">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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


def validate_pandas_datasource_configuration_snippets():
    """
    Tests that the configuration snippets for all keys that are required by Inferred, Configured, and Runtime Pandas
    Datasources are consistent with each other and with the full configurations presented in the guide.

    """
    # Snippet: create an empty dict for your configuration.
    # <snippet name="datasource_configuration_empty_dictionary">
    datasource_config: dict = {}
    # </snippet>

    # Snippet: adding a name to your datasource
    datasource_config: dict = {
        # <snippet name="datasource_configuration_name_key">
        "name": "my_datasource_name",  # Preferably name it something relevant
        # </snippet>
    }
    prev_snippet = datasource_config

    # Snippet: full config after adding a name to your Datasource.
    # <snippet name="datasource_configuration_post_name_key">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
    }
    # </snippet>
    assert datasource_config == prev_snippet

    # Snippet: Adding a class_name and module_name to your Datasource.
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        # <snippet name="datasource_configuration_class_and_module_keys">
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource"
        # </snippet>
    }
    is_subset(prev_snippet, datasource_config)
    prev_snippet = datasource_config

    # Snippet: Full configuration after adding class_name and module_name to your Datasource.
    # <snippet name="datasource_configuration_post_class_and_module_keys">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
    }
    # </snippet>
    assert datasource_config == prev_snippet

    # Snippet: Add an execution_engine to your Datasource.
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        # <snippet name="datasource_configuration_add_execution_engine">
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        # </snippet>
    }
    is_subset(prev_snippet, datasource_config)
    prev_snippet = datasource_config

    # Snippet: Full config after adding an Execution Engine to your Datasource.
    # <snippet name="datasource_configuration_post_execution_engine">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
    }
    # </snippet>
    assert datasource_config == prev_snippet
    prev_snippet = datasource_config

    # Snippet: Add an empty dictionary for your data_connectors configuration.
    # <snippet name="datasource_configuration_add_empty_data_connectors_key">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {},
    }
    # </snippet>
    is_subset(prev_snippet, datasource_config)

    # Verify that the final version of the universally relevant configuration is consistent with the
    # full configurations demonstrated in the guide.
    for full_config in [
        get_full_pandas_configured_datasource_single_batch(),
        get_full_pandas_inferred_datasource_single_batch(),
        get_full_pandas_configured_datasource_multi_batch(),
        get_full_pandas_inferred_datasource_multi_batch(),
        get_full_pandas_runtime_datasource(),
    ]:
        is_subset(datasource_config, full_config)


def validate_pandas_datasource_configuration_inferred_snippets():
    # Get the full configurations to test against
    full_inferred_single_batch_config = (
        get_full_pandas_inferred_datasource_single_batch()
    )
    full_inferred_multi_batch_config = get_full_pandas_inferred_datasource_multi_batch()

    # Snippet for adding the Data Connector configuration dictionary.
    # <snippet name="datasource_config add empty inferred_data_connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_inferred_data_connector": {}},
    }
    # </snippet>
    is_subset(datasource_config, full_inferred_single_batch_config)
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet for populating the Data Connector class_name
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="inferred data connector populate class_name">
                "class_name": "InferredAssetFilesystemDataConnector",
                # </snippet>
            }
        }
    }
    is_subset(datasource_config, full_inferred_single_batch_config)
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet for adding the Data Connector base_directory
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="inferred data connector add base_directory">
                "base_directory": "../data",
                # </snippet>
            }
        }
    }
    is_subset(datasource_config, full_inferred_single_batch_config)
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet for the final version of data_connectors class_name and module_name configuration.
    # <snippet name="datasource_configuration_pandas_inferred_empty_regex">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_inferred_single_batch_config)
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet: Add a glob_directive
    datasource_config = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="datasource_configuration_glob_directive">
                "glob_directive": "*.*"
                # </snippet>
            }
        }
    }
    prev_config = datasource_config
    # Snippet: Full config after adding glob_directive.
    # <snippet name="datasource_configuration_post_glob_directive">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "glob_directive": "*.*",
                "default_regex": {},
            }
        },
    }
    # </snippet>

    # Verify that the two glob_directive snippets are consistent with each other.
    is_subset(prev_config, datasource_config)
    # Verify that the final glob_directive snippet is consistent with the full configs, other than the "glob_directive"
    # key which is not included in the final configuration.
    del datasource_config["data_connectors"]["name_of_my_inferred_data_connector"][
        "glob_directive"
    ]
    is_subset(datasource_config, full_inferred_single_batch_config)
    is_subset(datasource_config, full_inferred_multi_batch_config)


def validate_pandas_datasource_configuration_inferred_single_batch_snippets():
    full_inferred_single_batch_config = (
        get_full_pandas_inferred_datasource_single_batch()
    )
    # Snippet: single batch pattern in default_regex
    # Snippet: single batch group_names in default_regex
    datasource_config = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "default_regex": {
                    # <snippet name="datasource_configuration_inferred_single_batch_regex_pattern">
                    "pattern": "(.*)\\.csv",
                    # </snippet>
                    # <snippet name="datasource_configuration_inferred_single_batch_group_names">
                    "group_names": ["data_asset_name"],
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_inferred_single_batch_config)

    # Snippet: full inferred_data_connector with single batch default_regex
    datasource_config = {
        "data_connectors": {
            # <snippet name="data_connector_configuration_post_single_batch_regex">
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {
                    "pattern": "(.*)\\.csv",
                    "group_names": ["data_asset_name"],
                },
            }
            # </snippet>
        }
    }
    is_subset(datasource_config, full_inferred_single_batch_config)

    # Snippet: Full configuration for inferred Datasource with single batch default_regex.
    # <snippet name="datasource_configuration_post_inferred_data_connector">
    datasource_config = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_inferred_single_batch_config)


def validate_pandas_datasource_configuration_inferred_multi_batch_snippets():
    full_inferred_multi_batch_config = get_full_pandas_inferred_datasource_multi_batch()
    # Snippet: multibatch pattern for inferred Data Connector default_regex
    # Snippet: multibatch group_names for inferred Data Connector default_regex
    datasource_config = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "default_regex": {
                    # <snippet name="datasource_configuration_inferred_multi_batch_regex_pattern">
                    "pattern": "(yellow_tripdata_sample_2020)-(\\d.*)\\.csv",
                    # </snippet>
                    # <snippet name="group_names for inferred multi batch default_regex key">
                    "group_names": ["data_asset_name", "month"],
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet: full data connector config for inferred Data Connector multi batch
    datasource_config = {
        "data_connectors": {
            # <snippet name="data connector config for inferred filesystem data connector multi batch">
            "name_of_my_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "default_regex": {
                    "pattern": "(yellow_tripdata_sample_2020)-(\\d.*)\\.csv",
                    "group_names": ["data_asset_name", "month"],
                },
            }
            # </snippet>
        }
    }
    is_subset(datasource_config, full_inferred_multi_batch_config)

    # Snippet: Full configuration for Pandas multibatch Inferred Datasource.
    # <snippet name="datasource_configuration_post_multi_batch_inferred_data_connector">
    datasource_config = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_inferred_multi_batch_config)


def validate_pandas_datasource_configuration_configured_snippets():
    # Get the full configurations to test against
    full_configured_single_batch_config = (
        get_full_pandas_configured_datasource_single_batch()
    )
    full_configured_multi_batch_config = (
        get_full_pandas_configured_datasource_multi_batch()
    )

    # Snippet: Add a dictionary for your configured data_connector
    # <snippet name="datasource_configuration_add_empty_configured_data_connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_configured_data_connector": {}},
    }
    # </snippet>
    is_subset(datasource_config, full_configured_single_batch_config)
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Add class_name to your data connector config.
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                # <snippet name="data_connector_configuration_configured_class_name">
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                # </snippet>
                # <snippet name="data_connector_configuration_configured_base_directory">
                "base_directory": "../data",
                # </snippet>
            }
        }
    }
    is_subset(datasource_config, full_configured_single_batch_config)
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Full config for data connector, with empty assets dictionary
    # <snippet name="datasource_config_configured_data_connector_empty_data_asset">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {},
                "batch_spec_passthrough": {},
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_configured_single_batch_config)
    is_subset(datasource_config, full_configured_multi_batch_config)


def validate_pandas_datasource_configuration_configured_single_batch_snippets():
    # Get the full configurations to test against
    full_configured_single_batch_config = (
        get_full_pandas_configured_datasource_single_batch()
    )

    # Snippet: Empty dictionary for an asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="empty_data_asset_configuration_configured_single_batch">
                    "yellow_tripdata_jan": {}
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_single_batch_config)

    # Snippet: Single batch pattern for an asset configuration.
    # Snippet: Single batch group_names for an asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    "yellow_tripdata_jan": {
                        # <snippet name="pattern_for_single_batch_configured_data_asset_configuration">
                        "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                        # </snippet>
                        # <snippet name="group_names for single batch configured assets configuration">
                        "group_names": ["month"],
                        # </snippet>
                    }
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_single_batch_config)

    # Snippet: Full single batch asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="full_data_asset_for_single_batch_configured_data_connector">
                    "yellow_tripdata_jan": {
                        "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                        "group_names": ["month"],
                    }
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_single_batch_config)

    # Snippet: Full configuration for a Configured Data Connector with a single-batch Data Asset.
    datasource_config = {
        "data_connectors": {
            # <snippet name="full_single_batch_configured_data_connector_configuration">
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    "yellow_tripdata_jan": {
                        "pattern": "yellow_tripdata_sample_2020-(01)\\.csv",
                        "group_names": ["month"],
                    }
                },
            }
            # </snippet>
        }
    }
    is_subset(datasource_config, full_configured_single_batch_config)

    # Snippet: Full configuration for a Datasource using a Configured Data Connector and single-batch Data Asset.
    # <snippet name="full datasource_config for pandas execution engine, configured data connector; single batch asset">
    datasource_config = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_configured_single_batch_config)


def validate_pandas_datasource_configuration_configured_multi_batch_snippets():
    # Get the full configurations to test against
    full_configured_multi_batch_config = (
        get_full_pandas_configured_datasource_multi_batch()
    )

    # Snippet: Empty dictionary for an asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="empty data asset for configured, multi batch datasource">
                    "yellow_tripdata_2020": {}
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Single batch pattern for an asset configuration.
    # Snippet: Single batch group_names for an asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    "yellow_tripdata_2020": {
                        # <snippet name="pattern for asset in configured, multi-batch datasource">
                        "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                        # </snippet>
                        # <snippet name="group_names for asset in configured, multi-batch datasource">
                        "group_names": ["month"],
                        # </snippet>
                    }
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Full single batch asset configuration.
    datasource_config = {
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "assets": {
                    # <snippet name="multi batch data asset for configured pandas datasource">
                    "yellow_tripdata_2020": {
                        "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                        "group_names": ["month"],
                    }
                    # </snippet>
                }
            }
        }
    }
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Full configuration for a Configured Data Connector with a single-batch Data Asset.
    datasource_config = {
        "data_connectors": {
            # <snippet name="data connector for configured, multi-batch, Pandas datasource">
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
                "assets": {
                    "yellow_tripdata_2020": {
                        "pattern": "yellow_tripdata_sample_2020-(.*)\\.csv",
                        "group_names": ["month"],
                    }
                },
            }
            # </snippet>
        }
    }
    is_subset(datasource_config, full_configured_multi_batch_config)

    # Snippet: Full configuration for a Datasource using a Configured Data Connector and single-batch Data Asset.
    # <snippet name="full config for Pandas, configured, multi-batch Datasource">
    datasource_config = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_configured_multi_batch_config)


def validate_pandas_datsource_configuration_runtime_snippets():
    full_runtime_config = get_full_pandas_runtime_datasource()

    # Snippet: Add empty dictionary for runtime data connector.
    # <snippet name="add empty dictionary for runtime data connector">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {"name_of_my_runtime_data_connector": {}},
    }
    # </snippet>
    is_subset(datasource_config, full_runtime_config)

    # Snippet: add class name to runtime Data Connector config.
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                # <snippet name="class_name for pandas runtime data connector">
                "class_name": "RuntimeDataConnector",
                # </snippet>
            }
        }
    }
    is_subset(datasource_config, full_runtime_config)

    # Snippet: Full datasource config for runtime data connector with blank batch_identifiers list.
    # <snippet name="full config for pandas runtime Datasource with blank data asset identifiers">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_spec_passthrough": {},
                "batch_identifiers": [],
            }
        },
    }
    # </snippet>
    is_subset(datasource_config, full_runtime_config)

    # Snippet: Adding a batch_identifiers entry for batch_timestamp.
    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_runtime_data_connector": {
                # <snippet name="batch_identifiers for filesystem runtime data connector">
                "batch_identifiers": ["batch_timestamp"]
                # </snippet>
            }
        }
    }
    is_subset(datasource_config, full_runtime_config)

    # Snippet: Full Pandas Datasource configuration with Runtime data connector and timestamp batch identifier.
    # <snippet name="full pandas runtime Datasource configuration">
    datasource_config: dict = {
        "name": "my_datasource_name",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
    is_subset(datasource_config, full_runtime_config)


def validate_pandas_batch_spec_passthrough_config_for_inferred():
    full_configs = (
        get_full_pandas_inferred_datasource_single_batch(),
        get_full_pandas_inferred_datasource_multi_batch(),
        # get_full_pandas_configured_datasource_single_batch(),
        # get_full_pandas_configured_datasource_multi_batch(),
        # get_full_pandas_runtime_datasource(),
    )

    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="empty batch_spec_passthrough">
                "batch_spec_passthrough": {
                    "reader_method": "",
                    "reader_options": {},
                    # </snippet>
                },
            }
        },
    }
    for full_config in full_configs:
        is_subset(datasource_config, full_config)

    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "batch_spec_passthrough": {
                    # <snippet name="set reader_method to csv">
                    "reader_method": "csv",
                    # </snippet>
                    # <snippet name="empty keys for reader_options">
                    "reader_options": {
                        "header": "",
                        "inferSchema": "",
                    },
                    # </snippet>
                },
            }
        },
    }
    for full_config in full_configs:
        is_subset(datasource_config, full_config)

    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                "batch_spec_passthrough": {
                    "reader_options": {
                        # <snippet name="populate header for reader_options as True">
                        "header": True,
                        # </snippet>
                        # <snippet name="populate inferSchema for reader_options as True">
                        "inferSchema": True,
                        # </snippet>
                    },
                },
            }
        },
    }
    for full_config in full_configs:
        is_subset(datasource_config, full_config)

    datasource_config: dict = {
        "data_connectors": {
            "name_of_my_inferred_data_connector": {
                # <snippet name="fully populated batch_spec_passthrough configuration">
                "batch_spec_passthrough": {
                    "reader_method": "csv",
                    "reader_options": {
                        "header": True,
                        "inferSchema": True,
                    },
                },
                # </snippet>
            }
        },
    }
    for full_config in full_configs:
        is_subset(datasource_config, full_config)

    # <snippet name="inferred datasource_config up to batch_spec_passthrough">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
    is_subset(datasource_config, get_full_pandas_inferred_datasource_multi_batch())
    is_subset(datasource_config, get_full_pandas_inferred_datasource_single_batch())

    # <snippet name="configured datasource_config up to batch_spec_passthrough">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "name_of_my_configured_data_connector": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "../data",
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
    is_subset(datasource_config, get_full_pandas_configured_datasource_multi_batch())
    is_subset(datasource_config, get_full_pandas_configured_datasource_single_batch())

    # <snippet name="runtime datasource_config up to batch_spec_passthrough">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
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
    is_subset(datasource_config, get_full_pandas_runtime_datasource())


validate_pandas_datasource_configuration_snippets()

validate_pandas_datasource_configuration_inferred_snippets()
validate_pandas_datasource_configuration_inferred_single_batch_snippets()
validate_pandas_datasource_configuration_inferred_multi_batch_snippets()

validate_pandas_datasource_configuration_configured_snippets()
validate_pandas_datasource_configuration_configured_single_batch_snippets()
validate_pandas_datasource_configuration_configured_multi_batch_snippets()

validate_pandas_datsource_configuration_runtime_snippets()

validate_pandas_batch_spec_passthrough_config_for_inferred()


def test_pandas_inferred_single_batch_full_configuration():
    datasource_config = get_full_pandas_inferred_datasource_single_batch()
    test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
    datasource_check = test_result.self_check(max_examples=12)

    # NOTE: The following code is only for testing and can be ignored by users.
    # Assert that all of our files have individually become data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "data_asset_count"
        ]
        == 12
    )
    # Assert that all of our data assets have only a single batch definition.
    for data_asset in datasource_check["data_connectors"][
        "name_of_my_inferred_data_connector"
    ]["data_assets"].values():
        assert data_asset["batch_definition_count"] == 1
    # Assert that all of our files are now associated with data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "unmatched_data_reference_count"
        ]
        == 0
    )
    # Assert that the data assets have the expected name format.
    assert "yellow_tripdata_sample_2020-01" in set(
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "data_assets"
        ].keys()
    )
    # Assert that the data assets' data references consist of a list of a single .csv file.
    assert ["yellow_tripdata_sample_2020-01.csv",] in [
        _["example_data_references"]
        for _ in datasource_check["data_connectors"][
            "name_of_my_inferred_data_connector"
        ]["data_assets"].values()
    ]


def test_pandas_inferred_multi_batch_full_configuration():
    datasource_config = get_full_pandas_inferred_datasource_multi_batch()
    test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
    datasource_check = test_result.self_check(max_examples=12)

    # NOTE: The following code is only for testing and can be ignored by users.
    # Assert that all of our files have individually become data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "data_asset_count"
        ]
        == 1
    )
    # Assert that our data asset contains all of the source files.
    assert (
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "data_assets"
        ]["yellow_tripdata_sample_2020"]["batch_definition_count"]
        == 12
    )
    # Assert that all of our files are now associated with data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_inferred_data_connector"][
            "unmatched_data_reference_count"
        ]
        == 0
    )


def test_pandas_configured_single_batch_full_configuration():
    datasource_config = get_full_pandas_configured_datasource_single_batch()
    test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
    datasource_check = test_result.self_check(max_examples=12)

    # NOTE: The following code is only for testing and can be ignored by users.
    # Assert that there is only one data asset, since only one was explicitly defined.
    assert (
        datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "data_asset_count"
        ]
        == 1
    )
    # Assert that all of our data assets have only a single batch definition.
    for data_asset in datasource_check["data_connectors"][
        "name_of_my_configured_data_connector"
    ]["data_assets"].values():
        assert data_asset["batch_definition_count"] == 1
    # Assert that all of our other files aren't associated with anything.
    assert (
        datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "unmatched_data_reference_count"
        ]
        == 11
    )
    # Assert that the data assets have the expected name format.
    assert (
        "yellow_tripdata_jan"
        in datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "data_assets"
        ].keys()
    )
    # Assert that the data assets' data references consist of a list of a single .csv file.
    assert ["yellow_tripdata_sample_2020-01.csv",] in [
        _["example_data_references"]
        for _ in datasource_check["data_connectors"][
            "name_of_my_configured_data_connector"
        ]["data_assets"].values()
    ]


def test_pandas_configured_multi_batch_full_configuration():
    datasource_config = get_full_pandas_configured_datasource_multi_batch()
    test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
    datasource_check = test_result.self_check(max_examples=12)

    # NOTE: The following code is only for testing and can be ignored by users.
    # Assert that all of our files have individually become data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "data_asset_count"
        ]
        == 1
    )
    # Assert that our data asset contains all of the source files.
    assert (
        datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "data_assets"
        ]["yellow_tripdata_2020"]["batch_definition_count"]
        == 12
    )
    # Assert that all of our files are now associated with data assets.
    assert (
        datasource_check["data_connectors"]["name_of_my_configured_data_connector"][
            "unmatched_data_reference_count"
        ]
        == 0
    )


def test_pandas_configured_runtime_full_configuration():
    datasource_config = get_full_pandas_runtime_datasource()

    test_result = data_context.test_yaml_config(yaml.dump(datasource_config))
    datasource_check = test_result.self_check(max_examples=12)

    # NOTE: The following code is only for testing and can be ignored by users.
    # Assert that there are no data sets -- those get defined in a Batch Request.
    assert (
        datasource_check["data_connectors"]["name_of_my_runtime_data_connector"][
            "data_asset_count"
        ]
        == 0
    )


def test_adding_your_datasource_to_the_datacontext():
    datasource_config = get_full_pandas_runtime_datasource()

    # <snippet name="test your config with test_yaml_config">
    data_context.test_yaml_config(yaml.dump(datasource_config))
    # </snippet>

    # <snippet name="add your datasource to your data_context">
    data_context.add_datasource(**datasource_config)
    # </snippet>

    # <snippet name="add your datasource to your data_context only if it does not already exit">
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


test_pandas_inferred_single_batch_full_configuration()
test_pandas_inferred_multi_batch_full_configuration()
test_pandas_configured_single_batch_full_configuration()
test_pandas_configured_multi_batch_full_configuration()
test_pandas_configured_runtime_full_configuration()
test_adding_your_datasource_to_the_datacontext()
