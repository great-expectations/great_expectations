import datetime
import os
import shutil
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
)
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from tests.conftest import skip_if_python_below_minimum_version

yaml = YAML(typ="safe")


@pytest.fixture
def alice_columnar_table_single_batch_context(
    monkeypatch,
    empty_data_context,
    alice_columnar_table_single_batch,
):
    skip_if_python_below_minimum_version()

    context: DataContext = empty_data_context
    monkeypatch.chdir(context.root_directory)
    data_relative_path: str = "../data"
    data_path: str = os.path.join(context.root_directory, data_relative_path)
    os.makedirs(data_path, exist_ok=True)

    # Copy data
    filename: str = alice_columnar_table_single_batch["sample_data_relative_path"]
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "..",
                "test_sets",
                f"{filename}",
            ),
        ),
        str(os.path.join(data_path, filename)),
    )

    data_connector_base_directory: str = "./"
    monkeypatch.setenv("base_directory", data_connector_base_directory)
    monkeypatch.setenv("data_fixtures_root", data_relative_path)

    datasource_name: str = "alice_columnar_table_single_batch_datasource"
    data_connector_name: str = "alice_columnar_table_single_batch_data_connector"
    data_asset_name: str = "alice_columnar_table_single_batch_data_asset"
    datasource_config: str = rf"""
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  {data_connector_name}:
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      {data_asset_name}:
        module_name: great_expectations.datasource.data_connector.asset
        group_names:
          - filename
        pattern: (.*)\.csv
        reader_options:
          delimiter: ","
        class_name: Asset
        base_directory: ${{data_fixtures_root}}
        glob_directive: "*.csv"
    base_directory: ${{base_directory}}
    module_name: great_expectations.datasource.data_connector
        """

    context.add_datasource(name=datasource_name, **yaml.load(datasource_config))

    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                data_connector_name: {
                    "assets": {
                        data_asset_name: {
                            "base_directory": data_relative_path,
                            "class_name": "Asset",
                            "glob_directive": "*.csv",
                            "group_names": ["filename"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "(.*)\\.csv",
                        }
                    },
                    "base_directory": data_connector_base_directory,
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": datasource_name,
        }
    ]
    return context


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def bobby_columnar_table_multi_batch(empty_data_context):
    """
    About the "Bobby" User Workflow Fixture
    Bobby has multiple tables of columnar data called user_events (DataAsset) that he wants to check periodically as new
    data is added.
      - He knows what some of the columns are of the accounting/financial/account type.
    He wants to use a configurable profiler to generate a description (ExpectationSuite) about tables so that he can:
        1. monitor the average number of rows in the tables
        2. use it to validate min/max boundaries of all columns are of the accounting/financial/account type and set up
           alerts for when things change
        3. have a place to add his domain knowledge of the data (that can also be validated against new data)
        4. if all goes well, generalize some of the Profiler to use on his other tables
    Bobby uses a crude, highly inaccurate deterministic parametric estimator -- for illustrative purposes.
    Bobby configures his Profiler using the YAML configurations and data file locations captured in this fixture.
    """
    skip_if_python_below_minimum_version()

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_fixtures",
            "rule_based_profiler",
            "bobby_user_workflow_verbose_profiler_config.yml",
        ),
    )

    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    my_row_count_range_rule_expectation_configurations_oneshot_sampling_method: List[
        ExpectationConfiguration
    ] = [
        ExpectationConfiguration(
            **{
                "kwargs": {"min_value": 7505, "max_value": 8495},
                "expectation_type": "expect_table_row_count_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "domain_kwargs": {},
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
    ]

    my_column_ranges_rule_expectation_configurations_oneshot_sampling_method: List[
        ExpectationConfiguration
    ] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "VendorID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "VendorID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "passenger_count"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 0,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "passenger_count"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 6,
                    "max_value": 6,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "trip_distance"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "trip_distance"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 37.62,
                    "max_value": 57.85,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "RatecodeID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "RatecodeID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 5,
                    "max_value": 6,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "PULocationID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "PULocationID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "DOLocationID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "DOLocationID"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "payment_type"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "payment_type"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "fare_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -51.84,
                    "max_value": -21.16,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "fare_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": 228.94,
                    "max_value": 2990.05,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "extra"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "extra",
                    "min_value": -36.53,
                    "max_value": -1.18,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "extra"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "extra",
                    "min_value": 4.51,
                    "max_value": 6.99,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "mta_tax"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -0.5,
                    "max_value": -0.5,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "mta_tax"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": 0.69,
                    "max_value": 37.32,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "tip_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "tip_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 46.84,
                    "max_value": 74.86,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "tolls_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "tolls_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": 26.4,
                    "max_value": 497.67,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "improvement_surcharge"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": -0.3,
                    "max_value": -0.3,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "improvement_surcharge"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": 0.3,
                    "max_value": 0.3,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "total_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -52.66,
                    "max_value": -24.44,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "total_amount"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "total_amount",
                    "min_value": 550.18,
                    "max_value": 2992.47,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {"column": "congestion_surcharge"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -2.49,
                    "max_value": -0.01,
                    "mostly": 1.0,
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {"column": "congestion_surcharge"},
                        },
                        "num_batches": 2,
                    }
                },
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": 0.01,
                    "max_value": 2.49,
                    "mostly": 1.0,
                },
            },
        ),
    ]

    my_column_timestamps_rule_expectation_configurations_oneshot_sampling_method: List[
        ExpectationConfiguration
    ] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_strftime_format",
                "kwargs": {
                    "column": "pickup_datetime",
                    "strftime_format": {
                        "value": "%Y-%m-%d %H:%M:%S",
                        "details": {"success_ratio": 1.0},
                    },
                },
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in _datetime are of the format detected by parameter builder SimpleDateFormatStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_strftime_format",
                "kwargs": {
                    "column": "dropoff_datetime",
                    "strftime_format": {
                        "value": "%Y-%m-%d %H:%M:%S",
                        "details": {"success_ratio": 1.0},
                    },
                },
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in _datetime are of the format detected by parameter builder SimpleDateFormatStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
    ]

    my_column_regex_rule_expectation_configurations_oneshot_sampling_method: List[
        ExpectationConfiguration
    ] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "VendorID",
                },
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in ID are of the format detected by parameter builder RegexPatternStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_regex",
                "meta": {"notes": {"format": "markdown", "content": None}},
                "kwargs": {"column": "RatecodeID"},
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in ID are of the format detected by parameter builder RegexPatternStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_regex",
                "meta": {"notes": {"format": "markdown", "content": None}},
                "kwargs": {"column": "PULocationID"},
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in ID are of the format detected by parameter builder RegexPatternStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_match_regex",
                "meta": {"notes": {"format": "markdown", "content": None}},
                "kwargs": {"column": "DOLocationID"},
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": [
                            "### This expectation confirms that fields ending in ID are of the format detected by parameter builder RegexPatternStringParameterBuilder"
                        ],
                    }
                },
            }
        ),
    ]

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(
        my_row_count_range_rule_expectation_configurations_oneshot_sampling_method
    )
    expectation_configurations.extend(
        my_column_ranges_rule_expectation_configurations_oneshot_sampling_method
    )
    expectation_configurations.extend(
        my_column_timestamps_rule_expectation_configurations_oneshot_sampling_method
    )
    expectation_configurations.extend(
        my_column_regex_rule_expectation_configurations_oneshot_sampling_method
    )
    expectation_suite_name_oneshot_sampling_method: str = (
        "bobby_columnar_table_multi_batch_oneshot_sampling_method"
    )
    expected_expectation_suite_oneshot_sampling_method: ExpectationSuite = (
        ExpectationSuite(
            expectation_suite_name=expectation_suite_name_oneshot_sampling_method,
            data_context=empty_data_context,
        )
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        # NOTE Will 20211208 add_expectation() method, although being called by an ExpectationSuite instance, is being
        # called within a fixture, and we will prevent it from sending a usage_event by calling the private method.
        expected_expectation_suite_oneshot_sampling_method._add_expectation(
            expectation_configuration=expectation_configuration, send_usage_event=False
        )

    profiler_config: dict = yaml.load(verbose_profiler_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    expected_expectation_suite_oneshot_sampling_method.add_citation(
        comment="Suite created by Rule-Based Profiler with the configuration included.",
        profiler_config=serialized_config,
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration_oneshot_sampling_method": {
            "expectation_suite_name": expectation_suite_name_oneshot_sampling_method,
            "expected_expectation_suite": expected_expectation_suite_oneshot_sampling_method,
        },
    }


@pytest.fixture
def bobby_columnar_table_multi_batch_deterministic_data_context(
    tmp_path_factory,
    monkeypatch,
) -> DataContext:
    skip_if_python_below_minimum_version()

    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "..",
                "integration",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_7500_lines_sample_2019-01.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-01.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_8500_lines_sample_2019-02.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-02.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "random_subsamples",
                "yellow_tripdata_9000_lines_sample_2019-03.csv",
            ),
        ),
        str(
            os.path.join(
                context_path, "..", "data", "yellow_tripdata_sample_2019-03.csv"
            )
        ),
    )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


@pytest.fixture
def bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000():
    """
    About the "Bobster" User Workflow Fixture

    Bobster has multiple tables of columnar data called user_events (DataAsset) that he wants to check periodically as
    new data is added.

      - He knows what some of the columns are of the acconting/financial/account type, but he is currently interested in
        the average table size (in terms of the number of rows in a table).

    He wants to use a configurable profiler to generate a description (ExpectationSuite) about tables so that he can:

        1. monitor the average number of rows in the tables

        2. have a place to add his domain knowledge of the data (that can also be validated against new data)

        3. if all goes well, generalize some of the Profiler to use on his other tables

    Bobster uses a custom implementation of the "bootstrap" non-parametric (i.e, data-driven) statistical estimator.

    Bobster configures his Profiler using the YAML configurations and data file locations captured in this fixture.
    """
    skip_if_python_below_minimum_version()

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_fixtures",
            "rule_based_profiler",
            "bobster_user_workflow_verbose_profiler_config.yml",
        ),
    )

    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    expectation_suite_name_bootstrap_sampling_method: str = (
        "bobby_columnar_table_multi_batch_bootstrap_sampling_method"
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value: int = (
        5000
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value: float = (
        1.0e3
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds: float = (
        3.00
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value: int = round(
        float(
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        )
        - (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value: int = round(
        float(
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        )
        + (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration_bootstrap_sampling_method": {
            "expectation_suite_name": expectation_suite_name_bootstrap_sampling_method,
            "expect_table_row_count_to_be_between_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value,
            "expect_table_row_count_to_be_between_min_value_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value,
            "expect_table_row_count_to_be_between_max_value_mean_value": my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value,
        },
    }


@pytest.fixture
def bobster_columnar_table_multi_batch_normal_mean_5000_stdev_1000_data_context(
    tmp_path_factory,
    monkeypatch,
) -> DataContext:
    """
    This fixture generates three years' worth (36 months; i.e., 36 batches) of taxi trip data with the number of rows
    of a batch sampled from a normal distribution with the mean of 5,000 rows and the standard deviation of 1,000 rows.
    """
    skip_if_python_below_minimum_version()

    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("taxi_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "fixtures",
                "yellow_tripdata_pandas_fixture",
                "great_expectations",
                "great_expectations.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    base_directory: str = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
        ),
    )
    file_name_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
        base_directory_path=base_directory, glob_directive="*.csv"
    )
    file_name_list = sorted(file_name_list)
    num_files: int = len(file_name_list)

    rnd_num_sample: np.float64
    output_file_lenths: List[int] = [
        round(rnd_num_sample)
        for rnd_num_sample in np.random.normal(loc=5.0e3, scale=1.0e3, size=num_files)
    ]

    idx: int
    file_name: str

    output_file_name_length_map: Dict[str, int] = {
        file_name_list[idx]: output_file_lenths[idx]
        for idx, file_name in enumerate(file_name_list)
    }

    csv_source_path: str
    df: pd.DataFrame
    for file_name in file_name_list:
        csv_source_path = os.path.join(base_directory, file_name)
        df = pd.read_csv(filepath_or_buffer=csv_source_path)
        df = df.sample(
            n=output_file_name_length_map[file_name], replace=False, random_state=1
        )
        # noinspection PyTypeChecker
        df.to_csv(
            path_or_buf=os.path.join(context_path, "..", "data", file_name), index=False
        )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    return context


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def alice_columnar_table_single_batch(empty_data_context):
    """
    About the "Alice" User Workflow Fixture

    Alice has a single table of columnar data called user_events (DataAsset) that she wants to check periodically as new
    data is added.

      - She knows what some of the columns mean, but not all - and there are MANY of them (only a subset currently shown
        in examples and fixtures).

      - She has organized other tables similarly so that for example column name suffixes indicate which are for user
        ids (_id) and which timestamps are for versioning (_ts).

    She wants to use a configurable profiler to generate a description (ExpectationSuite) about table so that she can:

        1. use it to validate the user_events table periodically and set up alerts for when things change

        2. have a place to add her domain knowledge of the data (that can also be validated against new data)

        3. if all goes well, generalize some of the Profiler to use on her other tables

    Alice configures her Profiler using the YAML configurations and data file locations captured in this fixture.
    """
    skip_if_python_below_minimum_version()

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "..",
            "test_fixtures",
            "rule_based_profiler",
            "alice_user_workflow_verbose_profiler_config.yml",
        ),
    )

    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    my_rule_for_user_ids_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {
                    "column": "user_id",
                    "type_": "INTEGER",
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "min_value": 397433,  # From the data
                    "max_value": 999999999999,
                    "column": "user_id",
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "user_id",
                },
                "meta": {},
            }
        ),
    ]

    event_ts_column_data: Dict[str, str] = {
        "column_name": "event_ts",
        "observed_max_time_str": "2004-10-19 11:05:20",
        "observed_strftime_format": "%Y-%m-%d %H:%M:%S",
    }

    my_rule_for_timestamps_column_data: List[Dict[str, str]] = [
        event_ts_column_data,
        {
            "column_name": "server_ts",
            "observed_max_time_str": "2004-10-19 11:05:20",
        },
        {
            "column_name": "device_ts",
            "observed_max_time_str": "2004-10-19 11:05:22",
        },
    ]
    my_rule_for_timestamps_expectation_configurations: List[
        ExpectationConfiguration
    ] = []
    column_data: Dict[str, str]
    for column_data in my_rule_for_timestamps_column_data:
        my_rule_for_timestamps_expectation_configurations.extend(
            [
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "type_": "TIMESTAMP",
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_increasing",
                        "kwargs": {
                            "column": column_data["column_name"],
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                        "kwargs": {
                            "column": column_data["column_name"],
                        },
                        "meta": {},
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_min_to_be_between",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "min_value": "2004-10-19T10:23:54",  # From variables
                            "max_value": "2004-10-19T10:23:54",  # From variables
                        },
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": [
                                    "### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**"
                                ],
                            }
                        },
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_max_to_be_between",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "min_value": "2004-10-19T10:23:54",  # From variables
                            "max_value": event_ts_column_data[
                                "observed_max_time_str"
                            ],  # Pin to event_ts column
                        },
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": [
                                    "### This expectation confirms that the event_ts contains the latest timestamp of all domains"
                                ],
                            }
                        },
                    }
                ),
                ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_match_strftime_format",
                        "kwargs": {
                            "column": column_data["column_name"],
                            "strftime_format": {
                                "value": event_ts_column_data[
                                    "observed_strftime_format"
                                ],  # Pin to event_ts column
                                "details": {"success_ratio": 1.0},
                            },
                        },
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": [
                                    "### This expectation confirms that fields ending in _ts are of the format detected by parameter builder SimpleDateFormatStringParameterBuilder"
                                ],
                            }
                        },
                    }
                ),
            ]
        )

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(my_rule_for_user_ids_expectation_configurations)
    expectation_configurations.extend(my_rule_for_timestamps_expectation_configurations)

    expectation_suite_name: str = "alice_columnar_table_single_batch"
    expected_expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name, data_context=empty_data_context
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        # NOTE Will 20211208 add_expectation() method, although being called by an ExpectationSuite instance, is being
        # called within a fixture, and we will prevent it from sending a usage_event by calling the private method
        # _add_expectation().
        expected_expectation_suite._add_expectation(
            expectation_configuration=expectation_configuration, send_usage_event=False
        )

    # NOTE that this expectation suite should fail when validated on the data in "sample_data_relative_path"
    # because the device_ts is ahead of the event_ts for the latest event
    sample_data_relative_path: str = "alice_columnar_table_single_batch_data.csv"

    profiler_config: dict = yaml.load(verbose_profiler_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")
    expected_expectation_suite.add_citation(
        comment="Suite created by Rule-Based Profiler with the configuration included.",
        profiler_config=serialized_config,
    )

    return {
        "profiler_config": verbose_profiler_config,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
        "sample_data_relative_path": sample_data_relative_path,
    }


# TODO: AJB 20210525 This fixture is not yet used but may be helpful to generate batches for unit tests of multibatch
#  workflows.  It should probably be extended to add different column types / data.
@pytest.fixture
def multibatch_generic_csv_generator():
    """
    Construct a series of csv files with many data types for use in multibatch testing
    """
    skip_if_python_below_minimum_version()

    def _multibatch_generic_csv_generator(
        data_path: str,
        start_date: Optional[datetime.datetime] = None,
        num_event_batches: Optional[int] = 20,
        num_events_per_batch: Optional[int] = 5,
    ) -> List[str]:

        if start_date is None:
            start_date = datetime.datetime(2000, 1, 1)

        file_list = []
        category_strings = {
            0: "category0",
            1: "category1",
            2: "category2",
            3: "category3",
            4: "category4",
            5: "category5",
            6: "category6",
        }
        for batch_num in range(num_event_batches):
            # generate a dataframe with multiple column types
            batch_start_date = start_date + datetime.timedelta(
                days=(batch_num * num_events_per_batch)
            )
            # TODO: AJB 20210416 Add more column types
            df = pd.DataFrame(
                {
                    "event_date": [
                        (batch_start_date + datetime.timedelta(days=i)).strftime(
                            "%Y-%m-%d"
                        )
                        for i in range(num_events_per_batch)
                    ],
                    "batch_num": [batch_num + 1 for _ in range(num_events_per_batch)],
                    "string_cardinality_3": [
                        category_strings[i % 3] for i in range(num_events_per_batch)
                    ],
                }
            )
            filename = f"csv_batch_{batch_num + 1:03}_of_{num_event_batches:03}.csv"
            file_list.append(filename)
            # noinspection PyTypeChecker
            df.to_csv(
                os.path.join(data_path, filename),
                index_label="intra_batch_index",
            )

        return file_list

    return _multibatch_generic_csv_generator


@pytest.fixture
def multibatch_generic_csv_generator_context(monkeypatch, empty_data_context):
    skip_if_python_below_minimum_version()

    context: DataContext = empty_data_context
    monkeypatch.chdir(context.root_directory)
    data_relative_path = "../data"
    data_path = os.path.join(context.root_directory, data_relative_path)
    os.makedirs(data_path, exist_ok=True)

    data_connector_base_directory = "./"
    monkeypatch.setenv("base_directory", data_connector_base_directory)
    monkeypatch.setenv("data_fixtures_root", data_relative_path)

    datasource_name = "generic_csv_generator"
    data_connector_name = "daily_data_connector"
    asset_name = "daily_data_asset"
    datasource_config = rf"""
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  {data_connector_name}:
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      {asset_name}:
        module_name: great_expectations.datasource.data_connector.asset
        group_names:
          - batch_num
          - total_batches
        pattern: csv_batch_(\d.+)_of_(\d.+)\.csv
        reader_options:
          delimiter: ","
        class_name: Asset
        base_directory: $data_fixtures_root
        glob_directive: "*.csv"
    base_directory: $base_directory
    module_name: great_expectations.datasource.data_connector
        """

    context.add_datasource(name=datasource_name, **yaml.load(datasource_config))

    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                data_connector_name: {
                    "assets": {
                        asset_name: {
                            "base_directory": data_relative_path,
                            "class_name": "Asset",
                            "glob_directive": "*.csv",
                            "group_names": ["batch_num", "total_batches"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "csv_batch_(\\d.+)_of_(\\d.+)\\.csv",
                        }
                    },
                    "base_directory": data_connector_base_directory,
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                }
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "generic_csv_generator",
        }
    ]
    return context
