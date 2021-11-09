from typing import List

import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
@freeze_time("09/26/2019 13:42:41")
def bobby_columnar_table_multi_batch():
    """
    # TODO: <Alex>ALEX -- Add DocString</Alex>
    """

    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "bobby_user_workflow_verbose_profiler_config.yml"
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

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(
        my_row_count_range_rule_expectation_configurations_oneshot_sampling_method
    )
    expectation_configurations.extend(
        my_column_ranges_rule_expectation_configurations_oneshot_sampling_method
    )

    expectation_suite_name_oneshot_sampling_method: str = (
        "bobby_columnar_table_multi_batch_oneshot_sampling_method"
    )
    expected_expectation_suite_oneshot_sampling_method: ExpectationSuite = (
        ExpectationSuite(
            expectation_suite_name=expectation_suite_name_oneshot_sampling_method
        )
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite_oneshot_sampling_method.add_expectation(
            expectation_configuration
        )

    yaml = YAML()
    profiler_config: dict = yaml.load(verbose_profiler_config)
    expected_expectation_suite_oneshot_sampling_method.add_citation(
        comment="Suite created by Rule-Based Profiler with the configuration included.",
        profiler_config=profiler_config,
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration_oneshot_sampling_method": {
            "expectation_suite_name": expectation_suite_name_oneshot_sampling_method,
            "expected_expectation_suite": expected_expectation_suite_oneshot_sampling_method,
        },
    }
