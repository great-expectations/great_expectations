from typing import List

import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
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
                "kwargs": {"min_value": 6179, "max_value": 9821, "mostly": 1.0},
                "expectation_type": "expect_table_row_count_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "metric_domain_kwargs": {},
                        }
                    }
                },
            },
        ),
    ]

    my_column_ranges_rule_expectation_configurations_oneshot_sampling_method: List[
        ExpectationConfiguration
    ] = [
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "VendorID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "VendorID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": -1,
                    "max_value": 2,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 6,
                    "max_value": 6,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 10.52,
                    "max_value": 84.95,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 4,
                    "max_value": 7,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -92.96,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": 0.0,
                    "max_value": 6689.35,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "extra",
                    "min_value": -83.9,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "extra",
                    "min_value": 1.2,
                    "max_value": 10.3,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -0.5,
                    "max_value": -0.5,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": 0.0,
                    "max_value": 86.41,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 9.3,
                    "max_value": 112.4,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": 0.0,
                    "max_value": 1129.07,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": -0.3,
                    "max_value": -0.3,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": 0.3,
                    "max_value": 0.3,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -90.46,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "total_amount",
                    "min_value": 0.0,
                    "max_value": 6264.59,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -5.8,
                    "max_value": 3.3,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -3.3,
                    "max_value": 5.8,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "021563e94d7866f395288f6e306aed9b",
                            },
                        }
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

    expectation_suite_name_bootstrap_sampling_method: str = (
        "bobby_columnar_table_multi_batch_bootstrap_sampling_method"
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value: float = (
        8000
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value: float = (
        380
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds: float = (
        2.6
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        - (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_min_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value
        - 5e-1
        * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_max_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_min_value_mean_value
        + 5e-1
        * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
    )

    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_mean_value
        + (
            my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_num_stds
            * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
        )
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_min_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value
        - 5e-1
        * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
    )
    my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_max_value: int = round(
        my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_max_value_mean_value
        + 5e-1
        * my_row_count_range_rule_expect_table_row_count_to_be_between_expectation_std_value
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration_oneshot_sampling_method": {
            "expectation_suite_name": expectation_suite_name_oneshot_sampling_method,
            "expected_expectation_suite": expected_expectation_suite_oneshot_sampling_method,
        },
    }
