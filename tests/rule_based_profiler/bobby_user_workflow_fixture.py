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

    my_row_count_rule_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 6727,
                    "max_value": 9940,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "table.row_count",
                            "metric_domain_kwargs": {
                                "batch_id": "021563e94d7866f395288f6e306aed9b"
                            },
                        }
                    }
                },
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "VendorID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "VendorID",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "VendorID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": -1,
                    "max_value": 2,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 6,
                    "max_value": 6,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 23.851853257587763,
                    "max_value": 68.3948134090789,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 4,
                    "max_value": 7,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "PULocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "DOLocationID",
                    "min_value": 265,
                    "max_value": 265,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 1,
                    "max_value": 1,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "payment_type",
                    "min_value": 4,
                    "max_value": 4,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -68.59847823682462,
                    "max_value": 4.931811570157951,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -2209.1828021039673,
                    "max_value": 4521.849468770634,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "extra",
                    "min_value": -56.21022384832043,
                    "max_value": 30.270223848320434,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "extra",
                    "min_value": 3.131022720469251,
                    "max_value": 9.202310612864082,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -0.5,
                    "max_value": -0.5,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -32.10300631283988,
                    "max_value": 57.77633964617321,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 13.525954664480246,
                    "max_value": 93.50737866885311,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": -369.11158771824796,
                    "max_value": 753.6649210515812,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": -0.3,
                    "max_value": -0.3,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "improvement_surcharge",
                    "min_value": 0.3,
                    "max_value": 0.3,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -67.92491675149579,
                    "max_value": -1.0084165818375368,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -1920.1647047134945,
                    "max_value": 4448.798038046828,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -4.702310612864083,
                    "max_value": 1.3689772795307495,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -1.3689772795307495,
                    "max_value": 4.702310612864083,
                    "mostly": 1.0,
                },
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
            }
        ),
    ]

    expectation_configurations: List[ExpectationConfiguration] = []

    expectation_configurations.extend(my_row_count_rule_expectation_configurations)

    expectation_suite_name: str = "bobby_columnar_table_multi_batch"
    expected_expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in expectation_configurations:
        expected_expectation_suite.add_expectation(expectation_configuration)

    return {
        "profiler_config": verbose_profiler_config,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
    }
