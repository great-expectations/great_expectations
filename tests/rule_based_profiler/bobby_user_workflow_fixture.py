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
                    "min_value": 6712,
                    "max_value": 9288,
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
                            "metric_domain_kwargs": {
                                "column": "VendorID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": -1,
                    "max_value": 2,
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
                            "metric_domain_kwargs": {
                                "column": "passenger_count",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "trip_distance",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 21.42290366424798,
                    "max_value": 74.04709633575202,
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
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "RatecodeID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "RatecodeID",
                    "min_value": 4,
                    "max_value": 7,
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
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "PULocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "DOLocationID",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "payment_type",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -76.42535420500796,
                    "max_value": 3.425354205007963,
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
                            "metric_domain_kwargs": {
                                "column": "fare_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -1982.4939637989423,
                    "max_value": 5201.493963798943,
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
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "extra",
                    "min_value": -64.84643221486563,
                    "max_value": 27.136432214865625,
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
                            "metric_domain_kwargs": {
                                "column": "extra",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "extra",
                    "min_value": 2.530213370563874,
                    "max_value": 8.969786629436125,
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
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "mta_tax",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -28.660721262172405,
                    "max_value": 66.6707212621724,
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
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "tip_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 24.40201535478306,
                    "max_value": 97.29798464521694,
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
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "tolls_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": -351.0510116841917,
                    "max_value": 875.1210116841917,
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
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "improvement_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
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
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -75.25556757557183,
                    "max_value": -1.8444324244281631,
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
                            "metric_domain_kwargs": {
                                "column": "total_amount",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -1405.8960501949803,
                    "max_value": 4948.54605019498,
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
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -4.4697866294361255,
                    "max_value": 1.969786629436126,
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
                            "metric_domain_kwargs": {
                                "column": "congestion_surcharge",
                                "batch_id": "92bcffc67c34a1c9a67e0062ed4a9529",
                            },
                        }
                    }
                },
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -1.969786629436126,
                    "max_value": 4.4697866294361255,
                    "mostly": 1.0,
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
