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

    my_row_count_rule_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "kwargs": {"min_value": 6712, "max_value": 9288, "mostly": 1.0},
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
                            "domain_kwargs": {
                                "column": "VendorID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "VendorID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "passenger_count",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "passenger_count",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "trip_distance",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "trip_distance",
                    "min_value": 21.42,
                    "max_value": 74.05,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "trip_distance",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "RatecodeID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "RatecodeID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "PULocationID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "PULocationID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "DOLocationID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "DOLocationID",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "payment_type",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "payment_type",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -76.43,
                    "max_value": 3.43,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {
                                "column": "fare_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "fare_amount",
                    "min_value": -1982.49,
                    "max_value": 5201.49,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "fare_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "extra",
                    "min_value": -64.85,
                    "max_value": 27.14,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {
                                "column": "extra",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "extra",
                    "min_value": 2.53,
                    "max_value": 8.97,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "extra",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "mta_tax",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "mta_tax",
                    "min_value": -28.66,
                    "max_value": 66.67,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "mta_tax",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "tip_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tip_amount",
                    "min_value": 24.4,
                    "max_value": 97.3,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "tip_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "tolls_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "tolls_amount",
                    "min_value": -351.05,
                    "max_value": 875.12,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "tolls_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "improvement_surcharge",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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
                            "domain_kwargs": {
                                "column": "improvement_surcharge",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -75.26,
                    "max_value": -1.84,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {
                                "column": "total_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "total_amount",
                    "min_value": -1405.9,
                    "max_value": 4948.55,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "total_amount",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -4.47,
                    "max_value": 1.97,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.min",
                            "domain_kwargs": {
                                "column": "congestion_surcharge",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
        ),
        ExpectationConfiguration(
            **{
                "kwargs": {
                    "column": "congestion_surcharge",
                    "min_value": -1.97,
                    "max_value": 4.47,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "metric_name": "column.max",
                            "domain_kwargs": {
                                "column": "congestion_surcharge",
                            },
                        },
                        "num_batches": 2,
                    },
                },
            },
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

    yaml = YAML()
    profiler_config: dict = yaml.load(verbose_profiler_config)
    expected_expectation_suite.add_citation(
        comment="Suite created by Rule-Based Profiler with the following config",
        profiler_config=profiler_config,
    )

    return {
        "profiler_config": verbose_profiler_config,
        "expected_expectation_suite_name": expectation_suite_name,
        "expected_expectation_suite": expected_expectation_suite,
    }
