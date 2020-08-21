#!/usr/bin/env python3

import logging
import socket
import sys
import time
import uuid

import pandas as pd

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

logging.basicConfig(level=logging.INFO)


def guard(*args, **kwargs):
    raise ConnectionError("Internet Access is Blocked!")


def main(
    nap_duration=1, block_network=False, usage_statistics_enabled=True,
):
    if block_network:
        socket.socket = guard

    print("Beginning to construct a DataContext.")
    config = DataContextConfig(
        config_version=2,
        datasources={"pandas": {"class_name": "PandasDatasource"}},
        expectations_store_name="expectations",
        validations_store_name="validations",
        evaluation_parameter_store_name="evaluation_parameters",
        plugins_directory=None,
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    }
                ],
            }
        },
        stores={
            "expectations": {"class_name": "ExpectationsStore"},
            "validations": {"class_name": "ValidationsStore"},
            "evaluation_parameters": {"class_name": "EvaluationParameterStore"},
        },
        data_docs_sites={},
        config_variables_file_path=None,
        anonymous_usage_statistics={
            "enabled": usage_statistics_enabled,
            # Leaving data_context_id as none would cause a new id to be generated
            # This will be overridden when tests set an environment variable
            "usage_statistics_url": "https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics",
        },
        commented_map=None,
    )
    context = BaseDataContext(project_config=config)
    print("Done constructing a DataContext.")
    print("Building a suite and validating.")
    df = pd.DataFrame({"a": [1, 2, 3]})
    context.create_expectation_suite("testing.batch")
    batch = context.get_batch(
        batch_kwargs={"datasource": "pandas", "dataset": df},
        expectation_suite_name="testing.batch",
    )
    batch.expect_column_values_to_be_between("a", 0, 5)
    batch.expect_column_to_exist("a")
    batch.save_expectation_suite()
    res = context.run_validation_operator("action_list_operator", [batch])
    print(res)
    print("Beginning a nap.")
    time.sleep(nap_duration)
    print("Ending a long nap.")


if __name__ == "__main__":

    try:
        nap_duration = int(sys.argv[2])
    except IndexError:
        nap_duration = 1
    except ValueError:
        print("Unrecognized sleep duration. Using 1 second.")
        nap_duration = 1

    try:
        res = sys.argv[3]
        if res in ["y", "yes", "True", "true", "t", "T"]:
            block_network = True
        else:
            block_network = False
    except IndexError:
        block_network = False
    except ValueError:
        print("Unrecognized value for block_network. Setting to false.")
        block_network = False

    try:
        res = sys.argv[4]
        if res in ["y", "yes", "True", "true", "t", "T"]:
            usage_statistics_enabled = True
        else:
            usage_statistics_enabled = False
    except IndexError:
        usage_statistics_enabled = True
    except ValueError:
        print("Unrecognized value for usage_statistics_enabled. Setting to True.")
        usage_statistics_enabled = True

    ge_logger = logging.getLogger("great_expectations")
    ge_logger.setLevel(logging.DEBUG)
    main(
        nap_duration,
        block_network=block_network,
        usage_statistics_enabled=usage_statistics_enabled,
    )
