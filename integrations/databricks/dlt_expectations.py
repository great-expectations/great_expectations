# This file contains several decorators used in Databricks Delta Live Tables
# To use these decorators, import this module and then use the decorators in place of the
# decorators provided by delta live tables.

import functools

from ruamel.yaml import YAML

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from integrations.databricks.dlt_expectation_translator import (
    translate_expectation_config_to_dlt_expectation,
)

yaml = YAML()


def expect(
    _func=None,
    *,
    dlt_expectation_name: str = None,
    data_context: BaseDataContext = None,
    expectation_configuration: ExpectationConfiguration = None,
):
    """Run a single expectation on a Delta Live Table"""

    def decorator_expect(func):
        @functools.wraps(func)
        def wrapper_expect(*args, **kwargs):

            # Preparation outside of decorator:
            # 1. Set up GE (metadata & data docs stores, runtime data connector)
            # 2. Create Expectation Suite

            # DLT Execution in decorator
            # Translate GE Suite or Expectation Config to DLT expectation
            translated_dlt_expectation = (
                translate_expectation_config_to_dlt_expectation(
                    expectation_configuration=expectation_configuration,
                    dlt_expectation_name=dlt_expectation_name,
                )
            )

            # Apply each DLT expectation as a decorator
            if len(translated_dlt_expectation) > 1:
                dlt_expect_all_expectations = {
                    name: condition for (name, condition) in translated_dlt_expectation
                }
                print(
                    f"Here we would apply: @dlt.expect_all({dlt_expect_all_expectations})"
                )
            if len(translated_dlt_expectation) == 1:
                print(
                    f"Here we would apply: @dlt.expect({translated_dlt_expectation[0][0]}, {translated_dlt_expectation[0][1]})"
                )
            # Compute resulting dataframe

            # GE Execution in decorator
            # Load GE context
            print(data_context)
            print(data_context.list_datasources())
            # Create RuntimeBatchRequest from computed resulting dataframe before records are removed
            df = args[0]

            batch_request = RuntimeBatchRequest(
                datasource_name="example_datasource",
                data_connector_name="default_runtime_data_connector_name",
                # TODO: data_asset_name as a decorator param with default?
                data_asset_name="transformation_name",  # This can be anything that identifies this data_asset for you
                runtime_parameters={"batch_data": df},  # df is your dataframe
                # TODO: batch_identifiers as a decorator param with default?
                batch_identifiers={"default_identifier_name": "default_identifier"},
            )
            # Create Expectation Suite (from Expectation Configuration, in multi-expectation decorators with suites we can use the suite directly)
            expectation_suite_name: str = "tmp_expectation_suite"
            expectation_suite: ExpectationSuite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name
            )
            expectation_suite.append_expectation(
                expectation_config=expectation_configuration
            )

            data_context.save_expectation_suite(
                expectation_suite=expectation_suite,
                expectation_suite_name=expectation_suite_name,
                overwrite_existing=True,
            )

            # Run Checkpoint & Actions
            checkpoint_name = "tmp_checkpoint"
            yaml_config = f"""
            name: {checkpoint_name}
            config_version: 1.0
            class_name: SimpleCheckpoint
            run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
            """

            data_context.add_checkpoint(**yaml.load(yaml_config))

            checkpoint_result = data_context.run_checkpoint(
                checkpoint_name=checkpoint_name,
                validations=[
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": expectation_suite_name,
                    }
                ],
            )
            print("Checkpoint result")
            print(checkpoint_result)

            return func(*args, **kwargs)

        return wrapper_expect

    if _func is None:
        return decorator_expect
    else:
        return decorator_expect(_func)
