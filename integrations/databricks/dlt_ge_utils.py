import datetime
from typing import Union

import pandas as pd
import pyspark.sql
from ruamel import yaml

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext


def run_ge_checkpoint_on_dataframe_from_suite(
    data_context: BaseDataContext,
    df: Union[pd.DataFrame, pyspark.sql.DataFrame],
    expectation_configuration: ExpectationConfiguration,
) -> CheckpointResult:
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
    # TODO: allow expectation suite customization
    expectation_suite_name: str = f"tmp_expectation_suite_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
    expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name
    )
    if expectation_configuration is not None:
        expectation_suite.append_expectation(
            expectation_config=expectation_configuration
        )

    # TODO: We probably don't want to save this expectation suite, just doing it here out of convenience temporarily to use the checkpoint. In the future we may wish to work with in-memory Checkpoints.
    data_context.save_expectation_suite(
        expectation_suite=expectation_suite,
        expectation_suite_name=expectation_suite_name,
        overwrite_existing=True,
    )

    # Run Checkpoint & Actions
    checkpoint_name: str = (
        f"tmp_checkpoint_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
    )
    yaml_config: str = f"""
                    name: {checkpoint_name}
                    config_version: 1.0
                    class_name: SimpleCheckpoint
                    run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
                    """

    data_context.add_checkpoint(**yaml.load(yaml_config))

    checkpoint_result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    )
    print(
        f"GE Checkpoint was just run with converted expectations. Result of GE Checkpoint: {checkpoint_result.success}",
        "\n",
    )

    return checkpoint_result
