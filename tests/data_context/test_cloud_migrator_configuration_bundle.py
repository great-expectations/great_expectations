"""TODO: Add docstring"""
import pytest
from great_expectations.core.id_dict import BatchSpec, IDDict

from great_expectations.core.batch_spec import BatchMarkers

from great_expectations.core.batch import BatchDefinition

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResultMeta,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier, ExpectationSuiteIdentifier,
)

from great_expectations.core.data_context_key import DataContextKey

from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)

from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig

from great_expectations.checkpoint import Checkpoint

from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    RunIdentifier,
)

from great_expectations.data_context.types.base import DataContextConfig

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.cloud_migrator import ConfigurationBundle


@pytest.fixture
def in_memory_runtime_context_with_configs_in_stores(in_memory_runtime_context: BaseDataContext, profiler_rules: dict) -> BaseDataContext:
    # Add items to the context:

    context: BaseDataContext = in_memory_runtime_context

    # Add expectation suite
    expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="my_suite"
    )
    context.save_expectation_suite(expectation_suite)

    # Add checkpoint
    checkpoint: Checkpoint = context.add_checkpoint(
        name="my_checkpoint", class_name="SimpleCheckpoint"
    )

    # Add profiler
    profiler = context.add_profiler(
        "my_profiler", config_version=1.0, rules=profiler_rules
    )

    # Add validation result
    expectation_suite_validation_result = ExpectationSuiteValidationResult(
        success=True,
        # meta=expectation_suite_validation_result_meta,
    )
    context.validations_store.set(
        key=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier("my_suite"),
            run_id=RunIdentifier(run_name="my_run", run_time=None),
            batch_identifier="my_batch_identifier",
        ),
        value=expectation_suite_validation_result,
    )

    return context


@pytest.mark.integration
def test_configuration_bundle_init(
    in_memory_runtime_context_with_configs_in_stores: BaseDataContext,
    in_memory_runtime_context: BaseDataContext,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
    profiler_rules: dict,
):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    bundling. We can replace this test with unit tests.
    """

    # Add items to the context:

    context: BaseDataContext = in_memory_runtime_context
    expectation_suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="my_suite"
    )
    context.save_expectation_suite(expectation_suite)
    checkpoint: Checkpoint = context.add_checkpoint(
        name="my_checkpoint", class_name="SimpleCheckpoint"
    )

    profiler = context.add_profiler(
        "my_profiler", config_version=1.0, rules=profiler_rules
    )

    # expectation_suite_validation_result_meta = ExpectationSuiteValidationResultMeta(
    #     active_batch_definition=BatchDefinition(
    #         batch_identifiers=IDDict({}),
    #         data_asset_name="my_data_asset",
    #         data_connector_name="my_data_connector",
    #         datasource_name="my_datasource",
    #     ),
    #     batch_markers=BatchMarkers(ge_load_time="20220922T180039.991359Z"),
    #     batch_spec=BatchSpec(path="/some/path"),
    #     checkpoint_name="my_checkpoint",
    #     expectation_suite_name="my_suite",
    #     great_expectations_version="some_version",
    #     run_id=RunIdentifier(run_name="my_run", run_time=None),
    #     validation_time="20220922T180040.002102Z",
    # )

    expectation_suite_validation_result = ExpectationSuiteValidationResult(
        success=True,
        # meta=expectation_suite_validation_result_meta,
    )
    context.validations_store.set(
        key=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier("my_suite"),
            run_id=RunIdentifier(run_name="my_run", run_time=None),
            batch_identifier="my_batch_identifier",
        ),
        value=expectation_suite_validation_result,
    )

    config_bundle = ConfigurationBundle(context)

    assert not config_bundle.is_usage_statistics_key_set()

    # Spot check items in variables
    assert (
        config_bundle._data_context_variables.checkpoint_store_name
        == "checkpoint_store"
    )

    assert config_bundle._expectation_suites == [expectation_suite]

    # TODO: Add these assets:
    # assert config_bundle._checkpoints == [checkpoint.config] # Why does this not work, contents are identical?
    # assert config_bundle._profilers[0] == profiler.config
    assert config_bundle._validation_results == [expectation_suite_validation_result]


@pytest.mark.integration
def test_configuration_bundle_serialization(in_memory_runtime_context: BaseDataContext):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    ConfigurationBundle serialization. We can replace this test with unit tests.
    """

    raise NotImplementedError
