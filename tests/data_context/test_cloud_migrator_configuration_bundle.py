"""TODO: Add docstring"""
from dataclasses import dataclass
from typing import List, Optional

import pytest
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,
    EphemeralDataContextVariables,
)

from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    DataContextConfig,
    CheckpointConfig,
)

from great_expectations.checkpoint import Checkpoint
from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    RunIdentifier,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.cloud_migrator import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)


class StubUsageStats:
    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return AnonymizedUsageStatisticsConfig(enabled=True)


class StubCheckpointStore:
    def get_checkpoint(self, name: str, ge_cloud_id: Optional[str]) -> CheckpointConfig:
        return CheckpointConfig(name=name, class_name="Checkpoint")


class StubValidationsStore:
    def list_keys(self):
        # Note: Key just has to return an iterable here
        return ["some_key"]

    def get(self, key):
        # Note: Key is unused
        return ExpectationSuiteValidationResult(
            success=True,
        )


class StubBaseDataContext:
    """Stub for testing ConfigurationBundle."""

    @property
    def _data_context_variables(self) -> StubUsageStats:
        return StubUsageStats()

    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return self.variables.anonymous_usage_statistics

    @property
    def variables(self) -> DataContextVariables:
        config = DataContextConfig(
            anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=True)
        )
        return EphemeralDataContextVariables(config=config)

    @property
    def checkpoint_store(self) -> StubCheckpointStore:
        return StubCheckpointStore()

    @property
    def validations_store(self) -> StubValidationsStore:
        return StubValidationsStore()

    def list_expectation_suite_names(self) -> List[str]:
        return ["my_suite"]

    def get_expectation_suite(self, name: str) -> ExpectationSuite:
        return ExpectationSuite(expectation_suite_name=name)

    def list_checkpoints(self) -> List[str]:
        return ["my_checkpoint"]

    def list_profilers(self) -> List[str]:
        return ["my_profiler"]

    def get_profiler(self, name: str) -> RuleBasedProfiler:
        return RuleBasedProfiler(name, config_version=1.0, rules={})


@pytest.fixture
def stub_base_data_context() -> StubBaseDataContext:
    return StubBaseDataContext()


@dataclass
class DataContextWithSavedItems:
    context: BaseDataContext
    expectation_suite: ExpectationSuite
    checkpoint: Checkpoint
    profiler: RuleBasedProfiler
    validation_result: ExpectationSuiteValidationResult


@pytest.fixture
def in_memory_runtime_context_with_configs_in_stores(
    in_memory_runtime_context: BaseDataContext, profiler_rules: dict
) -> DataContextWithSavedItems:

    context: BaseDataContext = in_memory_runtime_context

    # Add items to the context:

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
    )
    context.validations_store.set(
        key=ValidationResultIdentifier(
            expectation_suite_identifier=ExpectationSuiteIdentifier("my_suite"),
            run_id=RunIdentifier(run_name="my_run", run_time=None),
            batch_identifier="my_batch_identifier",
        ),
        value=expectation_suite_validation_result,
    )

    return DataContextWithSavedItems(
        context=context,
        expectation_suite=expectation_suite,
        checkpoint=checkpoint,
        profiler=profiler,
        validation_result=expectation_suite_validation_result,
    )


def dict_equal(dict_1: dict, dict_2: dict) -> bool:
    return convert_to_json_serializable(dict_1) == convert_to_json_serializable(dict_2)


def list_of_dicts_equal(list_1: list, list_2: list) -> bool:

    if len(list_1) != len(list_2):
        return False
    for i in range(len(list_1)):
        if not dict_equal(list_1[i], list_2[i]):
            return False

    return True


@pytest.mark.integration
def test_configuration_bundle_init(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out that
    ConfigurationBundle successfully bundles. We can replace this test with
    unit tests.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    config_bundle = ConfigurationBundle(context)

    assert not config_bundle.is_usage_stats_enabled()

    # Spot check items in variables
    assert (
        config_bundle._data_context_variables.checkpoint_store_name
        == "checkpoint_store"
    )
    assert config_bundle._data_context_variables.progress_bars is None

    assert config_bundle._expectation_suites == [
        in_memory_runtime_context_with_configs_in_stores.expectation_suite
    ]

    assert list_of_dicts_equal(
        config_bundle._checkpoints,
        [in_memory_runtime_context_with_configs_in_stores.checkpoint.config],
    )

    roundtripped_profiler_config = ruleBasedProfilerConfigSchema.load(
        ruleBasedProfilerConfigSchema.dump(
            in_memory_runtime_context_with_configs_in_stores.profiler.config
        )
    )
    assert list_of_dicts_equal(config_bundle._profilers, [roundtripped_profiler_config])

    assert config_bundle._validation_results == [
        in_memory_runtime_context_with_configs_in_stores.validation_result
    ]


@pytest.mark.integration
def test_configuration_bundle_serialization(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
    serialized_configuration_bundle: dict,
):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    ConfigurationBundle serialization. We can replace this test with unit tests.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    config_bundle = ConfigurationBundle(context)

    serializer = ConfigurationBundleJsonSerializer(schema=ConfigurationBundleSchema())

    serialized_bundle: dict = serializer.serialize(config_bundle)

    expected_serialized_bundle = serialized_configuration_bundle

    # Remove meta before comparing since it contains the GX version
    serialized_bundle["expectation_suites"][0].pop("meta", None)
    expected_serialized_bundle["expectation_suites"][0].pop("meta", None)

    assert serialized_bundle == expected_serialized_bundle


def test_is_usage_statistics_key_set_if_key_not_present():
    """What does this test and why?

    The ConfigurationBundle should handle a context that has not set the config for
     anonymous_usage_statistics.
    """
    # TODO: Implementation
    pass


@pytest.mark.integration
def test_anonymous_usage_statistics_removed_during_serialization_integration(
    in_memory_runtime_context_with_configs_in_stores: DataContextWithSavedItems,
):
    """What does this test and why?

    When serializing a ConfigurationBundle we need to remove the
    anonymous_usage_statistics key.

    This is currently an integration test using a real Data Context, it can
    be converted to a unit test.
    """

    context: BaseDataContext = in_memory_runtime_context_with_configs_in_stores.context

    assert context.anonymous_usage_statistics is not None

    config_bundle = ConfigurationBundle(context)

    serializer = ConfigurationBundleJsonSerializer(schema=ConfigurationBundleSchema())

    serialized_bundle: dict = serializer.serialize(config_bundle)

    assert (
        serialized_bundle["data_context_variables"].get("anonymous_usage_statistics")
        is None
    )


@pytest.mark.unit
def test_anonymous_usage_statistics_removed_during_serialization(
    stub_base_data_context: StubBaseDataContext
):
    """What does this test and why?
    When serializing a ConfigurationBundle we need to remove the
    anonymous_usage_statistics key.
    This is currently an integration test using a real Data Context, it can
    be converted to a unit test.
    """

    context: StubBaseDataContext = stub_base_data_context

    assert context.anonymous_usage_statistics is not None

    config_bundle = ConfigurationBundle(context)

    serializer = ConfigurationBundleJsonSerializer(schema=ConfigurationBundleSchema())

    serialized_bundle: dict = serializer.serialize(config_bundle)

    assert (
        serialized_bundle["data_context_variables"].get("anonymous_usage_statistics")
        is None
    )
