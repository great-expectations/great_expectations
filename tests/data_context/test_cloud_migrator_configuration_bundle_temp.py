"""TODO: Note these changes should go into test_cloud_migrator_configuration_bundle.py after PR 6068 is merged."""
from dataclasses import dataclass
from typing import List, Optional, cast

from marshmallow import Schema, fields, post_dump

from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteSchema,
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultSchema,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import AbstractDataContext, BaseDataContext
from great_expectations.data_context.data_context_variables import DataContextVariables, EphemeralDataContextVariables
from great_expectations.data_context.store.ge_cloud_store_backend import AnyPayload
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    CheckpointConfigSchema,
    DataContextConfigSchema,
    GeCloudConfig, DataContextConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.rule_based_profiler.config import (
    RuleBasedProfilerConfig,
    RuleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)

#----------------DELETE ABOVE THIS LINE-----------------------------------------

from typing import List, Optional

import pytest

from great_expectations.data_context.cloud_migrator import ConfigurationBundle
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    CheckpointConfig,
)

from great_expectations.core import (
    ExpectationSuite, ExpectationSuiteValidationResult,
)

from great_expectations.rule_based_profiler import RuleBasedProfiler


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
        config = DataContextConfig(anonymous_usage_statistics=AnonymizedUsageStatisticsConfig(enabled=True))
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


#----------------DELETE BELOW THIS LINE-----------------------------------------

def test_stub_base_data_context():

    context = StubBaseDataContext()
    assert context._data_context_variables.anonymous_usage_statistics.enabled

    assert [
        context.get_expectation_suite(name)
        for name in context.list_expectation_suite_names()
    ] == [ExpectationSuite(expectation_suite_name="my_suite")]

    # assert [
    #         context.checkpoint_store.get_checkpoint(
    #             name=checkpoint_name, ge_cloud_id=None
    #         )
    #         for checkpoint_name in context.list_checkpoints()
    #     ] == [CheckpointConfig(name="my_checkpoint", class_name="Checkpoint")]

class ConfigurationBundle:
    def __init__(self, context: BaseDataContext) -> None:

        self._context = context

        self._data_context_variables: DataContextVariables = context.variables

        self._expectation_suites: List[
            ExpectationSuite
        ] = self._get_all_expectation_suites()
        self._checkpoints: List[CheckpointConfig] = self._get_all_checkpoints()
        self._profilers: List[RuleBasedProfilerConfig] = self._get_all_profilers()
        self._validation_results: List[
            ExpectationSuiteValidationResult
        ] = self._get_all_validation_results()

    def is_usage_stats_enabled(self) -> bool:
        """Determine whether usage stats are enabled.
        Also returns false if there are no usage stats settings provided.
        Returns: Boolean of whether the usage statistics are enabled.
        """
        if self._data_context_variables.anonymous_usage_statistics:
            return self._data_context_variables.anonymous_usage_statistics.enabled
        else:
            return False

    def _get_all_expectation_suites(self) -> List[ExpectationSuite]:
        return [
            self._context.get_expectation_suite(name)
            for name in self._context.list_expectation_suite_names()
        ]

    def _get_all_checkpoints(self) -> List[CheckpointConfig]:
        return [self._context.checkpoint_store.get_checkpoint(name=checkpoint_name, ge_cloud_id=None) for checkpoint_name in self._context.list_checkpoints()]  # type: ignore[arg-type]

    def _get_all_profilers(self) -> List[RuleBasedProfilerConfig]:
        def round_trip_profiler_config(
            profiler_config: RuleBasedProfilerConfig,
        ) -> RuleBasedProfilerConfig:
            return ruleBasedProfilerConfigSchema.load(
                ruleBasedProfilerConfigSchema.dump(profiler_config)
            )

        return [
            round_trip_profiler_config(self._context.get_profiler(name).config)
            for name in self._context.list_profilers()
        ]

    def _get_all_validation_results(
        self,
    ) -> List[ExpectationSuiteValidationResult]:
        return [
            cast(
                ExpectationSuiteValidationResult,
                self._context.validations_store.get(key),
            )
            for key in self._context.validations_store.list_keys()
        ]


class ConfigurationBundleJsonSerializer:
    def __init__(self, schema: Schema) -> None:
        """
        Args:
            schema: Marshmallow schema defining raw serialized version of object.
        """
        self.schema = schema

    def serialize(self, obj: ConfigurationBundle) -> dict:
        """Serialize config to json dict.
        Args:
            obj: AbstractConfig object to serialize.
        Returns:
            Representation of object as a dict suitable for serializing to json.
        """
        config: dict = self.schema.dump(obj)

        json_serializable_dict: dict = convert_to_json_serializable(data=config)

        return json_serializable_dict


class ConfigurationBundleSchema(Schema):
    """Marshmallow Schema for the Configuration Bundle."""

    _data_context_variables = fields.Nested(
        DataContextConfigSchema, allow_none=False, data_key="data_context_variables"
    )
    _expectation_suites = fields.List(
        fields.Nested(ExpectationSuiteSchema, allow_none=True, required=True),
        required=True,
        data_key="expectation_suites",
    )
    _checkpoints = fields.List(
        fields.Nested(CheckpointConfigSchema, allow_none=True, required=True),
        required=True,
        data_key="checkpoints",
    )
    _profilers = fields.List(
        fields.Nested(RuleBasedProfilerConfigSchema, allow_none=True, required=True),
        required=True,
        data_key="profilers",
    )
    _validation_results = fields.List(
        fields.Nested(
            ExpectationSuiteValidationResultSchema, allow_none=True, required=True
        ),
        required=True,
        data_key="validation_results",
    )

    @post_dump
    def clean_up(self, data, **kwargs) -> dict:
        data_context_variables = data.get("data_context_variables", {})
        data_context_variables.pop("anonymous_usage_statistics", None)
        return data


#----------------DELETE ABOVE THIS LINE-----------------------------------------

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