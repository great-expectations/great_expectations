"""TODO: Note these changes should go into test_cloud_migrator_configuration_bundle.py after PR 6068 is merged."""


from typing import List, Optional

from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    CheckpointConfig,
)

from great_expectations.core import (
    ExpectationSuite,
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
        pass

    def get(self, key):
        pass



class StubBaseDataContext:
    """Stub for testing ConfigurationBundle."""

    @property
    def _data_context_variables(self) -> StubUsageStats:
        return StubUsageStats()

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

    def get_profiler(self) -> RuleBasedProfiler:
        return RuleBasedProfiler("my_profiler", config_version=1.0)




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