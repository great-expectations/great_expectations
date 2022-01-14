from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig


class ProfilerStore(ConfigurationStore):
    """
    A ProfilerStore manages Profilers for the DataContext.
    """

    _configuration_class = RuleBasedProfilerConfig

    def serialization_self_check(self, pretty_print: bool) -> None:
        pass
