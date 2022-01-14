import random

from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig


class ProfilerStore(ConfigurationStore):
    """
    A ProfilerStore manages Profilers for the DataContext.
    """

    _configuration_class = RuleBasedProfilerConfig

    def serialization_self_check(self, pretty_print: bool) -> None:
        test_profiler_name = f"profiler_{''.join([random.choice(list('0123456789ABCDEF')) for _ in range(20)])}"
        test_checkpoint_configuration = RuleBasedProfilerConfig(
            **{"name": test_checkpoint_name}
        )
