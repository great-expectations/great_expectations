from great_expectations.data_context.store.configuration_store import ConfigurationStore


class ProfilerStore(ConfigurationStore):
    """
    A ProfilerStore manages Profilers for the DataContext.
    """

    _configuration_class = "RuleBasedProfilerConfig"  # TODO(cdkini): Convert to class upon merging schema PR
