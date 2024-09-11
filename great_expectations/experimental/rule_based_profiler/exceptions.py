from great_expectations.exceptions.exceptions import GreatExpectationsError


class ProfilerError(GreatExpectationsError):
    pass


class ProfilerConfigurationError(ProfilerError):
    """A configuration error for a "RuleBasedProfiler" class."""

    pass


class ProfilerExecutionError(ProfilerError):
    """A runtime error for a "RuleBasedProfiler" class."""

    pass


class ProfilerNotFoundError(ProfilerError):
    pass
