from great_expectations.exceptions import GreatExpectationsError


class ProfilerError(GreatExpectationsError):
    pass


class ProfilerConfigurationError(ProfilerError):
    """A configuration error for a profiler."""

    pass


class ProfilerExecutionError(ProfilerError):
    """A runtime error for a profiler."""

    pass
