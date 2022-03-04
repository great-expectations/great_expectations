from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.types import SerializableDictDot


class ProfilerResult(SerializableDictDot):
    def __init__(
        self,
        expectation_suite: ExpectationSuite,
        effective_profiler: "RuleBasedProfiler",
    ) -> None:
        self._expectation_suite = expectation_suite
        self._effective_profiler = effective_profiler

    @property
    def expectation_suite(self) -> ExpectationSuite:
        return self._expectation_suite

    @property
    def effective_profiler(self) -> "RuleBasedProfiler":
        return self._effective_profiler
