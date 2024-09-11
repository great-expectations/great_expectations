"""
This module contains exceptions related to resource freshness.

A resource is considered "fresh" if it has been added to the DataContext and is up-to-date with
its persisted equivalent.

Please see the `FreshnessDiagnostics` class for how these exceptions are aggregated and raised.


Hierarchy:
    GreatExpectationsAggregateError
        ResourceFreshnessAggregateError
            CheckpointRelatedResourcesFreshnessError
            ValidationDefinitionRelatedResourcesFreshnessError

    ResourceFreshnessError
        ExpectationSuiteNotAddedError
        ExpectationSuiteNotFreshError
        BatchDefinitionNotAddedError
        BatchDefinitionNotFreshError
        ValidationDefinitionNotAddedError
        ValidationDefinitionNotFreshError
        CheckpointNotAddedError
        CheckpointNotFreshError
"""

from great_expectations.exceptions.exceptions import (
    DataContextError,
    GreatExpectationsAggregateError,
)


class ResourceFreshnessAggregateError(GreatExpectationsAggregateError):
    pass


class CheckpointRelatedResourcesFreshnessError(ResourceFreshnessAggregateError):
    pass


class ValidationDefinitionRelatedResourcesFreshnessError(ResourceFreshnessAggregateError):
    pass


class ResourceFreshnessError(DataContextError):
    pass


class ExpectationSuiteNotAddedError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ExpectationSuite '{name}' must be added to the DataContext before it can be updated. "
            "Please call `context.suites.add(<SUITE_OBJECT>)`, "
            "then try your action again."
        )


class ExpectationSuiteNotFreshError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ExpectationSuite '{name}' has changed since it has last been saved. "
            "Please update with `<SUITE_OBJECT>.save()`, then try your action again."
        )


class BatchDefinitionNotAddedError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"BatchDefinition '{name}' must be added to the DataContext before it can be updated. "
            "Please update using the parent asset or data source, then try your action again."
        )


class BatchDefinitionNotFreshError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"BatchDefinition '{name}' has changed since it has last been saved. "
            "Please update using the parent asset or data source, then try your action again."
        )


class ValidationDefinitionNotAddedError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ValidationDefinition '{name}' must be added to the DataContext before it can be updated. "  # noqa: E501
            "Please call `context.validation_definitions.add(<VALIDATION_DEFINITION_OBJECT>)`, "
            "then try your action again."
        )


class ValidationDefinitionNotFreshError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ValidationDefinition '{name}' has changed since it has last been saved. "
            "Please update with `<VALIDATION_DEFINITION_OBJECT>.save()`, then try your action again."  # noqa: E501
        )


class CheckpointNotAddedError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Checkpoint '{name}' must be added to the DataContext before it can be updated. "
            "Please call `context.checkpoints.add(<CHECKPOINT_OBJECT>)`, "
            "then try your action again."
        )


class CheckpointNotFreshError(ResourceFreshnessError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Checkpoint '{name}' has changed since it has last been saved. "
            "Please update with `<CHECKPOINT_OBJECT>.save()`, then try your action again."
        )
