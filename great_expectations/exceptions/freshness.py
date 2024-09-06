"""
TODO: Module level docstring
"""

from great_expectations.exceptions.exceptions import (
    DataContextError,
    GreatExpectationsAggregateError,
)


class ResourcesNotAddedError(GreatExpectationsAggregateError):
    pass


class CheckpointRelatedResourcesNotAddedError(ResourcesNotAddedError):
    pass


class ValidationDefinitionRelatedResourcesNotAddedError(ResourcesNotAddedError):
    pass


class ResourceNotAddedError(DataContextError):
    pass


class ExpectationSuiteNotAddedError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ExpectationSuite '{name}' must be added to the DataContext before it can be updated. "
            "Please call `context.suites.add(<SUITE_OBJECT>)`, "
            "then try your action again."
        )


class ExpectationSuiteNotFreshError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ExpectationSuite '{name}' has changed since it has last been saved. "
            "Please update with `<SUITE_OBJECT>.save()`, then try your action again."
        )


class BatchDefinitionNotAddedError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"BatchDefinition '{name}' must be added to the DataContext before it can be updated. "
            "Please update using the parent asset or data source, then try your action again."
        )


class BatchDefinitionNotFreshError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"BatchDefinition '{name}' has changed since it has last been saved. "
            "Please update using the parent asset or data source, then try your action again."
        )


class ValidationDefinitionNotAddedError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ValidationDefinition '{name}' must be added to the DataContext before it can be updated. "  # noqa: E501
            "Please call `context.validation_definitions.add(<VALIDATION_DEFINITION_OBJECT>)`, "
            "then try your action again."
        )


class ValidationDefinitionNotFreshError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"ValidationDefinition '{name}' has changed since it has last been saved. "
            "Please update with `<VALIDATION_DEFINITION_OBJECT>.save()`, then try your action again."  # noqa: E501
        )


class CheckpointNotAddedError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Checkpoint '{name}' must be added to the DataContext before it can be updated. "
            "Please call `context.checkpoints.add(<CHECKPOINT_OBJECT>)`, "
            "then try your action again."
        )


class CheckpointNotFreshError(ResourceNotAddedError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"Checkpoint '{name}' has changed since it has last been saved. "
            "Please update with `<CHECKPOINT_OBJECT>.save()`, then try your action again."
        )
