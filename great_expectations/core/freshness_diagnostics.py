from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Tuple, Type

from great_expectations.compatibility.typing_extensions import override
from great_expectations.exceptions.exceptions import (
    BatchDefinitionNotAddedError,
    CheckpointNotAddedError,
    CheckpointRelatedResourcesNotFreshError,
    ExpectationSuiteNotAddedError,
    ResourceNotAddedError,
    ResourcesNotFreshError,
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesNotFreshError,
)


@dataclass
class FreshnessDiagnostics:
    """
    Wrapper around a list of errors; used to determine if a resource has been added successfully
    and is "fresh" or up-to-date with its persisted equivalent.

    Note that some resources may have dependencies on other resources - in order to be considered
    "fresh", the root resource and all of its dependencies must be "fresh".
    For example, a Checkpoint may have dependencies on ValidationDefinitions, which may have
    dependencies on ExpectationSuites and BatchDefinitions.

    GX requires that all resources are persisted successfully before they can be used to prevent
    unexpected behavior.
    """

    raise_for_error_class: ClassVar[Type[ResourcesNotFreshError]] = ResourcesNotFreshError
    errors: list[ResourceNotAddedError]

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def raise_for_error(self) -> None:
        """
        Conditionally raises an error if the resource has not been added successfully;
        should prescribe the correct action(s) to take.
        """
        if not self.success:
            raise self.raise_for_error_class(errors=self.errors)


@dataclass
class BatchDefinitionFreshnessDiagnostics(FreshnessDiagnostics):
    pass


@dataclass
class ExpectationSuiteFreshnessDiagnostics(FreshnessDiagnostics):
    pass


@dataclass
class _ParentFreshnessDiagnostics(FreshnessDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]]
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]]

    def update_with_children(self, *children_diagnostics: FreshnessDiagnostics) -> None:
        for diagnostics in children_diagnostics:
            # Child errors should be prepended to parent errors so diagnostics are in order
            self.errors = diagnostics.errors + self.errors

    @property
    def parent_added(self) -> bool:
        return all(not isinstance(err, self.parent_error_class) for err in self.errors)

    @property
    def children_added(self) -> bool:
        return all(not isinstance(err, self.children_error_classes) for err in self.errors)

    @override
    def raise_for_error(self) -> None:
        if not self.success:
            raise self.raise_for_error_class(errors=self.errors)


@dataclass
class ValidationDefinitionFreshnessDiagnostics(_ParentFreshnessDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = ValidationDefinitionNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ExpectationSuiteNotAddedError,
        BatchDefinitionNotAddedError,
    )
    raise_for_error_class: ClassVar[Type[ResourcesNotFreshError]] = (
        ValidationDefinitionRelatedResourcesNotFreshError
    )


@dataclass
class CheckpointFreshnessDiagnostics(_ParentFreshnessDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = CheckpointNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ValidationDefinitionNotAddedError,
    )
    raise_for_error_class: ClassVar[Type[ResourcesNotFreshError]] = (
        CheckpointRelatedResourcesNotFreshError
    )
