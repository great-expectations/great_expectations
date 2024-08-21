from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Tuple, Type

from great_expectations.compatibility.typing_extensions import override
from great_expectations.exceptions.exceptions import (
    BatchDefinitionNotAddedError,
    CheckpointNotAddedError,
    CheckpointRelatedResourcesNotAddedError,
    ExpectationSuiteNotAddedError,
    ResourceNotAddedError,
    ResourcesNotAddedError,
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesNotAddedError,
)


@dataclass
class AddedDiagnostics:
    """
    Wrapper around a list of errors; used to determine if a resource has been added successfully.

    Note that some resources may have dependencies on other resources - in order to be considered
    "added", the root resource and all of its dependencies must be added successfully.
    For example, a Checkpoint may have dependencies on ValidationDefinitions, which may have
    dependencies on ExpectationSuites and BatchDefinitions.

    GX requires that all resources are added successfully before they can be used to prevent
    unexpected behavior.
    """

    errors: list[ResourceNotAddedError]

    @property
    def is_added(self) -> bool:
        return len(self.errors) == 0

    @abstractmethod
    def raise_for_error(self) -> None:
        """
        Conditionally raises an error if the resource has not been added successfully;
        should prescribe the correct action(s) to take.
        """
        raise NotImplementedError


@dataclass
class _ChildAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.is_added:
            raise self.errors[0]  # Child node so only one error


@dataclass
class BatchDefinitionAddedDiagnostics(_ChildAddedDiagnostics):
    pass


@dataclass
class ExpectationSuiteAddedDiagnostics(_ChildAddedDiagnostics):
    pass


@dataclass
class _ParentAddedDiagnostics(AddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]]
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]]
    raise_for_error_class: ClassVar[Type[ResourcesNotAddedError]]

    def update_with_children(self, *children_diagnostics: AddedDiagnostics) -> None:
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
        if not self.is_added:
            raise self.raise_for_error_class(errors=self.errors)


@dataclass
class ValidationDefinitionAddedDiagnostics(_ParentAddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = ValidationDefinitionNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ExpectationSuiteNotAddedError,
        BatchDefinitionNotAddedError,
    )
    raise_for_error_class: ClassVar[Type[ResourcesNotAddedError]] = (
        ValidationDefinitionRelatedResourcesNotAddedError
    )


@dataclass
class CheckpointAddedDiagnostics(_ParentAddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = CheckpointNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ValidationDefinitionNotAddedError,
    )
    raise_for_error_class: ClassVar[Type[ResourcesNotAddedError]] = (
        CheckpointRelatedResourcesNotAddedError
    )
