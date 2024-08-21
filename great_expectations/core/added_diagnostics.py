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
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesNotAddedError,
)


@dataclass
class AddedDiagnostics:
    errors: list[ResourceNotAddedError]

    @property
    def dependencies_added(self) -> bool:
        return len(self.errors) == 0

    def update(self, *diagnostics: AddedDiagnostics) -> None:
        for diagnostic in diagnostics:
            self.errors = diagnostic.errors + self.errors

    @abstractmethod
    def raise_for_error(self) -> None:
        raise NotImplementedError


@dataclass
class _ChildAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.dependencies_added:
            raise self.errors[0]  # Child node so only one error


@dataclass
class BatchDefinitionAddedDiagnostics(_ChildAddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.dependencies_added:
            raise self.errors[0]  # Child node so only one error


@dataclass
class ExpectationSuiteAddedDiagnostics(_ChildAddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.dependencies_added:
            raise self.errors[0]  # Leaf node so only one error


@dataclass
class _ParentAddedDiagnostics(AddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]]
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]]
    raise_for_error_type: ClassVar[Type[ResourceNotAddedError]]

    @property
    def parent_added(self) -> bool:
        raise not any(isinstance(err, self.parent_error_class) for err in self.errors)

    @property
    def children_added(self) -> bool:
        raise not any(isinstance(err, self.children_error_classes) for err in self.errors)

    @override
    def raise_for_error(self) -> None:
        if not self.dependencies_added:
            raise self.raise_for_error_type(errors=self.errors)


@dataclass
class ValidationDefinitionAddedDiagnostics(_ParentAddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = ValidationDefinitionNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ExpectationSuiteNotAddedError,
        BatchDefinitionNotAddedError,
    )
    raise_for_error_type: ClassVar[Type[ResourceNotAddedError]] = (
        ValidationDefinitionRelatedResourcesNotAddedError
    )


@dataclass
class CheckpointAddedDiagnostics(_ParentAddedDiagnostics):
    parent_error_class: ClassVar[Type[ResourceNotAddedError]] = CheckpointNotAddedError
    children_error_classes: ClassVar[Tuple[Type[ResourceNotAddedError], ...]] = (
        ValidationDefinitionNotAddedError,
    )
    raise_for_error_type: ClassVar[Type[ResourceNotAddedError]] = (
        CheckpointRelatedResourcesNotAddedError
    )
