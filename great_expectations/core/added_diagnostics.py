from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass

from great_expectations.compatibility.typing_extensions import override
from great_expectations.exceptions.exceptions import (
    CheckpointNotAddedError,
    CheckpointRelatedResourcesNotAddedError,
    ResourceNotAddedError,
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesNotAddedError,
)


@dataclass
class AddedDiagnostics:
    errors: list[ResourceNotAddedError]

    @property
    def added(self) -> bool:
        return len(self.errors) == 0

    def update(self, *diagnostics: AddedDiagnostics) -> None:
        for diagnostic in diagnostics:
            self.errors = diagnostic.errors + self.errors

    @abstractmethod
    def raise_for_error(self) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def parent_added(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def children_added(self) -> bool:
        raise NotImplementedError


class BatchDefinitionAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.added:
            raise self.errors[0]  # Leaf node so only one error


class ExpectationSuiteAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.added:
            raise self.errors[0]  # Leaf node so only one error


class ValidationDefinitionAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.added:
            raise ValidationDefinitionRelatedResourcesNotAddedError(errors=self.errors)

    @override
    @property
    def parent_added(self) -> bool:
        return not any(isinstance(err, ValidationDefinitionNotAddedError) for err in self.errors)

    @override
    @property
    def children_added(self) -> bool:
        for err in self.errors:
            if isinstance(err, (ExpectationSuiteAddedDiagnostics, BatchDefinitionAddedDiagnostics)):
                return False
        return True


class CheckpointAddedDiagnostics(AddedDiagnostics):
    @override
    def raise_for_error(self) -> None:
        if not self.added:
            raise CheckpointRelatedResourcesNotAddedError(errors=self.errors)

    @override
    @property
    def parent_added(self) -> bool:
        return not any(isinstance(err, CheckpointNotAddedError) for err in self.errors)

    @override
    @property
    def children_added(self) -> bool:
        return not any(isinstance(err, (ValidationDefinitionNotAddedError)) for err in self.errors)
