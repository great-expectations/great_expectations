from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, cast

from great_expectations._docs_decorators import public_api
from great_expectations.analytics.client import submit as submit_event
from great_expectations.analytics.events import (
    ValidationDefinitionCreatedEvent,
    ValidationDefinitionDeletedEvent,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.factory.factory import Factory
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.exceptions.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.store.validation_definition_store import (
        ValidationDefinitionStore,
    )


@public_api
class ValidationDefinitionFactory(Factory[ValidationDefinition]):
    def __init__(self, store: ValidationDefinitionStore) -> None:
        self._store = store

    @public_api
    @override
    def add(self, validation: ValidationDefinition) -> ValidationDefinition:
        """Add a ValidationDefinition to the collection.

        Parameters:
            validation: ValidationDefinition to add

        Raises:
            DataContextError if ValidationDefinition already exists
        """
        key = self._store.get_key(name=validation.name, id=None)
        if self._store.has_key(key=key):
            raise DataContextError(  # noqa: TRY003
                f"Cannot add ValidationDefinition with name {validation.name} because it already exists."  # noqa: E501
            )
        self._store.add(key=key, value=validation)

        submit_event(
            event=ValidationDefinitionCreatedEvent(
                validation_definition_id=validation.id,
            )
        )

        return validation

    @public_api
    @override
    def delete(self, name: str) -> None:
        """Delete a ValidationDefinition from the collection.

        Parameters:
            name: The name of the ValidationDefinition to delete

        Raises:
            DataContextError if ValidationDefinition doesn't exist
        """
        try:
            validation_definition = self.get(name=name)
        except DataContextError as e:
            raise DataContextError(  # noqa: TRY003
                f"Cannot delete ValidationDefinition with name {name} because it cannot be found."
            ) from e

        key = self._store.get_key(name=validation_definition.name, id=validation_definition.id)
        self._store.remove_key(key=key)

        submit_event(
            event=ValidationDefinitionDeletedEvent(
                validation_definition_id=validation_definition.id,
            )
        )

    @public_api
    @override
    def get(self, name: str) -> ValidationDefinition:
        """Get a ValidationDefinition from the collection by name.

        Parameters:
            name: Name of ValidationDefinition to get

        Raises:
            DataContextError when ValidationDefinition is not found.
        """
        key = self._store.get_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"ValidationDefinition with name {name} was not found.")  # noqa: TRY003

        return cast(ValidationDefinition, self._store.get(key=key))

    @public_api
    @override
    def all(self) -> Iterable[ValidationDefinition]:
        """Get all ValidationDefinitions."""
        return self._store.get_all()
