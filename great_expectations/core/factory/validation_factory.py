from __future__ import annotations

from typing import TYPE_CHECKING, cast

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.factory.factory import Factory
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.exceptions.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.store.validation_config_store import (
        ValidationConfigStore,
    )


# TODO: Add analytics as needed
class ValidationFactory(Factory[ValidationConfig]):
    def __init__(self, store: ValidationConfigStore) -> None:
        self._store = store

    @public_api
    @override
    def add(self, validation: ValidationConfig) -> ValidationConfig:
        """Add a ValidationConfig to the collection.

        Parameters:
            validation: ValidationConfig to add

        Raises:
            DataContextError if ValidationConfig already exists
        """
        key = self._store.get_key(name=validation.name, id=None)
        if self._store.has_key(key=key):
            raise DataContextError(
                f"Cannot add ValidationConfig with name {validation.name} because it already exists."  # noqa: E501
            )
        self._store.add(key=key, value=validation)

        return validation

    @public_api
    @override
    def delete(self, validation: ValidationConfig) -> ValidationConfig:
        """Delete a ValidationConfig from the collection.

        Parameters:
            validation: ValidationConfig to delete

        Raises:
            DataContextError if ValidationConfig doesn't exist
        """
        key = self._store.get_key(name=validation.name, id=validation.id)
        if not self._store.has_key(key=key):
            raise DataContextError(
                f"Cannot delete ValidationConfig with name {validation.name} because it cannot be found."  # noqa: E501
            )
        self._store.remove_key(key=key)

        return validation

    @public_api
    @override
    def get(self, name: str) -> ValidationConfig:
        """Get a ValidationConfig from the collection by name.

        Parameters:
            name: Name of ValidationConfig to get

        Raises:
            DataContextError when ValidationConfig is not found.
        """
        key = self._store.get_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"ValidationConfig with name {name} was not found.")

        return cast(ValidationConfig, self._store.get(key=key))
