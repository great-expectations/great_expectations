from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.factory.factory import Factory
from great_expectations.core.validation_config import ValidationConfig

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
        raise NotImplementedError

    @public_api
    @override
    def delete(self, validation: ValidationConfig) -> ValidationConfig:
        """Delete a ValidationConfig from the collection.

        Parameters:
            validation: ValidationConfig to delete

        Raises:
            DataContextError if ValidationConfig doesn't exist
        """
        raise NotImplementedError

    @public_api
    @override
    def get(self, name: str) -> ValidationConfig:
        """Get a ValidationConfig from the collection by name.

        Parameters:
            name: Name of ValidationConfig to get

        Raises:
            DataContextError when ValidationConfig is not found.
        """
        raise NotImplementedError
