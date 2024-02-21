from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.factory.factory import Factory

if TYPE_CHECKING:
    from great_expectations.core.validation import Validation


# TODO: Add analytics as needed
class ValidationFactory(Factory[Validation]):
    def __init__(self, store) -> None:
        # TODO: Update type hints when new ValidationStore is implemented
        self._store = store

    @public_api
    @override
    def add(self, validation: Validation) -> Validation:
        """Add a Validation to the collection.

        Parameters:
            validation: Validation to add

        Raises:
            DataContextError if Validation already exists
        """
        raise NotImplementedError

    @public_api
    @override
    def delete(self, validation: Validation) -> Validation:
        """Delete a Validation from the collection.

        Parameters:
            validation: Validation to delete

        Raises:
            DataContextError if Validation doesn't exist
        """
        raise NotImplementedError

    @public_api
    @override
    def get(self, name: str) -> Validation:
        """Get a Validation from the collection by name.

        Parameters:
            name: Name of Validation to get

        Raises:
            DataContextError when Validation is not found.
        """
        raise NotImplementedError
