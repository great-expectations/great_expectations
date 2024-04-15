from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.v1_checkpoint import Checkpoint
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.factory.factory import Factory
from great_expectations.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.core.data_context_key import StringKey
    from great_expectations.data_context.store.checkpoint_store import (
        V1CheckpointStore as CheckpointStore,
    )
    from great_expectations.data_context.types.resource_identifiers import GXCloudIdentifier


# TODO: Add analytics as needed
class CheckpointFactory(Factory[Checkpoint]):
    def __init__(self, store: CheckpointStore):
        self._store = store

    @public_api
    @override
    def add(self, checkpoint: Checkpoint) -> Checkpoint:
        """Add a Checkpoint to the collection.

        Parameters:
            checkpoint: Checkpoint to add

        Raises:
            DataContextError if Checkpoint already exists
        """
        key = self._store.get_key(name=checkpoint.name, id=None)
        if self._store.has_key(key=key):
            raise DataContextError(  # noqa: TRY003
                f"Cannot add Checkpoint with name {checkpoint.name} because it already exists."
            )

        self._store.add(key=key, value=checkpoint)

        # TODO: Add id adding logic to CheckpointStore to prevent round trip
        return self._get(key=key)

    @public_api
    @override
    def delete(self, checkpoint: Checkpoint) -> Checkpoint:
        """Delete a Checkpoint from the collection.

        Parameters:
            checkpoint: Checkpoint to delete

        Raises:
            DataContextError if Checkpoint doesn't exist
        """
        key = self._store.get_key(name=checkpoint.name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(  # noqa: TRY003
                f"Cannot delete Checkpoint with name {checkpoint.name} because it cannot be found."
            )

        self._store.remove_key(key=key)
        return checkpoint

    @public_api
    @override
    def get(self, name: str) -> Checkpoint:
        """Get a Checkpoint from the collection by name.

        Parameters:
            name: Name of Checkpoint to get

        Raises:
            DataContextError when Checkpoint is not found.
        """
        key = self._store.get_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"Checkpoint with name {name} was not found.")  # noqa: TRY003

        return self._get(key=key)

    @public_api
    @override
    def all(self) -> Iterable[Checkpoint]:
        """Get all Checkpoints."""
        return self._store.get_all()

    def _get(self, key: GXCloudIdentifier | StringKey) -> Checkpoint:
        checkpoint = self._store.get(key=key)
        if not isinstance(checkpoint, Checkpoint):
            raise ValueError(f"Object with key {key} was found, but it is not a Checkpoint.")  # noqa: TRY003, TRY004

        return checkpoint
