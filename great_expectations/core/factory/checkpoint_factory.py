from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.factory.factory import Factory
from great_expectations.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.store import CheckpointStore


# TODO: Add analytics as needed
class CheckpointFactory(Factory[Checkpoint]):
    def __init__(self, store: CheckpointStore):
        self._store = store

    @public_api
    def add(self, checkpoint: Checkpoint) -> Checkpoint:
        """Add a Checkpoint to the collection.

        Parameters:
            checkpoint: Checkpoint to add

        Raises:
            DataContextError if Checkpoint already exists
        """
        checkpoint = self._store.add_checkpoint(checkpoint=checkpoint)

        # submit_event(
        #     event=ExpectationSuiteCreatedEvent(
        #         expectation_suite_id=suite.ge_cloud_id,
        #     )
        # )

        return checkpoint

    @public_api
    def delete(self, checkpoint: Checkpoint) -> Checkpoint:
        """Delete a Checkpoint from the collection.

        Parameters:
            checkpoint: Checkpoint to delete

        Raises:
            DataContextError if Checkpoint doesn't exist
        """
        key = self._store._determine_key(
            name=checkpoint.name, id=checkpoint.ge_cloud_id
        )
        if not self._store.has_key(key=key):
            raise DataContextError(
                f"Cannot delete Checkpoint with name {checkpoint.name} because it cannot be found."
            )
        self._store.remove_key(key=key)

        # submit_event(
        #     event=ExpectationSuiteDeletedEvent(
        #         expectation_suite_id=suite.ge_cloud_id,
        #     )
        # )

        return checkpoint

    @public_api
    def get(self, name: str) -> Checkpoint:
        """Get a Checkpoint from the collection by name.

        Parameters:
            name: Name of Checkpoint to get

        Raises:
            DataContextError when Checkpoint is not found.
        """
        key = self._store._determine_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"Checkpoint with name {name} was not found.")
        checkpoint_dict = self._store.get(key=key)
        return Checkpoint(**checkpoint_dict)
