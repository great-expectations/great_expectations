from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations._docs_decorators import public_api
from great_expectations.analytics.client import submit as submit_event
from great_expectations.analytics.events import (
    ExpectationSuiteCreatedEvent,
    ExpectationSuiteDeletedEvent,
)
from great_expectations.core import ExpectationSuite
from great_expectations.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.store import ExpectationsStore


class SuiteFactory:
    def __init__(self, store: ExpectationsStore, include_rendered_content: bool):
        self._store = store
        self._include_rendered_content = include_rendered_content

    @public_api
    def add(self, suite: ExpectationSuite) -> ExpectationSuite:
        """Add an ExpectationSuite to the collection.

        Parameters:
            suite: ExpectationSuite to add

        Raises:
            DataContextError if ExpectationSuite already exists
        """
        key = self._store.get_key(name=suite.name, id=None)
        if self._store.has_key(key=key):
            raise DataContextError(
                f"Cannot add ExpectationSuite with name {suite.name} because it already exists."
            )
        self._store.add(key=key, value=suite)

        submit_event(
            event=ExpectationSuiteCreatedEvent(
                expectation_suite_id=suite.ge_cloud_id,
            )
        )

        return suite

    @public_api
    def delete(self, suite: ExpectationSuite) -> ExpectationSuite:
        """Delete an ExpectationSuite from the collection.

        Parameters:
            suite: ExpectationSuite to delete

        Raises:
            DataContextError if ExpectationSuite doesn't exist
        """
        key = self._store.get_key(name=suite.name, id=suite.ge_cloud_id)
        if not self._store.has_key(key=key):
            raise DataContextError(
                f"Cannot delete ExpectationSuite with name {suite.name} because it cannot be found."
            )
        self._store.remove_key(key=key)

        submit_event(
            event=ExpectationSuiteDeletedEvent(
                expectation_suite_id=suite.ge_cloud_id,
            )
        )

        return suite

    @public_api
    def get(self, name: str) -> ExpectationSuite:
        """Get an ExpectationSuite from the collection by name.

        Parameters:
            name: Name of ExpectationSuite to get

        Raises:
            DataContextError when ExpectationSuite is not found.
        """

        key = self._store.get_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"ExpectationSuite with name {name} was not found.")
        suite_dict = self._store.get(key=key)
        suite = ExpectationSuite(**suite_dict)
        if self._include_rendered_content:
            suite.render()
        return suite
