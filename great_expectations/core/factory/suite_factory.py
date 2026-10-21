from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.pydantic import ValidationError as PydanticValidationError
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import ExpectationSuite
from great_expectations.core.factory.factory import Factory
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.store import ExpectationsStore


@public_api
class SuiteFactory(Factory[ExpectationSuite]):
    """
    Responsible for basic CRUD operations on a Data Context's ExpectationSuites.
    """

    def __init__(self, store: ExpectationsStore):
        self._store = store

    @property
    def _include_rendered_content(self) -> bool:
        return project_manager.is_using_cloud()

    @public_api
    @override
    def add(self, suite: ExpectationSuite) -> ExpectationSuite:
        """Add an ExpectationSuite to the collection.

        Args:
            suite: ExpectationSuite to add

        Raises:
            DataContextError: if ExpectationSuite already exists
        """
        key = self._store.get_key(name=suite.name, id=None)
        if self._store.has_key(key=key):
            raise DataContextError(  # noqa: TRY003 # FIXME CoP
                f"Cannot add ExpectationSuite with name {suite.name} because it already exists."
            )
        self._store.add(key=key, value=suite)

        # Re-fetch so the returned object matches what was persisted.
        return self.get(name=suite.name)

    @public_api
    @override
    def delete(self, name: str) -> None:
        """Delete an ExpectationSuite from the collection.

        Args:
            name: The name of the ExpectationSuite to delete

        Raises:
            DataContextError: if ExpectationSuite doesn't exist
        """
        try:
            suite = self.get(name=name)
        except DataContextError as e:
            raise DataContextError(  # noqa: TRY003 # FIXME CoP
                f"Cannot delete ExpectationSuite with name {name} because it cannot be found."
            ) from e

        key = self._store.get_key(name=suite.name, id=suite.id)
        self._store.remove_key(key=key)

    @public_api
    @override
    def get(self, name: str) -> ExpectationSuite:
        """Get an ExpectationSuite from the collection by name.

        Args:
            name: Name of ExpectationSuite to get

        Raises:
            DataContextError: when ExpectationSuite is not found.
        """

        key = self._store.get_key(name=name, id=None)
        if not self._store.has_key(key=key):
            raise DataContextError(f"ExpectationSuite with name {name} was not found.")  # noqa: TRY003 # FIXME CoP
        suite_dict = self._store.get(key=key)
        return self._store.deserialize_suite_dict(suite_dict)

    @public_api
    @override
    def all(self) -> Iterable[ExpectationSuite]:
        """Get all ExpectationSuites."""
        dicts = self._store.get_all()
        # Marshmallow validation was done in the previous get_all() call for
        # suites but we can still die here because pydantic validation happens
        # on the expectations inside the suites here.
        # TODO: deserialization should not live in the factory and should
        # TODO: live in the store like in other domain objects. That will
        # TODO: allow us delete this error handling here.
        deserializable_suites: list[ExpectationSuite] = []
        bad_dicts: list[Any] = []
        for suite_dict in dicts:
            try:
                deserializable_suites.append(self._store.deserialize_suite_dict(suite_dict))
            except PydanticValidationError:
                bad_dicts.append(suite_dict)
            except Exception:
                raise
        return deserializable_suites

    @public_api
    @override
    def add_or_update(self, suite: ExpectationSuite) -> ExpectationSuite:
        """Add or update an ExpectationSuite by name.

        If an ExpectationSuite with the same name exists, overwrite it, otherwise
        create a new ExpectationSuite. On update, Expectations in the Suite which
        match a previously existing Expectation maintain a stable ID, and
        Expectations which have changed receive a new ID.

        Args:
            suite: ExpectationSuite to add or update
        """
        try:
            existing_suite = self.get(name=suite.name)
        except DataContextError:
            return self.add(suite=suite)

        # add IDs to expectations that haven't changed
        existing_expectations = existing_suite.expectations
        for expectation in suite.expectations:
            try:
                index = existing_expectations.index(expectation)
                expectation.id = existing_expectations[index].id
            except ValueError:
                pass  # expectation is new or updated

        suite.id = existing_suite.id
        # Write through this factory's store, not the caller's suite: a suite built by hand
        # belongs to no Data Context, so ``suite.save()`` would resolve to the current one,
        # and a suite owned by another context would write back to that one (#12209). The
        # store stamps the suite with this factory's context once the write succeeds. The steps
        # are those of ``ExpectationSuite.save()``.
        if self._include_rendered_content:
            suite.render()
        key = self._store.get_key(name=suite.name, id=suite.id)
        self._store.update(key=key, value=suite)

        # Re-fetch so the returned object matches what was persisted.
        return self.get(name=suite.name)
