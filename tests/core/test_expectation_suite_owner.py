"""An ExpectationSuite belongs to the Data Context whose store built or wrote it (#12209).

A store records its context on every suite it builds or writes, so the suite resolves through
that context whatever context is current later. A suite that was never added, and any copy of one,
belongs to no context and resolves through the current one.
"""

from __future__ import annotations

import copy
import pathlib
import pickle
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite, ExpectationSuiteSchema
from great_expectations.core.freshness_diagnostics import BatchDefinitionFreshnessDiagnostics
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.exceptions import (
    DataContextError,
    DataContextRequiredError,
    ExpectationSuiteNotAddedError,
)
from great_expectations.exceptions.exceptions import (
    ExpectationSuiteNotFoundError,
    ValidationDefinitionNotFoundError,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.data_context import AbstractDataContext

SCHEMA_FIELDS = set(ExpectationSuiteSchema().fields)


@pytest.fixture
def restore_current_context() -> Iterator[None]:
    """Put the process-wide current Data Context back as the test found it."""
    try:
        previous: AbstractDataContext | None = project_manager.get_current_project()
    except DataContextRequiredError:
        previous = None
    yield
    set_context(previous)


@pytest.fixture
def c1_and_c2(
    restore_current_context: None,
) -> tuple[AbstractDataContext, AbstractDataContext]:
    """Two distinct ephemeral contexts, with the second (C2) current."""
    c1 = gx.get_context(mode="ephemeral")
    c2 = gx.get_context(mode="ephemeral")
    assert c1 is not c2
    assert project_manager.get_current_project() is c2
    return c1, c2


@pytest.fixture
def single_context(restore_current_context: None) -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


class TestStoreStampsTheSuitesItHandsOut:
    @pytest.mark.unit
    def test_add_returns_a_suite_owned_by_the_context_it_was_added_to(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2

        added = c1.suites.add(ExpectationSuite(name="my_suite"))

        assert added._owner is c1

    @pytest.mark.unit
    def test_add_stamps_the_callers_original_object(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        original = ExpectationSuite(name="my_suite")
        assert original._owner is None

        returned = c1.suites.add(original)

        assert original._owner is c1
        assert returned is not original

    @pytest.mark.unit
    def test_a_failed_add_leaves_the_callers_object_unstamped(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        c1.suites.add(ExpectationSuite(name="my_suite"))
        duplicate = ExpectationSuite(name="my_suite")

        with pytest.raises(DataContextError):
            c1.suites.add(duplicate)

        assert duplicate._owner is None

    @pytest.mark.unit
    def test_a_failed_store_write_leaves_the_callers_object_unstamped(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        # Below the factory's own existence check: the backend itself refuses the second write.
        c1, _ = c1_and_c2
        store = c1.suites._store
        key = store.get_key(name="my_suite", id=None)
        store.add(key=key, value=ExpectationSuite(name="my_suite"))
        duplicate = ExpectationSuite(name="my_suite")

        with pytest.raises(Exception, match="my_suite"):
            store.add(key=key, value=duplicate)

        assert duplicate._owner is None

    @pytest.mark.unit
    def test_get_returns_a_suite_owned_by_its_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        c1.suites.add(ExpectationSuite(name="my_suite"))

        assert c1.suites.get("my_suite")._owner is c1

    @pytest.mark.unit
    def test_add_or_update_returns_a_suite_owned_by_its_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2

        created = c1.suites.add_or_update(ExpectationSuite(name="my_suite"))

        assert created._owner is c1

    @pytest.mark.unit
    def test_all_returns_suites_owned_by_their_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        c1.suites.add(ExpectationSuite(name="first"))
        c1.suites.add(ExpectationSuite(name="second"))

        suites = list(c1.suites.all())

        assert len(suites) == 2
        assert all(suite._owner is c1 for suite in suites)

    @pytest.mark.unit
    def test_update_stamps_the_suite_it_writes(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        store = c1.suites._store
        persisted = c1.suites.add(ExpectationSuite(name="my_suite"))
        unowned = ExpectationSuite(name="my_suite", id=persisted.id)
        assert unowned._owner is None

        store.update(key=store.get_key(name="my_suite", id=persisted.id), value=unowned)

        assert unowned._owner is c1

    @pytest.mark.unit
    def test_a_failed_update_leaves_the_callers_object_unstamped(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        # The suite was never added, so the backend refuses to update it.
        c1, _ = c1_and_c2
        store = c1.suites._store
        never = ExpectationSuite(name="never_added")

        with pytest.raises(ExpectationSuiteNotAddedError):
            store.update(key=store.get_key(name="never_added", id=None), value=never)

        assert never._owner is None

    @pytest.mark.unit
    def test_each_context_stamps_its_own_suites(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        c1.suites.add(ExpectationSuite(name="in_one"))
        c2.suites.add(ExpectationSuite(name="in_two"))

        assert c1.suites.get("in_one")._owner is c1
        assert c2.suites.get("in_two")._owner is c2

    @pytest.mark.unit
    def test_a_later_set_context_does_not_change_the_owner(
        self, restore_current_context: None
    ) -> None:
        c2 = gx.get_context(mode="ephemeral")
        c1 = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is c1
        owned = c1.suites.add(ExpectationSuite(name="my_suite"))
        unowned = copy.deepcopy(owned)

        set_context(c2)

        assert project_manager.get_current_project() is c2
        assert owned._owner is c1
        assert owned._resolve_context().context is c1
        assert owned._resolve_context().bound is True
        # A suite that belongs to no context follows the current one.
        assert unowned._resolve_context().context is c2
        assert unowned._resolve_context().bound is False


class TestStoreWithoutAContext:
    @pytest.mark.unit
    def test_a_directly_constructed_store_has_no_context(self) -> None:
        assert ExpectationsStore().data_context is None

    @pytest.mark.unit
    def test_a_directly_constructed_store_leaves_suites_unowned(
        self,
        single_context: AbstractDataContext,  # a current context, for the suite's own lookups
    ) -> None:
        store = ExpectationsStore()
        key = store.get_key(name="my_suite", id=None)
        suite = ExpectationSuite(name="my_suite")

        store.add(key=key, value=suite)
        built = store.deserialize_suite_dict(store.get(key=key))

        assert suite._owner is None
        assert built._owner is None

    @pytest.mark.unit
    def test_a_store_built_by_a_context_exposes_that_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2

        assert c1.suites._store.data_context is c1
        assert c2.suites._store.data_context is c2


class TestFileBackedStore:
    @pytest.mark.filesystem
    def test_file_backed_stores_stamp_their_context(
        self, restore_current_context: None, tmp_path: pathlib.Path
    ) -> None:
        c1 = gx.get_context(mode="file", project_root_dir=tmp_path / "one")
        c2 = gx.get_context(mode="file", project_root_dir=tmp_path / "two")
        assert project_manager.get_current_project() is c2
        original = ExpectationSuite(name="my_suite")

        added = c1.suites.add(original)

        assert original._owner is c1
        assert added._owner is c1
        assert c1.suites.get("my_suite")._owner is c1
        assert [suite._owner for suite in c1.suites.all()] == [c1]


class TestCopiesBelongToNoContext:
    """A suite added to a single context stays copyable, and a copy is owned by nobody."""

    @pytest.fixture
    def owned(self, single_context: AbstractDataContext) -> ExpectationSuite:
        suite = single_context.suites.add(ExpectationSuite(name="my_suite"))
        assert suite._owner is single_context
        return suite

    @pytest.mark.unit
    def test_deepcopy_is_unowned(self, owned: ExpectationSuite) -> None:
        duplicate = copy.deepcopy(owned)

        assert duplicate._owner is None
        assert set(duplicate.to_dict()) == SCHEMA_FIELDS

    @pytest.mark.unit
    def test_shallow_copy_is_unowned(self, owned: ExpectationSuite) -> None:
        duplicate = copy.copy(owned)

        assert duplicate._owner is None
        assert set(duplicate.to_dict()) == SCHEMA_FIELDS

    @pytest.mark.unit
    def test_pickle_round_trip_is_unowned(self, owned: ExpectationSuite) -> None:
        duplicate = pickle.loads(pickle.dumps(owned))

        assert duplicate._owner is None
        assert set(duplicate.to_dict()) == SCHEMA_FIELDS

    @pytest.mark.unit
    def test_copying_leaves_the_original_owned(self, owned: ExpectationSuite) -> None:
        owner = owned._owner
        copy.deepcopy(owned)
        copy.copy(owned)
        pickle.loads(pickle.dumps(owned))

        assert owned._owner is owner


def _suite_with_one_expectation(context: AbstractDataContext) -> ExpectationSuite:
    suite = ExpectationSuite(
        name="shared_name",
        expectations=[gxe.ExpectColumnValuesToNotBeNull(column="original")],
    )
    return context.suites.add(suite)


def _columns(context: AbstractDataContext) -> list[str]:
    return sorted(
        e.configuration.kwargs["column"] for e in context.suites.get("shared_name").expectations
    )


class TestChangesToASuiteLandInItsOwnContext:
    """A suite changed while another context is current writes to the context it came from.

    C1 and C2 are distinct projects that both hold a suite named ``shared_name``, so a write
    that went to the wrong context would show up as a change in C2.
    """

    @pytest.fixture
    def held(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> tuple[AbstractDataContext, AbstractDataContext, ExpectationSuite]:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        suite = c1.suites.get("shared_name")
        assert project_manager.get_current_project() is c2
        return c1, c2, suite

    @pytest.mark.unit
    def test_add_expectation(
        self, held: tuple[AbstractDataContext, AbstractDataContext, ExpectationSuite]
    ) -> None:
        c1, c2, suite = held

        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="added"))

        assert _columns(c1) == ["added", "original"]
        assert _columns(c2) == ["original"]

    @pytest.mark.unit
    def test_delete_expectation(
        self, held: tuple[AbstractDataContext, AbstractDataContext, ExpectationSuite]
    ) -> None:
        c1, c2, suite = held

        suite.delete_expectation(suite.expectations[0])

        assert _columns(c1) == []
        assert _columns(c2) == ["original"]

    @pytest.mark.unit
    def test_save(
        self, held: tuple[AbstractDataContext, AbstractDataContext, ExpectationSuite]
    ) -> None:
        c1, c2, suite = held
        suite.notes = "changed in the owner"

        suite.save()

        assert c1.suites.get("shared_name").notes == "changed in the owner"
        assert c2.suites.get("shared_name").notes is None

    @pytest.mark.unit
    def test_save_on_one_of_its_expectations(
        self, held: tuple[AbstractDataContext, AbstractDataContext, ExpectationSuite]
    ) -> None:
        c1, c2, suite = held
        expectation = suite.expectations[0]
        assert isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
        expectation.column = "edited"

        expectation.save()

        assert _columns(c1) == ["edited"]
        assert _columns(c2) == ["original"]

    @pytest.mark.unit
    def test_the_context_that_lacks_the_name_is_left_without_it(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        suite = _suite_with_one_expectation(c1)

        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="added"))

        assert _columns(c1) == ["added", "original"]
        assert [s.name for s in c2.suites.all()] == []


def _batch_definition_in(context: AbstractDataContext) -> BatchDefinition:
    asset = context.data_sources.add_pandas("source").add_dataframe_asset("asset")
    return asset.add_batch_definition_whole_dataframe("batch_definition")


class TestAHeldSuiteTakesItsHoldersContext:
    @pytest.mark.unit
    def test_a_bound_validation_definition_records_its_suites_owner(
        self, restore_current_context: None
    ) -> None:
        c2 = gx.get_context(mode="ephemeral")
        c1 = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is c1
        batch_definition = _batch_definition_in(c1)
        stored = c1.suites.add(ExpectationSuite(name="held_suite"))
        held = ExpectationSuite(name="held_suite", id=stored.id)
        assert held._owner is None
        validation_definition = ValidationDefinition(
            name="my_vd", data=batch_definition, suite=held
        )
        assert validation_definition.suite._owner is None

        set_context(c2)

        assert project_manager.get_current_project() is c2

        resolved = validation_definition._resolve_context()

        assert resolved.context is c1
        assert resolved.bound is True
        assert validation_definition.suite._owner is c1

    @pytest.mark.unit
    def test_the_recorded_owner_routes_the_suites_reads_and_writes(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        batch_definition = _batch_definition_in(c1)
        stored = _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        validation_definition = ValidationDefinition(
            name="my_vd",
            data=batch_definition,
            suite=ExpectationSuite(
                name="shared_name",
                id=stored.id,
                expectations=[gxe.ExpectColumnValuesToNotBeNull(column="original")],
            ),
        )
        validation_definition._resolve_context()

        validation_definition.suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column="added")
        )

        assert _columns(c1) == ["added", "original"]
        assert _columns(c2) == ["original"]

    @pytest.mark.unit
    def test_a_suite_without_an_owner_attribute_does_not_stop_resolution(
        self,
        c1_and_c2: tuple[AbstractDataContext, AbstractDataContext],
        mocker: MockerFixture,
    ) -> None:
        c1, _ = c1_and_c2
        batch_definition = _batch_definition_in(c1)
        suite = mocker.Mock(spec=ExpectationSuite)
        validation_definition = ValidationDefinition.construct(
            name="my_vd", data=batch_definition, suite=suite
        )

        resolved = validation_definition._resolve_context()

        assert resolved.context is c1
        assert resolved.bound is True

    @pytest.mark.unit
    def test_a_suite_that_already_has_an_owner_keeps_it(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        batch_definition = _batch_definition_in(c1)
        owned_by_c2 = c2.suites.add(ExpectationSuite(name="held_suite"))
        validation_definition = ValidationDefinition(
            name="my_vd", data=batch_definition, suite=owned_by_c2
        )

        validation_definition._resolve_context()

        assert validation_definition.suite._owner is c2

    @pytest.mark.unit
    def test_an_unbound_validation_definition_leaves_its_suite_unowned(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        del c1_and_c2  # two contexts exist; the batch definition belongs to neither
        batch_definition = (
            PandasDatasource(name="source")
            .add_dataframe_asset("asset")
            .add_batch_definition_whole_dataframe("batch_definition")
        )
        suite = ExpectationSuite(name="held_suite")
        validation_definition = ValidationDefinition(
            name="my_vd", data=batch_definition, suite=suite
        )
        assert validation_definition._resolve_context().bound is False

        assert validation_definition.suite._owner is None


class TestFreshnessChecksAHeldSuiteThroughItsHoldersContext:
    """``is_fresh()`` records the held suite's owner before it checks the suite.

    The batch definition's own freshness check is stubbed to succeed: these tests are about
    the order of the suite check and the validation definition's own lookup.
    """

    @pytest.fixture(autouse=True)
    def _fresh_batch_definition(self, mocker: MockerFixture) -> None:
        mocker.patch.object(
            BatchDefinition,
            "is_fresh",
            return_value=BatchDefinitionFreshnessDiagnostics(errors=[]),
        )

    @staticmethod
    def _held(context: AbstractDataContext) -> ExpectationSuite:
        suite = copy.deepcopy(context.suites.add(ExpectationSuite(name="held_suite")))
        assert suite._owner is None
        return suite

    @staticmethod
    def _error_types(validation_definition: ValidationDefinition) -> list[type]:
        return [type(error) for error in validation_definition.is_fresh().errors]

    @pytest.mark.unit
    def test_the_first_check_reads_the_suite_through_the_holders_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, _ = c1_and_c2
        validation_definition = ValidationDefinition(
            name="my_vd", data=_batch_definition_in(c1), suite=self._held(c1), id="my_vd_id"
        )

        assert self._error_types(validation_definition) == [ValidationDefinitionNotFoundError]
        assert validation_definition.suite._owner is c1

    @pytest.mark.unit
    def test_consecutive_checks_on_an_unchanged_object_agree(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        validation_definition = ValidationDefinition(
            name="my_vd", data=_batch_definition_in(c1), suite=self._held(c2), id="my_vd_id"
        )

        first = self._error_types(validation_definition)
        second = self._error_types(validation_definition)

        assert first == second == [ExpectationSuiteNotFoundError]
        assert validation_definition.suite._owner is c1


class TestASuiteWithoutAHolderUsesTheCurrentContext:
    @pytest.mark.unit
    def test_an_unowned_suite_reads_and_writes_through_the_current_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        unowned = ExpectationSuite(
            name="shared_name",
            id=c2.suites.get("shared_name").id,
            expectations=[gxe.ExpectColumnValuesToNotBeNull(column="original")],
        )
        assert unowned._owner is None
        assert unowned._store is c2.expectations_store

        unowned.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="added"))

        assert _columns(c2) == ["added", "original"]
        assert _columns(c1) == ["original"]


class TestAddOrUpdateWritesThroughTheFactorysOwnStore:
    """``add_or_update`` of an existing name writes to the context whose factory it was called on.

    The caller's suite was built by hand, so it belongs to no context; the write must not fall
    back to whichever context happens to be current (#12209).
    """

    @staticmethod
    def _changed(context: AbstractDataContext) -> ExpectationSuite:
        """A hand-built suite of the shared name, with an expectation the stored one lacks."""
        return ExpectationSuite(
            name="shared_name",
            id=context.suites.get("shared_name").id,
            expectations=[
                gxe.ExpectColumnValuesToNotBeNull(column="original"),
                gxe.ExpectColumnValuesToNotBeNull(column="added"),
            ],
        )

    @pytest.mark.unit
    def test_the_change_lands_in_the_factorys_context_and_not_the_current_one(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        changed = self._changed(c1)
        assert changed._owner is None

        c1.suites.add_or_update(changed)

        assert _columns(c1) == ["added", "original"]
        assert _columns(c2) == ["original"]

    @pytest.mark.unit
    def test_the_returned_suite_and_the_callers_suite_are_owned_by_the_factorys_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        changed = self._changed(c1)

        returned = c1.suites.add_or_update(changed)

        assert returned._owner is c1
        assert changed._owner is c1

    @pytest.mark.unit
    def test_the_call_succeeds_when_the_current_context_lacks_the_name(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        changed = self._changed(c1)
        assert [s.name for s in c2.suites.all()] == []

        returned = c1.suites.add_or_update(changed)

        assert returned._owner is c1
        assert _columns(c1) == ["added", "original"]
        assert [s.name for s in c2.suites.all()] == []

    @pytest.mark.unit
    def test_a_single_context_update_persists_by_value(
        self, single_context: AbstractDataContext
    ) -> None:
        _suite_with_one_expectation(single_context)
        changed = self._changed(single_context)
        changed.notes = "updated"

        returned = single_context.suites.add_or_update(changed)

        stored = single_context.suites.get("shared_name")
        assert _columns(single_context) == ["added", "original"]
        assert stored.notes == "updated"
        assert stored.id == returned.id == changed.id
        assert returned._owner is single_context

    @pytest.mark.unit
    def test_a_suite_without_an_id_takes_the_stored_suites_id(
        self, single_context: AbstractDataContext
    ) -> None:
        original = _suite_with_one_expectation(single_context)
        changed = self._changed(single_context)
        changed.id = None

        returned = single_context.suites.add_or_update(changed)

        assert changed.id == original.id
        assert returned.id == original.id
        assert single_context.suites.get("shared_name").id == original.id

    @pytest.mark.unit
    def test_the_write_is_keyed_by_the_stored_suites_name_and_id(
        self, single_context: AbstractDataContext, mocker: MockerFixture
    ) -> None:
        original = _suite_with_one_expectation(single_context)
        changed = self._changed(single_context)
        changed.id = None
        get_key = mocker.spy(single_context.expectations_store, "get_key")

        single_context.suites.add_or_update(changed)

        assert mocker.call(name="shared_name", id=original.id) in get_key.call_args_list

    @pytest.mark.unit
    def test_a_suite_owned_by_another_context_is_written_to_the_factorys_context(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        _suite_with_one_expectation(c1)
        _suite_with_one_expectation(c2)
        foreign = c2.suites.get("shared_name")
        foreign.notes = "changed elsewhere"
        foreign.id = c1.suites.get("shared_name").id
        assert foreign._owner is c2

        returned = c1.suites.add_or_update(foreign)

        assert c1.suites.get("shared_name").notes == "changed elsewhere"
        assert c2.suites.get("shared_name").notes is None
        assert returned._owner is c1
        assert foreign._owner is c1

    @pytest.mark.unit
    def test_the_write_is_rendered_when_the_context_asks_for_rendered_content(
        self,
        single_context: AbstractDataContext,
        mocker: MockerFixture,
    ) -> None:
        _suite_with_one_expectation(single_context)
        changed = self._changed(single_context)
        mocker.patch.object(project_manager, "is_using_cloud", return_value=True)
        render = mocker.spy(ExpectationSuite, "render")

        single_context.suites.add_or_update(changed)

        assert [c.args[0] for c in render.call_args_list if c.args[0] is changed] == [changed]

    @pytest.mark.unit
    def test_the_write_is_not_rendered_otherwise(
        self,
        single_context: AbstractDataContext,
        mocker: MockerFixture,
    ) -> None:
        _suite_with_one_expectation(single_context)
        changed = self._changed(single_context)
        render = mocker.spy(ExpectationSuite, "render")

        single_context.suites.add_or_update(changed)

        assert [c for c in render.call_args_list if c.args[0] is changed] == []


class TestAnOwnedSuitesMissIsReportedPlainly:
    """A miss on a suite that belongs to a context was looked up in that context, so it says so.

    The cause-naming note is for a lookup that went through the current context because the
    suite belongs to none; an owned suite's miss carries the plain text.
    """

    @pytest.mark.unit
    def test_a_miss_in_the_owning_context_carries_no_current_context_note(
        self, c1_and_c2: tuple[AbstractDataContext, AbstractDataContext]
    ) -> None:
        c1, c2 = c1_and_c2
        suite = c1.suites.add(ExpectationSuite(name="shared_name"))
        c2.suites.add(ExpectationSuite(name="shared_name"))
        c1.suites.delete("shared_name")
        assert [s.name for s in c1.suites.all()] == []
        assert [s.name for s in c2.suites.all()] == ["shared_name"]
        assert project_manager.get_current_project() is c2

        diagnostics = suite.is_fresh()

        assert diagnostics.success is False
        assert len(diagnostics.errors) == 1
        error = diagnostics.errors[0]
        assert isinstance(error, ExpectationSuiteNotFoundError)
        assert str(error) == (
            "ExpectationSuite 'shared_name' not found. Please check the name and try again."
        )
