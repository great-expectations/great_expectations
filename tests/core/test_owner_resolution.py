from __future__ import annotations

import copy
import dataclasses
import pathlib
import pickle
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import pytest

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite, ExpectationSuiteSchema
from great_expectations.core.owner_resolution import (
    ResolvedContext,
    consulted_context_note,
    owner_from_batch_definition,
    resolve_context,
    unbound_resolution_note,
)
from great_expectations.core.serdes import _IdentifierBundle
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.exceptions.exceptions import (
    BatchDefinitionNotFoundError,
    CheckpointNotFoundError,
    DataAssetNotFoundError,
    DataContextRequiredError,
    DatasourceNotFoundError,
    ExpectationSuiteNotFoundError,
    ValidationDefinitionNotFoundError,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# The note carries its own leading space: it is appended to the message verbatim.
NOTE = " This object is not bound to a Data Context; the current Data Context was consulted."


class TestNotFoundErrorNotes:
    """Each not-found error takes an optional note appended to its unchanged message (#12209)."""

    @pytest.mark.unit
    def test_expectation_suite_message_without_note_is_unchanged(self) -> None:
        error = ExpectationSuiteNotFoundError("my_suite")
        assert (
            str(error)
            == "ExpectationSuite 'my_suite' not found. Please check the name and try again."
        )

    @pytest.mark.unit
    def test_expectation_suite_message_with_note_appends_it(self) -> None:
        error = ExpectationSuiteNotFoundError("my_suite", note=NOTE)
        assert str(error) == (
            "ExpectationSuite 'my_suite' not found. Please check the name and try again."
            " This object is not bound to a Data Context; the current Data Context was consulted."
        )

    @pytest.mark.unit
    def test_validation_definition_message_without_note_is_unchanged(self) -> None:
        error = ValidationDefinitionNotFoundError("my_vd")
        assert (
            str(error)
            == "ValidationDefinition 'my_vd' not found. Please check the name and try again."
        )

    @pytest.mark.unit
    def test_validation_definition_message_with_note_appends_it(self) -> None:
        error = ValidationDefinitionNotFoundError("my_vd", note=NOTE)
        assert str(error) == (
            "ValidationDefinition 'my_vd' not found. Please check the name and try again."
            " This object is not bound to a Data Context; the current Data Context was consulted."
        )

    @pytest.mark.unit
    def test_checkpoint_message_without_note_is_unchanged(self) -> None:
        error = CheckpointNotFoundError("my_cp")
        assert str(error) == "Checkpoint 'my_cp' not found. Please check the name and try again."

    @pytest.mark.unit
    def test_checkpoint_message_with_note_appends_it(self) -> None:
        error = CheckpointNotFoundError("my_cp", note=NOTE)
        assert str(error) == (
            "Checkpoint 'my_cp' not found. Please check the name and try again."
            " This object is not bound to a Data Context; the current Data Context was consulted."
        )

    @pytest.mark.unit
    def test_batch_definition_message_without_note_is_unchanged(self) -> None:
        error = BatchDefinitionNotFoundError("my_bd")
        assert (
            str(error) == "BatchDefinition 'my_bd' not found. Please check the name and try again."
        )

    @pytest.mark.unit
    def test_batch_definition_message_with_note_appends_it(self) -> None:
        error = BatchDefinitionNotFoundError("my_bd", note=NOTE)
        assert str(error) == (
            "BatchDefinition 'my_bd' not found. Please check the name and try again."
            " This object is not bound to a Data Context; the current Data Context was consulted."
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "make_error",
        [
            ExpectationSuiteNotFoundError,
            ValidationDefinitionNotFoundError,
            CheckpointNotFoundError,
            BatchDefinitionNotFoundError,
        ],
    )
    def test_name_keyword_and_none_note_match_positional(
        self, make_error: Callable[..., Exception]
    ) -> None:
        positional = str(make_error("x"))
        assert str(make_error(name="x")) == positional
        assert str(make_error("x", note=None)) == positional


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
def no_current_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty the shared project manager itself, so every module that reads it sees no context.

    monkeypatch puts the previous current context back at teardown.
    """
    monkeypatch.setattr(project_manager, "_ProjectManager__project", None)


def _bound_batch_definition(context: AbstractDataContext) -> BatchDefinition[Any]:
    datasource = context.data_sources.add_pandas("my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    return asset.add_batch_definition_whole_dataframe("my_batch_definition")


class TestOwnerFromBatchDefinition:
    """The chain walk returns the owning context, or None, and never raises (#12209)."""

    @pytest.mark.unit
    def test_bare_batch_definition_has_no_owner(self) -> None:
        assert owner_from_batch_definition(BatchDefinition(name="bd")) is None

    @pytest.mark.unit
    def test_validation_definition_mock_has_no_owner(self, mocker: MockerFixture) -> None:
        assert owner_from_batch_definition(mocker.Mock(spec=ValidationDefinition)) is None

    @pytest.mark.unit
    def test_hand_assembled_datasource_without_context_has_no_owner(self) -> None:
        datasource = PandasDatasource(name="my_datasource")
        asset = datasource.add_dataframe_asset("my_asset")
        batch_definition = asset.add_batch_definition_whole_dataframe("bd")

        assert datasource.data_context is None
        assert owner_from_batch_definition(batch_definition) is None

    @pytest.mark.unit
    def test_batch_definition_mock_has_no_owner(self, mocker: MockerFixture) -> None:
        assert owner_from_batch_definition(mocker.Mock(spec=BatchDefinition)) is None

    @pytest.mark.unit
    def test_unspecced_mock_chain_has_no_owner(self, mocker: MockerFixture) -> None:
        assert owner_from_batch_definition(mocker.Mock()) is None

    @pytest.mark.unit
    def test_chain_ending_in_none_has_no_owner(self, mocker: MockerFixture) -> None:
        batch_definition = mocker.Mock()
        batch_definition.data_asset.datasource.data_context = None
        assert owner_from_batch_definition(batch_definition) is None

    @pytest.mark.unit
    def test_none_has_no_owner(self) -> None:
        assert owner_from_batch_definition(None) is None

    @pytest.mark.unit
    def test_real_chain_returns_the_owning_context(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(context)

        assert owner_from_batch_definition(batch_definition) is context


class TestResolveContext:
    """An owner resolves as bound; otherwise the current context resolves as not bound."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "make_object",
        [
            lambda mocker: BatchDefinition(name="bd"),
            lambda mocker: mocker.Mock(spec=ValidationDefinition),
            lambda mocker: (
                PandasDatasource(name="ds")
                .add_dataframe_asset("a")
                .add_batch_definition_whole_dataframe("bd")
            ),
            lambda mocker: mocker.Mock(spec=BatchDefinition),
        ],
        ids=["bare", "validation_definition_mock", "hand_assembled", "batch_definition_mock"],
    )
    def test_ownerless_shapes_resolve_to_the_current_context_unbound(
        self,
        make_object: Callable[[MockerFixture], Any],
        mocker: MockerFixture,
        restore_current_context: None,
    ) -> None:
        current = gx.get_context(mode="ephemeral")

        resolved = resolve_context(owner_from_batch_definition(make_object(mocker)))

        assert resolved == ResolvedContext(context=current, bound=False)
        assert resolved.context is current

    @pytest.mark.unit
    def test_real_chain_resolves_to_its_owner_while_another_context_is_current(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(owner)
        other = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is other

        resolved = resolve_context(owner_from_batch_definition(batch_definition))

        assert resolved.context is owner
        assert resolved.bound is True

    @pytest.mark.unit
    def test_real_chain_resolves_to_its_owner_after_set_context_selects_the_other(
        self, restore_current_context: None
    ) -> None:
        other = gx.get_context(mode="ephemeral")
        owner = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(owner)
        assert project_manager.get_current_project() is owner
        set_context(other)  # selecting the other context is the stimulus
        assert project_manager.get_current_project() is other

        resolved = resolve_context(owner_from_batch_definition(batch_definition))

        assert resolved.context is owner
        assert resolved.bound is True

    @pytest.mark.unit
    def test_no_owner_and_no_current_context_raises_the_required_error(
        self, no_current_context: None
    ) -> None:
        with pytest.raises(DataContextRequiredError) as exc_info:
            resolve_context(None)

        assert str(exc_info.value) == (
            "This action requires an active data context. "
            "Please call `great_expectations.get_context()` first, then try your action again."
        )

    @pytest.mark.unit
    def test_owner_resolves_without_any_current_context(
        self, restore_current_context: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        monkeypatch.setattr(project_manager, "_ProjectManager__project", None)

        assert resolve_context(owner) == ResolvedContext(context=owner, bound=True)

    @pytest.mark.unit
    def test_resolved_context_is_frozen(self, mocker: MockerFixture) -> None:
        resolved = ResolvedContext(context=mocker.Mock(spec=AbstractDataContext), bound=True)

        field = "bound"
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(resolved, field, False)


class TestUnboundResolutionNote:
    """The note says the object is not bound and names the context that was consulted."""

    @pytest.mark.unit
    def test_ephemeral_context_is_named_by_mode_alone(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")

        assert unbound_resolution_note(context) == (
            " This object is not bound to a Data Context,"
            " so the lookup went through the current ephemeral Data Context."
        )

    @pytest.mark.filesystem
    def test_file_context_is_named_by_mode_directory_and_id(
        self, tmp_path: pathlib.Path, restore_current_context: None
    ) -> None:
        context = gx.get_context(mode="file", project_root_dir=tmp_path)
        root_directory = str(tmp_path / "gx")
        context_id = str(context.data_context_id)
        assert context_id != "None"

        assert unbound_resolution_note(context) == (
            " This object is not bound to a Data Context,"
            f" so the lookup went through the current file Data Context at '{root_directory}'"
            f" (id {context_id})."
        )


class TestConsultedContextNote:
    """A lookup that cannot tell whether its object is bound names only the context consulted."""

    @pytest.mark.unit
    def test_ephemeral_context_is_named_by_mode_alone(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")

        assert consulted_context_note(context) == (
            " The lookup went through the current ephemeral Data Context."
        )

    @pytest.mark.filesystem
    def test_file_context_is_named_by_mode_directory_and_id(
        self, tmp_path: pathlib.Path, restore_current_context: None
    ) -> None:
        context = gx.get_context(mode="file", project_root_dir=tmp_path)
        root_directory = str(tmp_path / "gx")
        context_id = str(context.data_context_id)
        assert context_id != "None"

        assert consulted_context_note(context) == (
            f" The lookup went through the current file Data Context at '{root_directory}'"
            f" (id {context_id})."
        )


class TestExpectationSuiteOwnerSlot:
    """The owner is bookkeeping: no mapping view, serialized form, comparison or copy carries it."""

    @staticmethod
    def _owned_suite(context: AbstractDataContext) -> ExpectationSuite:
        suite = context.suites.add(ExpectationSuite(name="my_suite"))
        suite._owner = context  # nothing sets the owner yet, so the test does
        return suite

    @pytest.mark.unit
    def test_owner_is_not_a_mapping_key(self, restore_current_context: None) -> None:
        suite = self._owned_suite(gx.get_context(mode="ephemeral"))

        assert sorted(suite.keys()) == sorted(ExpectationSuiteSchema().fields)
        assert len(suite) == len(ExpectationSuiteSchema().fields)
        assert "_owner" not in dict(suite.items())

    @pytest.mark.unit
    def test_suite_rebuilds_from_its_own_mapping(self, restore_current_context: None) -> None:
        suite = self._owned_suite(gx.get_context(mode="ephemeral"))

        assert ExpectationSuite(**suite) == suite
        assert ExpectationSuite(**dict(suite)) == suite

    @pytest.mark.unit
    def test_owner_is_not_part_of_the_serialized_form_or_comparisons(
        self, restore_current_context: None
    ) -> None:
        suite = self._owned_suite(gx.get_context(mode="ephemeral"))
        unowned = ExpectationSuite(name=suite.name, id=suite.id, meta=suite.meta)

        assert sorted(suite.to_dict()) == sorted(ExpectationSuiteSchema().fields)
        assert suite.to_dict() == unowned.to_dict()
        assert suite.to_json_dict() == unowned.to_json_dict()
        assert suite == unowned
        assert hash(suite) == hash(unowned)
        assert repr(suite) == repr(unowned)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "make_copy",
        [
            pytest.param(copy.deepcopy, id="deepcopy"),
            pytest.param(copy.copy, id="copy"),
            pytest.param(lambda suite: pickle.loads(pickle.dumps(suite)), id="pickle"),
        ],
    )
    def test_a_copy_belongs_to_no_context(
        self,
        make_copy: Callable[[ExpectationSuite], ExpectationSuite],
        restore_current_context: None,
    ) -> None:
        context = gx.get_context(mode="ephemeral")
        suite = self._owned_suite(context)

        copied = make_copy(suite)

        assert copied._owner is None
        assert copied == suite
        assert sorted(copied.to_dict()) == sorted(ExpectationSuiteSchema().fields)
        assert suite._owner is context

    @pytest.mark.unit
    def test_owned_suite_resolves_to_its_owner(self, restore_current_context: None) -> None:
        owner = gx.get_context(mode="ephemeral")
        suite = self._owned_suite(owner)
        gx.get_context(mode="ephemeral")

        assert suite._resolve_context() == ResolvedContext(context=owner, bound=True)

    @pytest.mark.unit
    def test_owned_suite_miss_carries_no_note(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")
        suite = ExpectationSuite(name="never_added", id="a-suite-id")
        suite._owner = context

        diagnostics = suite.is_fresh()

        # The literal is what the unfixed tree reports for the same suite.
        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                ExpectationSuiteNotFoundError,
                "ExpectationSuite 'never_added' not found. Please check the name and try again.",
            )
        ]


class TestExpectationSuiteMiss:
    """A suite that belongs to no Data Context says which context the lookup went through."""

    @pytest.mark.unit
    def test_unbound_suite_miss_names_the_ephemeral_context_consulted(
        self, restore_current_context: None
    ) -> None:
        context = gx.get_context(mode="ephemeral")
        suite = ExpectationSuite(name="never_added", id="a-suite-id")

        diagnostics = suite.is_fresh()

        assert diagnostics.success is False
        assert len(diagnostics.errors) == 1
        error = diagnostics.errors[0]
        assert isinstance(error, ExpectationSuiteNotFoundError)
        assert str(error) == (
            "ExpectationSuite 'never_added' not found. Please check the name and try again."
            + unbound_resolution_note(context)
        )
        assert "This object is not bound to a Data Context" in str(error)
        assert "the current ephemeral Data Context." in str(error)

    @pytest.mark.filesystem
    def test_unbound_suite_miss_names_the_file_context_consulted(
        self, tmp_path: pathlib.Path, restore_current_context: None
    ) -> None:
        context = gx.get_context(mode="file", project_root_dir=tmp_path)
        root_directory = str(tmp_path / "gx")
        context_id = str(context.data_context_id)
        assert context_id != "None"
        suite = ExpectationSuite(name="never_added", id="a-suite-id")

        diagnostics = suite.is_fresh()

        assert len(diagnostics.errors) == 1
        error = diagnostics.errors[0]
        assert isinstance(error, ExpectationSuiteNotFoundError)
        assert str(error) == (
            "ExpectationSuite 'never_added' not found. Please check the name and try again."
            " This object is not bound to a Data Context,"
            f" so the lookup went through the current file Data Context at '{root_directory}'"
            f" (id {context_id})."
        )

    @pytest.mark.unit
    def test_unbound_suite_lookup_without_a_current_context_requires_one(
        self, no_current_context: None
    ) -> None:
        suite = ExpectationSuite(name="never_added", id="a-suite-id")

        with pytest.raises(DataContextRequiredError) as exc_info:
            suite.is_fresh()

        assert str(exc_info.value) == (
            "This action requires an active data context. "
            "Please call `great_expectations.get_context()` first, then try your action again."
        )


NOT_BOUND = "This object is not bound to a Data Context, so the lookup went through the current "
EPHEMERAL_NOTE = NOT_BOUND + "ephemeral Data Context."
# Parsing a record cannot tell whether it was read from the current context's store or another's,
# so a parse-time miss names the context consulted without saying the object is unbound.
CONSULTED_EPHEMERAL_NOTE = "The lookup went through the current ephemeral Data Context."


def _chain_in(context: AbstractDataContext) -> tuple[BatchDefinition[Any], ExpectationSuite]:
    """A datasource, asset, batch definition and suite, all added to ``context``."""
    batch_definition = _bound_batch_definition(context)
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    return batch_definition, suite


def _ownerless_copy_of(batch_definition: BatchDefinition[Any]) -> BatchDefinition[Any]:
    """The same batch definition, rebuilt on a datasource that belongs to no Data Context."""
    datasource = PandasDatasource(name="my_datasource")
    asset = datasource.add_dataframe_asset("my_asset")
    copy = asset.add_batch_definition_whole_dataframe("my_batch_definition")
    copy.id = batch_definition.id
    assert owner_from_batch_definition(copy) is None
    return copy


class TestBatchDefinitionMiss:
    """A batch definition's miss names the current context only when the lookup used it."""

    @pytest.mark.unit
    def test_resolves_to_its_owner(self, restore_current_context: None) -> None:
        owner = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(owner)
        gx.get_context(mode="ephemeral")

        assert batch_definition._resolve_context() == ResolvedContext(context=owner, bound=True)

    @pytest.mark.unit
    def test_ownerless_resolves_to_the_current_context(self, restore_current_context: None) -> None:
        current = gx.get_context(mode="ephemeral")
        batch_definition = _ownerless_copy_of(_bound_batch_definition(current))

        assert batch_definition._resolve_context() == ResolvedContext(context=current, bound=False)

    @pytest.mark.unit
    def test_bound_datasource_miss_keeps_todays_message(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(owner)
        owner.data_sources.delete("my_datasource")
        other = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is other
        assert "my_datasource" not in other.data_sources.all()

        diagnostics = batch_definition._is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (DatasourceNotFoundError, "Could not find datasource 'my_datasource'")
        ]

    @pytest.mark.unit
    def test_unbound_datasource_miss_names_the_context_consulted(
        self, restore_current_context: None
    ) -> None:
        elsewhere = gx.get_context(mode="ephemeral")
        batch_definition = _ownerless_copy_of(_bound_batch_definition(elsewhere))
        current = gx.get_context(mode="ephemeral")
        assert "my_datasource" not in current.data_sources.all()

        diagnostics = batch_definition._is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                DatasourceNotFoundError,
                "Could not find datasource 'my_datasource'. " + EPHEMERAL_NOTE,
            )
        ]

    @staticmethod
    def _current_context_with(*, asset: bool) -> AbstractDataContext:
        """A new current context holding the datasource, and optionally its asset."""
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_pandas("my_datasource")
        if asset:
            datasource.add_dataframe_asset("my_asset")
        return context

    @pytest.mark.unit
    def test_bound_asset_miss_keeps_todays_message(self, restore_current_context: None) -> None:
        owner = gx.get_context(mode="ephemeral")
        batch_definition = _bound_batch_definition(owner)
        owner.data_sources.get("my_datasource").delete_asset("my_asset")
        self._current_context_with(asset=False)
        assert batch_definition._resolve_context().bound is True

        diagnostics = batch_definition._is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (DataAssetNotFoundError, "Could not find asset 'my_asset'")
        ]

    @pytest.mark.unit
    def test_unbound_asset_miss_names_the_context_consulted(
        self, restore_current_context: None
    ) -> None:
        batch_definition = _ownerless_copy_of(
            _bound_batch_definition(gx.get_context(mode="ephemeral"))
        )
        self._current_context_with(asset=False)

        diagnostics = batch_definition._is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (DataAssetNotFoundError, "Could not find asset 'my_asset'. " + EPHEMERAL_NOTE)
        ]

    @pytest.mark.unit
    def test_bound_batch_definition_miss_carries_no_note(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        added = _bound_batch_definition(owner)
        batch_definition = BatchDefinition(name="other_batch_definition")
        batch_definition.set_data_asset(added.data_asset)
        self._current_context_with(asset=True)
        assert batch_definition._resolve_context().bound is True

        diagnostics = batch_definition._is_fresh()

        # The wording is the unfixed tree's, minus the sentence it nested inside the name.
        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                BatchDefinitionNotFoundError,
                "BatchDefinition 'other_batch_definition' not found."
                " Please check the name and try again.",
            )
        ]

    @pytest.mark.unit
    def test_unbound_batch_definition_miss_does_not_nest_a_sentence_in_the_name(
        self, restore_current_context: None
    ) -> None:
        missing = _ownerless_copy_of(_bound_batch_definition(gx.get_context(mode="ephemeral")))
        missing.name = "other_batch_definition"
        missing.id = "an-id"
        self._current_context_with(asset=True)

        diagnostics = missing._is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                BatchDefinitionNotFoundError,
                "BatchDefinition 'other_batch_definition' not found."
                " Please check the name and try again." + " " + EPHEMERAL_NOTE,
            )
        ]


class TestValidationDefinitionMiss:
    """A validation definition's miss names the current context only when the lookup used it."""

    @pytest.mark.unit
    def test_resolves_through_its_batch_definitions_owner(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        batch_definition, suite = _chain_in(owner)
        validation_definition = ValidationDefinition(
            name="my_vd", data=batch_definition, suite=suite
        )
        gx.get_context(mode="ephemeral")

        assert validation_definition._resolve_context() == ResolvedContext(
            context=owner, bound=True
        )

    @pytest.mark.unit
    def test_bound_miss_keeps_todays_class_and_message(self, restore_current_context: None) -> None:
        # Reads still go through the current context, so the owner is made current too: the
        # lookup then reaches the store and misses there, while the chain still resolves bound.
        context = gx.get_context(mode="ephemeral")
        batch_definition, suite = _chain_in(context)
        validation_definition = ValidationDefinition(
            name="my_vd", data=batch_definition, suite=suite, id="not-in-the-store"
        )
        assert validation_definition._resolve_context().bound is True

        diagnostics = validation_definition.is_fresh()

        # The literal is what the unfixed tree raises for the same scenario.
        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                ValidationDefinitionNotFoundError,
                "ValidationDefinition 'my_vd' not found. Please check the name and try again.",
            )
        ]

    @pytest.mark.unit
    def test_unbound_miss_names_the_context_consulted(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")
        batch_definition, suite = _chain_in(context)
        validation_definition = ValidationDefinition(
            name="my_vd",
            data=_ownerless_copy_of(batch_definition),
            suite=suite,
            id="not-in-the-store",
        )
        assert validation_definition._resolve_context().bound is False

        diagnostics = validation_definition.is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                ValidationDefinitionNotFoundError,
                "ValidationDefinition 'my_vd' not found. Please check the name and try again."
                " " + EPHEMERAL_NOTE,
            )
        ]

    @pytest.mark.unit
    def test_direct_parse_miss_raises_validation_error_naming_the_context(
        self, restore_current_context: None
    ) -> None:
        gx.get_context(mode="ephemeral")
        serialized = {
            "name": "my_vd",
            "data": {
                "datasource": {"name": "no_such_datasource", "id": None},
                "asset": {"name": "my_asset", "id": None},
                "batch_definition": {"name": "my_batch_definition", "id": None},
            },
            "suite": {"name": "no_such_suite", "id": "no-such-id"},
            "id": "an-id",
        }

        with pytest.raises(ValidationError) as exc_info:
            ValidationDefinition.parse_obj(serialized)

        message = str(exc_info.value)
        assert (
            "Could not find datasource named 'no_such_datasource'. " + CONSULTED_EPHEMERAL_NOTE
            in message
        )
        assert (
            "Could not find suite with name: no_such_suite and id: no-such-id. "
            + CONSULTED_EPHEMERAL_NOTE
            in message
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "asset_name, batch_definition_name, expected",
        [
            pytest.param(
                "no_such_asset",
                "my_batch_definition",
                "Could not find asset named 'no_such_asset' within 'my_datasource' datasource. ",
                id="asset",
            ),
            pytest.param(
                "my_asset",
                "no_such_batch_definition",
                "Could not find batch definition named 'no_such_batch_definition'"
                " within 'my_asset' asset and 'my_datasource' datasource. ",
                id="batch_definition",
            ),
        ],
    )
    def test_direct_parse_miss_below_the_datasource_names_the_context(
        self,
        asset_name: str,
        batch_definition_name: str,
        expected: str,
        restore_current_context: None,
    ) -> None:
        context = gx.get_context(mode="ephemeral")
        _, suite = _chain_in(context)
        serialized = {
            "name": "my_vd",
            "data": {
                "datasource": {"name": "my_datasource", "id": None},
                "asset": {"name": asset_name, "id": None},
                "batch_definition": {"name": batch_definition_name, "id": None},
            },
            "suite": {"name": suite.name, "id": suite.id},
            "id": "an-id",
        }

        with pytest.raises(ValidationError) as exc_info:
            ValidationDefinition.parse_obj(serialized)

        assert expected + CONSULTED_EPHEMERAL_NOTE in str(exc_info.value)

    @pytest.mark.unit
    def test_load_from_a_non_current_contexts_store_does_not_claim_the_object_is_unbound(
        self, restore_current_context: None
    ) -> None:
        # The record comes from the owner's own store, which resolves it through the owner; the
        # owner lacks the datasource, so the miss is the owner's, not the current context's.
        owner = gx.get_context(mode="ephemeral")
        batch_definition, suite = _chain_in(owner)
        owner.validation_definitions.add(
            ValidationDefinition(name="my_vd", data=batch_definition, suite=suite)
        )
        owner.data_sources.delete("my_datasource")
        gx.get_context(mode="ephemeral")

        with pytest.raises(ValidationError) as exc_info:
            owner.validation_definitions.get("my_vd")

        message = str(exc_info.value)
        assert "Could not find datasource named 'my_datasource'." in message
        assert CONSULTED_EPHEMERAL_NOTE not in message
        assert "not bound" not in message


class TestCheckpointMiss:
    """A checkpoint's miss names the current context only when the lookup used it."""

    @staticmethod
    def _stored_validation_definition(
        context: AbstractDataContext,
    ) -> tuple[BatchDefinition[Any], ValidationDefinition]:
        batch_definition, suite = _chain_in(context)
        context.validation_definitions.add(
            ValidationDefinition(name="stored_vd", data=batch_definition, suite=suite)
        )
        return batch_definition, context.validation_definitions.get("stored_vd")

    @pytest.mark.unit
    def test_empty_validation_definitions_resolve_to_the_current_context(
        self, restore_current_context: None
    ) -> None:
        current = gx.get_context(mode="ephemeral")

        checkpoint = Checkpoint(name="my_cp", validation_definitions=[])

        assert checkpoint._resolve_context() == ResolvedContext(context=current, bound=False)

    @pytest.mark.unit
    def test_validation_definition_mock_resolves_to_the_current_context(
        self, mocker: MockerFixture, restore_current_context: None
    ) -> None:
        current = gx.get_context(mode="ephemeral")
        checkpoint = Checkpoint(
            name="my_cp", validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
        )

        assert checkpoint._resolve_context() == ResolvedContext(context=current, bound=False)

    @pytest.mark.unit
    def test_resolves_through_its_first_validation_definitions_owner(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        _, stored = self._stored_validation_definition(owner)
        checkpoint = Checkpoint(name="my_cp", validation_definitions=[stored])
        gx.get_context(mode="ephemeral")

        assert checkpoint._resolve_context() == ResolvedContext(context=owner, bound=True)

    @pytest.mark.unit
    def test_bound_miss_keeps_todays_class_and_message(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")
        _, stored = self._stored_validation_definition(context)
        checkpoint = Checkpoint(name="my_cp", validation_definitions=[stored], id="not-in-store")
        assert checkpoint._resolve_context().bound is True

        diagnostics = checkpoint.is_fresh()

        # The literal is what the unfixed tree raises for the same scenario.
        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                CheckpointNotFoundError,
                "Checkpoint 'my_cp' not found. Please check the name and try again.",
            )
        ]

    @pytest.mark.unit
    def test_unbound_miss_names_the_context_consulted(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")
        batch_definition, stored = self._stored_validation_definition(context)
        ownerless = ValidationDefinition(
            name=stored.name,
            data=_ownerless_copy_of(batch_definition),
            suite=stored.suite,
            id=stored.id,
        )
        checkpoint = Checkpoint(name="my_cp", validation_definitions=[ownerless], id="not-in-store")
        assert checkpoint._resolve_context().bound is False

        diagnostics = checkpoint.is_fresh()

        assert [(type(e), str(e)) for e in diagnostics.errors] == [
            (
                CheckpointNotFoundError,
                "Checkpoint 'my_cp' not found. Please check the name and try again."
                " " + EPHEMERAL_NOTE,
            )
        ]

    @pytest.mark.unit
    def test_direct_parse_miss_raises_validation_error_naming_the_context(
        self, restore_current_context: None
    ) -> None:
        gx.get_context(mode="ephemeral")
        serialized = {
            "name": "my_cp",
            "validation_definitions": [{"name": "i_do_not_exist", "id": "an-id"}],
            "actions": [],
            "id": "a-checkpoint-id",
        }

        with pytest.raises(ValidationError) as exc_info:
            Checkpoint.parse_obj(serialized)

        assert (
            "Unable to retrieve validation definition name='i_do_not_exist' id='an-id' from store. "
            + CONSULTED_EPHEMERAL_NOTE
            in str(exc_info.value)
        )

    @pytest.mark.unit
    def test_deserializer_miss_through_a_handed_in_store_keeps_the_plain_text(
        self, restore_current_context: None
    ) -> None:
        # The store belongs to another context than the current one, so the lookup did not go
        # through the current context and the text must not say it did.
        owner = gx.get_context(mode="ephemeral")
        other = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is other
        bundle = _IdentifierBundle(name="i_do_not_exist", id="an-id")

        with pytest.raises(ValueError) as exc_info:
            Checkpoint._deserialize_identifier_bundles_to_validation_definitions(
                identifier_bundles=[bundle], store=owner.validation_definition_store
            )

        assert str(exc_info.value) == (
            "Unable to retrieve validation definition name='i_do_not_exist' id='an-id' from store"
        )

    @pytest.mark.unit
    def test_load_from_a_non_current_contexts_store_does_not_claim_the_object_is_unbound(
        self, restore_current_context: None
    ) -> None:
        owner = gx.get_context(mode="ephemeral")
        _, stored = self._stored_validation_definition(owner)
        owner.checkpoints.add(Checkpoint(name="my_cp", validation_definitions=[stored]))
        # The owner's own store resolves the record through the owner, so the miss must be the
        # owner's: remove the validation definition from it.
        owner.validation_definitions.delete("stored_vd")
        gx.get_context(mode="ephemeral")

        with pytest.raises(ValidationError) as exc_info:
            owner.checkpoints.get("my_cp")

        message = str(exc_info.value)
        assert (
            f"Unable to retrieve validation definition name='stored_vd' id='{stored.id}'"
            " from store (type=value_error)" in message
        )
        assert CONSULTED_EPHEMERAL_NOTE not in message
        assert "not bound" not in message
