"""A store-loaded object resolves its references through the Data Context that holds it (#12209).

A Validation Definition read back from a Data Context's store names a suite and a batch
definition, and a Checkpoint names Validation Definitions. They are looked up in that same
context, whatever context is current, and the object that comes back belongs to it. An object
built directly, outside any store, still resolves through the current context.
"""

from __future__ import annotations

import json
import logging
import pathlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.factory.checkpoint_factory import CheckpointFactory
from great_expectations.core.factory.validation_definition_factory import (
    ValidationDefinitionFactory,
)
from great_expectations.core.owner_resolution import owner_from_batch_definition
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.data_context.data_context.file_data_context import FileDataContext
from great_expectations.data_context.store import CheckpointStore, ValidationDefinitionStore
from great_expectations.exceptions import DataContextRequiredError

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# The wording a miss carries when the lookup went through the current context.
AMBIENT_NOTE = "The lookup went through the current"
CURRENT_CONTEXT_WORDING = "current Data Context"


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
def c1(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path, restore_current_context: None
) -> AbstractDataContext:
    """The first context, of the kind the test is parametrized with. It is current until C2 is."""
    if request.param == "file":
        return gx.get_context(mode="file", project_root_dir=tmp_path / "one")
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def make_c2(tmp_path: pathlib.Path, c1: AbstractDataContext) -> Callable[[], AbstractDataContext]:
    """Create a second context of the same kind, which makes it the current context.

    A test calls it after building its resources in C1, and the assertions run with C2 current.
    """

    def _make() -> AbstractDataContext:
        c2: AbstractDataContext
        if isinstance(c1, FileDataContext):
            c2 = gx.get_context(mode="file", project_root_dir=tmp_path / "two")
        else:
            c2 = gx.get_context(mode="ephemeral")
        assert c2 is not c1
        assert project_manager.get_current_project() is c2
        return c2

    return _make


# Each test runs against an ephemeral context and a file-backed one.
both_context_kinds = pytest.mark.parametrize(
    "c1",
    [
        pytest.param("ephemeral", id="ephemeral", marks=pytest.mark.unit),
        pytest.param("file", id="file", marks=pytest.mark.filesystem),
    ],
    indirect=True,
)


def _add_validation_definition(
    context: AbstractDataContext, name: str
) -> tuple[ValidationDefinition, BatchDefinition[Any]]:
    """Add a datasource, asset, batch definition and suite to `context`, then a definition."""
    datasource = context.data_sources.add_pandas(f"{name}_datasource")
    asset = datasource.add_dataframe_asset(f"{name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{name}_batch_definition")
    suite = context.suites.add(
        ExpectationSuite(
            name=f"{name}_suite",
            expectations=[gxe.ExpectColumnValuesToNotBeNull(column="a")],
        )
    )
    added = context.validation_definitions.add(
        ValidationDefinition(name=name, data=batch_definition, suite=suite)
    )
    return added, batch_definition


def _remove_batch_definition(
    context: AbstractDataContext, name: str, batch_definition: BatchDefinition[Any]
) -> None:
    """Make the stored definition `name` name a batch definition its context no longer has."""
    asset = context.data_sources.get(f"{name}_datasource").get_asset(f"{name}_asset")
    asset.delete_batch_definition(batch_definition.name)


def _break_reference(context: AbstractDataContext, name: str, reference: str) -> None:
    """Remove the datasource, asset or batch definition the stored definition `name` names."""
    datasource = context.data_sources.get(f"{name}_datasource")
    if reference == "datasource":
        context.data_sources.delete(datasource.name)
    elif reference == "asset":
        datasource.delete_asset(f"{name}_asset")
    else:
        datasource.get_asset(f"{name}_asset").delete_batch_definition(f"{name}_batch_definition")


# What each kind of miss in the record's own context says, with no note about another context.
PLAIN_MISS_TEXT = {
    "datasource": "Could not find datasource named 'vd_datasource'.",
    "asset": "Could not find asset named 'vd_asset' within 'vd_datasource' datasource.",
    "batch_definition": (
        "Could not find batch definition named 'vd_batch_definition' within 'vd_asset' asset "
        "and 'vd_datasource' datasource."
    ),
}
each_reference = pytest.mark.parametrize("reference", list(PLAIN_MISS_TEXT))


def _record_path(context: AbstractDataContext, folder: str, name: str) -> pathlib.Path:
    assert context.root_directory is not None
    return pathlib.Path(context.root_directory) / folder / f"{name}.json"


class TestValidationDefinitionStoreResolvesThroughItsContext:
    @both_context_kinds
    def test_get_returns_a_definition_whose_chain_reaches_its_context(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_validation_definition(c1, "vd")
        make_c2()

        loaded = c1.validation_definitions.get("vd")

        assert owner_from_batch_definition(loaded.data) is c1
        assert loaded.suite._owner is c1

    @both_context_kinds
    def test_get_returns_the_record_the_context_holds(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        added, _ = _add_validation_definition(c1, "vd")
        make_c2()

        loaded = c1.validation_definitions.get("vd")

        assert loaded.name == "vd"
        assert loaded.data.name == "vd_batch_definition"
        assert loaded.suite.name == "vd_suite"
        assert loaded == added

    @both_context_kinds
    def test_all_returns_every_stored_definition(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_validation_definition(c1, "first")
        _add_validation_definition(c1, "second")
        make_c2()

        definitions = list(c1.validation_definitions.all())

        assert len(definitions) == 2
        assert {vd.name for vd in definitions} == {"first", "second"}
        for definition in definitions:
            assert owner_from_batch_definition(definition.data) is c1
            assert definition.suite._owner is c1

    @both_context_kinds
    @each_reference
    def test_a_miss_in_its_context_raises_the_error_the_validator_raises(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        reference: str,
    ) -> None:
        _add_validation_definition(c1, "vd")
        _break_reference(c1, "vd", reference)
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("vd")

        assert PLAIN_MISS_TEXT[reference] in str(exc_info.value)
        assert exc_info.value.model is ValidationDefinition
        assert [error["loc"] for error in exc_info.value.errors()] == [("data",)]

    @both_context_kinds
    @each_reference
    def test_a_miss_in_its_context_carries_no_current_context_note(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        reference: str,
    ) -> None:
        # The lookup went through the context that holds the record, not the current one, so a
        # note that it did would be false.
        _add_validation_definition(c1, "vd")
        _break_reference(c1, "vd", reference)
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("vd")

        message = str(exc_info.value)
        assert message.endswith(f"{PLAIN_MISS_TEXT[reference]} (type=value_error)")
        assert AMBIENT_NOTE not in message
        assert CURRENT_CONTEXT_WORDING not in message

    @both_context_kinds
    def test_a_double_miss_reports_both_fields_as_the_validators_do(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        added, batch_definition = _add_validation_definition(c1, "vd")
        _remove_batch_definition(c1, "vd", batch_definition)
        c1.suites.delete("vd_suite")
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("vd")

        assert [error["loc"] for error in exc_info.value.errors()] == [("data",), ("suite",)]
        assert str(exc_info.value) == (
            "2 validation errors for ValidationDefinition\n"
            "data\n"
            "  " + PLAIN_MISS_TEXT["batch_definition"] + " (type=value_error)\n"
            "suite\n"
            f"  Could not find suite with name: vd_suite and id: {added.suite.id} "
            "(type=value_error)"
        )

    @both_context_kinds
    def test_a_suite_miss_in_its_context_carries_the_plain_message(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        added, _ = _add_validation_definition(c1, "vd")
        c1.suites.delete("vd_suite")
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("vd")

        message = str(exc_info.value)
        assert f"Could not find suite with name: vd_suite and id: {added.suite.id}" in message
        assert AMBIENT_NOTE not in message
        assert CURRENT_CONTEXT_WORDING not in message
        assert [error["loc"] for error in exc_info.value.errors()] == [("suite",)]

    @both_context_kinds
    def test_all_skips_an_unparseable_record_with_the_existing_warning(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _add_validation_definition(c1, "kept")
        _, batch_definition = _add_validation_definition(c1, "dropped")
        _remove_batch_definition(c1, "dropped", batch_definition)
        make_c2()

        with caplog.at_level(logging.WARNING):
            definitions = list(c1.validation_definitions.all())

        assert [vd.name for vd in definitions] == ["kept"]
        assert "Skipping Bad Configs" in caplog.text
        assert "dropped" in caplog.text


class TestARecordWithSeveralFailuresReportsThemAllInFieldOrder:
    """The text below is what parsing such a record raised before the store resolved it itself."""

    @pytest.fixture
    def context(self, tmp_path: pathlib.Path, restore_current_context: None) -> AbstractDataContext:
        context = gx.get_context(mode="file", project_root_dir=tmp_path / "only")
        batch_definition = (
            context.data_sources.add_pandas("d")
            .add_dataframe_asset("a")
            .add_batch_definition_whole_dataframe("b")
        )
        suite = context.suites.add(ExpectationSuite(name="s"))
        context.validation_definitions.add(
            ValidationDefinition(name="vd", data=batch_definition, suite=suite)
        )
        return context

    @pytest.fixture
    def record(self, context: AbstractDataContext) -> dict[str, Any]:
        store = context.validation_definition_store
        record: dict[str, Any] = json.loads(
            store.serialize(context.validation_definitions.get("vd"))
        )
        return record

    @staticmethod
    def _deserialize(context: AbstractDataContext, record: dict[str, Any]) -> ValidationError:
        with pytest.raises(ValidationError) as exc_info:
            context.validation_definition_store.deserialize(json.dumps(record))
        return exc_info.value

    @pytest.mark.filesystem
    def test_a_missing_name_and_suite_and_an_extra_key(
        self, context: AbstractDataContext, record: dict[str, Any]
    ) -> None:
        del record["name"]
        record["suite"] = {"name": "gone", "id": "x"}
        record["zzz"] = 1

        error = self._deserialize(context, record)

        assert error.model is ValidationDefinition
        assert [(e["loc"], e["msg"]) for e in error.errors()] == [
            (("name",), "field required"),
            (("suite",), "Could not find suite with name: gone and id: x"),
            (("zzz",), "extra fields not permitted"),
        ]
        assert str(error) == (
            "3 validation errors for ValidationDefinition\n"
            "name\n"
            "  field required (type=value_error.missing)\n"
            "suite\n"
            "  Could not find suite with name: gone and id: x (type=value_error)\n"
            "zzz\n"
            "  extra fields not permitted (type=value_error.extra)"
        )

    @pytest.mark.filesystem
    def test_a_missing_name_and_batch_definition(
        self, context: AbstractDataContext, record: dict[str, Any]
    ) -> None:
        del record["name"]
        record["data"]["batch_definition"] = {"name": "gone", "id": "x"}

        error = self._deserialize(context, record)

        assert [e["loc"] for e in error.errors()] == [("name",), ("data",)]
        assert str(error) == (
            "2 validation errors for ValidationDefinition\n"
            "name\n"
            "  field required (type=value_error.missing)\n"
            "data\n"
            "  Could not find batch definition named 'gone' within 'a' asset and 'd' "
            "datasource. (type=value_error)"
        )

    @pytest.mark.filesystem
    def test_both_references_missing_with_a_bad_id_after_them(
        self, context: AbstractDataContext, record: dict[str, Any]
    ) -> None:
        record["data"]["batch_definition"] = {"name": "gone", "id": "x"}
        record["suite"] = {"name": "gone", "id": "x"}
        record["id"] = [1]

        error = self._deserialize(context, record)

        assert [e["loc"] for e in error.errors()] == [("data",), ("suite",), ("id",)]


class TestCollectingTheOtherErrorsDoesNotLookThroughTheCurrentContext:
    @both_context_kinds
    def test_a_validation_definition_that_failed_to_resolve_is_not_looked_up_again(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        mocker: MockerFixture,
    ) -> None:
        _, batch_definition = _add_validation_definition(c1, "vd")
        _remove_batch_definition(c1, "vd", batch_definition)
        c1.suites.delete("vd_suite")
        make_c2()
        suites = mocker.spy(project_manager, "get_expectations_store")
        data_sources = mocker.spy(project_manager, "get_datasources")

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("vd")

        assert [e["loc"] for e in exc_info.value.errors()] == [("data",), ("suite",)]
        suites.assert_not_called()
        data_sources.assert_not_called()

    @both_context_kinds
    def test_a_checkpoint_whose_definitions_failed_to_resolve_is_not_looked_up_again(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        mocker: MockerFixture,
    ) -> None:
        _add_checkpoint(c1, "cp", ("dropped",))
        c1.validation_definitions.delete("dropped")
        make_c2()
        definitions = mocker.spy(project_manager, "get_validation_definition_store")

        with pytest.raises(ValidationError) as exc_info:
            c1.checkpoints.get("cp")

        assert [e["loc"] for e in exc_info.value.errors()] == [("validation_definitions",)]
        definitions.assert_not_called()


class TestAnUnreadableRecordIsSkippedNotRaised:
    @pytest.mark.filesystem
    @pytest.mark.parametrize("content", ["[1]", "null", '"text"', "7"])
    def test_a_record_that_is_not_an_object_is_skipped(
        self,
        tmp_path: pathlib.Path,
        restore_current_context: None,
        caplog: pytest.LogCaptureFixture,
        content: str,
    ) -> None:
        c1 = gx.get_context(mode="file", project_root_dir=tmp_path / "one")
        _add_validation_definition(c1, "kept")
        _add_validation_definition(c1, "broken")
        _record_path(c1, "validation_definitions", "broken").write_text(content)
        gx.get_context(mode="file", project_root_dir=tmp_path / "two")

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("broken")
        with caplog.at_level(logging.WARNING):
            definitions = list(c1.validation_definitions.all())

        assert exc_info.value.model is ValidationDefinition
        assert [vd.name for vd in definitions] == ["kept"]
        assert "Skipping Bad Configs" in caplog.text

    @pytest.mark.filesystem
    def test_a_record_whose_suite_file_is_empty_is_skipped(
        self,
        tmp_path: pathlib.Path,
        restore_current_context: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        c1 = gx.get_context(mode="file", project_root_dir=tmp_path / "one")
        _add_validation_definition(c1, "kept")
        _add_validation_definition(c1, "broken")
        _record_path(c1, "expectations", "broken_suite").write_text("")
        gx.get_context(mode="file", project_root_dir=tmp_path / "two")

        with pytest.raises(ValidationError) as exc_info:
            c1.validation_definitions.get("broken")
        with caplog.at_level(logging.WARNING):
            definitions = list(c1.validation_definitions.all())

        assert [error["loc"] for error in exc_info.value.errors()] == [("suite",)]
        assert [vd.name for vd in definitions] == ["kept"]
        assert "Skipping Bad Configs" in caplog.text


class TestACloudRecordThatIsNotAnObject:
    @pytest.mark.unit
    def test_it_raises_the_validation_error_pydantic_raises(
        self, restore_current_context: None, mocker: MockerFixture
    ) -> None:
        c1 = gx.get_context(mode="ephemeral")
        store = c1.validation_definition_store
        mocker.patch.object(
            type(store), "cloud_mode", new_callable=mocker.PropertyMock
        ).return_value = True
        assert store.cloud_mode is True

        with pytest.raises(ValidationError) as exc_info:
            store.deserialize([1])
        with pytest.raises(ValidationError) as expected:
            ValidationDefinition.parse_obj([1])

        assert exc_info.value.model is ValidationDefinition
        assert exc_info.value.errors() == expected.value.errors()
        assert [error["loc"] for error in exc_info.value.errors()] == [("__root__",)]


class TestObjectsBuiltOutsideAStoreResolveThroughTheCurrentContext:
    @pytest.mark.unit
    def test_a_direct_miss_names_the_current_context(self, restore_current_context: None) -> None:
        c1 = gx.get_context(mode="ephemeral")
        added, batch_definition = _add_validation_definition(c1, "vd")
        _remove_batch_definition(c1, "vd", batch_definition)
        c2 = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is c2
        record = {
            "name": "vd",
            "data": {
                "datasource": {"name": "vd_datasource", "id": None},
                "asset": {"name": "vd_asset", "id": None},
                "batch_definition": {"name": "vd_batch_definition", "id": None},
            },
            "suite": {"name": "vd_suite", "id": added.suite.id},
        }

        with pytest.raises(ValidationError) as exc_info:
            ValidationDefinition.parse_obj(record)

        # C2 is current and holds nothing named vd_datasource: the ambient lookup is the one
        # that misses, and it says so.
        assert "Could not find datasource named 'vd_datasource'." in str(exc_info.value)
        assert AMBIENT_NOTE in str(exc_info.value)

    @pytest.mark.unit
    def test_parse_obj_still_resolves_through_a_mock_current_context(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        batch_definition = BatchDefinition(name="vd_batch_definition")
        context = mocker.Mock(spec=AbstractDataContext)
        context.expectations_store.get.return_value = {"name": "vd_suite", "id": "suite-id"}
        datasource = mocker.Mock()
        datasource.get_asset.return_value.get_batch_definition.return_value = batch_definition
        context.data_sources.all.return_value = {"vd_datasource": datasource}
        monkeypatch.setattr(project_manager, "_ProjectManager__project", context)

        loaded = ValidationDefinition.parse_obj(
            {
                "name": "vd",
                "data": {
                    "datasource": {"name": "vd_datasource", "id": None},
                    "asset": {"name": "vd_asset", "id": None},
                    "batch_definition": {"name": "vd_batch_definition", "id": None},
                },
                "suite": {"name": "vd_suite", "id": "suite-id"},
            }
        )

        assert loaded.data.name == "vd_batch_definition"
        datasource.get_asset.return_value.get_batch_definition.assert_called_once_with(
            "vd_batch_definition"
        )
        assert loaded.suite.name == "vd_suite"
        context.expectations_store.get.assert_called_once()
        context.data_sources.all.assert_called_once_with()

    @pytest.mark.unit
    def test_a_store_built_directly_has_no_context(self) -> None:
        store = ValidationDefinitionStore()

        assert store.data_context is None

    @pytest.mark.unit
    def test_a_store_built_by_a_context_has_that_context(
        self, restore_current_context: None
    ) -> None:
        c1 = gx.get_context(mode="ephemeral")

        assert c1.validation_definition_store.data_context is c1


def _add_checkpoint(
    context: AbstractDataContext, name: str, definition_names: tuple[str, ...] = ("vd",)
) -> Checkpoint:
    """Add the named validation definitions to `context`, then a checkpoint that runs them."""
    definitions = [_add_validation_definition(context, n)[0] for n in definition_names]
    return context.checkpoints.add(Checkpoint(name=name, validation_definitions=definitions))


def _assert_bound_to(checkpoint: Checkpoint, context: AbstractDataContext, count: int) -> None:
    assert len(checkpoint.validation_definitions) == count
    for definition in checkpoint.validation_definitions:
        assert owner_from_batch_definition(definition.data) is context
        assert definition.suite._owner is context


def _checkpoint_record(context: AbstractDataContext, name: str) -> dict[str, Any]:
    record: dict[str, Any] = json.loads(
        context.checkpoint_store.serialize(context.checkpoints.get(name))
    )
    return record


MISSING_DEFINITION_TEXT = (
    "Unable to retrieve validation definition name='gone' id='nope' from store"
)
A_DEFINITION_THAT_IS_NOT_STORED = {"name": "gone", "id": "nope"}


class TestCheckpointStoreResolvesThroughItsContext:
    @both_context_kinds
    def test_get_returns_a_checkpoint_whose_chain_reaches_its_context(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_checkpoint(c1, "cp", ("first", "second"))
        make_c2()

        loaded = c1.checkpoints.get("cp")

        _assert_bound_to(loaded, c1, count=2)
        assert [vd.name for vd in loaded.validation_definitions] == ["first", "second"]

    @both_context_kinds
    def test_all_returns_every_stored_checkpoint(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_checkpoint(c1, "first_cp", ("first",))
        _add_checkpoint(c1, "second_cp", ("second",))
        make_c2()

        checkpoints = list(c1.checkpoints.all())

        assert len(checkpoints) == 2
        assert {cp.name for cp in checkpoints} == {"first_cp", "second_cp"}
        for checkpoint in checkpoints:
            _assert_bound_to(checkpoint, c1, count=1)

    @both_context_kinds
    def test_add_returns_a_checkpoint_bound_to_its_context(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        definitions = [_add_validation_definition(c1, n)[0] for n in ("first", "second")]
        make_c2()

        added = c1.checkpoints.add(Checkpoint(name="cp", validation_definitions=definitions))

        _assert_bound_to(added, c1, count=2)

    @both_context_kinds
    def test_a_missing_definition_raises_the_plain_error_of_the_validator(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_checkpoint(c1, "cp", ("kept", "dropped"))
        c1.validation_definitions.delete("dropped")
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.checkpoints.get("cp")

        # The lookup went through the context that holds the record, not the current one, so a
        # note that it did would be false.
        assert exc_info.value.model is Checkpoint
        assert [error["loc"] for error in exc_info.value.errors()] == [("validation_definitions",)]
        message = str(exc_info.value)
        assert message.startswith(
            "1 validation error for Checkpoint\nvalidation_definitions\n"
            "  Unable to retrieve validation definition name='dropped' id='"
        )
        assert message.endswith("' from store (type=value_error)")
        assert AMBIENT_NOTE not in message
        assert CURRENT_CONTEXT_WORDING not in message

    @both_context_kinds
    def test_a_definition_that_cannot_resolve_its_own_references_raises_its_error(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        _add_checkpoint(c1, "vd")
        _break_reference(c1, "vd", "batch_definition")
        make_c2()

        with pytest.raises(ValidationError) as exc_info:
            c1.checkpoints.get("vd")

        assert exc_info.value.model is Checkpoint
        assert [error["loc"] for error in exc_info.value.errors()] == [
            ("validation_definitions", "data")
        ]
        assert PLAIN_MISS_TEXT["batch_definition"] in str(exc_info.value)
        assert AMBIENT_NOTE not in str(exc_info.value)
        assert CURRENT_CONTEXT_WORDING not in str(exc_info.value)

    @both_context_kinds
    def test_all_skips_an_unresolvable_checkpoint_with_the_existing_warning(
        self,
        c1: AbstractDataContext,
        make_c2: Callable[[], AbstractDataContext],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _add_checkpoint(c1, "kept_cp", ("kept",))
        _add_checkpoint(c1, "dropped_cp", ("dropped",))
        c1.validation_definitions.delete("dropped")
        make_c2()

        with caplog.at_level(logging.WARNING):
            checkpoints = list(c1.checkpoints.all())

        assert [cp.name for cp in checkpoints] == ["kept_cp"]
        assert "Skipping Bad Configs" in caplog.text
        assert "dropped_cp" in caplog.text


class TestACheckpointRecordIsParsedAsPydanticParsesIt:
    """In a program with one context, the store raises what parsing the record raises."""

    @pytest.fixture
    def only_context(
        self, tmp_path: pathlib.Path, restore_current_context: None
    ) -> AbstractDataContext:
        context = gx.get_context(mode="file", project_root_dir=tmp_path / "only")
        _add_checkpoint(context, "cp")
        return context

    @staticmethod
    def _errors_of(parse: Callable[[], object]) -> tuple[type, list[Any], str]:
        with pytest.raises(ValidationError) as exc_info:
            parse()
        return exc_info.value.model, exc_info.value.errors(), str(exc_info.value)

    @pytest.mark.filesystem
    @pytest.mark.parametrize(
        ("change", "locations"),
        [
            pytest.param(
                {"validation_definitions": [{"name": "vd", "id": "x"}, 5]},
                [("validation_definitions",)],
                id="an entry that is not a mapping",
            ),
            pytest.param(
                {"validation_definitions": [{"id": "x"}]},
                [("validation_definitions", "name")],
                id="an entry without a name",
            ),
            pytest.param(
                {"extra": 1, "result_format": "BOGUS"},
                [("result_format",), ("result_format",), ("result_format",), ("extra",)],
                id="other fields",
            ),
        ],
    )
    def test_a_record_that_fails_without_a_missing_definition_matches_pydantic(
        self,
        only_context: AbstractDataContext,
        change: dict[str, Any],
        locations: list[tuple[str, ...]],
    ) -> None:
        record = {**_checkpoint_record(only_context, "cp"), **change}
        store = only_context.checkpoint_store

        from_store = self._errors_of(lambda: store.deserialize(json.dumps(record)))
        from_pydantic = self._errors_of(lambda: Checkpoint.parse_obj(record))

        assert from_store == from_pydantic
        assert [error["loc"] for error in from_store[1]] == locations

    @pytest.mark.filesystem
    @pytest.mark.parametrize(
        ("change", "locations"),
        [
            pytest.param({}, [("validation_definitions",)], id="alone"),
            pytest.param(
                {"name": None},
                [("name",), ("validation_definitions",)],
                id="after a missing name",
            ),
            pytest.param(
                {"id": [1], "extra": 1},
                [("validation_definitions",), ("id",), ("extra",)],
                id="before a bad id and an extra field",
            ),
        ],
    )
    def test_a_missing_definition_is_reported_in_field_order_with_the_other_failures(
        self,
        only_context: AbstractDataContext,
        change: dict[str, Any],
        locations: list[tuple[str, ...]],
    ) -> None:
        record = {
            **_checkpoint_record(only_context, "cp"),
            **change,
            "validation_definitions": [A_DEFINITION_THAT_IS_NOT_STORED],
        }
        record = {k: v for k, v in record.items() if v is not None}
        store = only_context.checkpoint_store

        model, errors, message = self._errors_of(lambda: store.deserialize(json.dumps(record)))
        _, expected_errors, expected_message = self._errors_of(lambda: Checkpoint.parse_obj(record))

        assert model is Checkpoint
        assert [error["loc"] for error in errors] == locations
        # The current context is the one holding the record, so pydantic's lookup misses too; the
        # only difference is the note it appends to say the lookup was a current-context one.
        assert [e["loc"] for e in expected_errors] == locations
        assert MISSING_DEFINITION_TEXT + " (type=value_error)" in message
        assert AMBIENT_NOTE in expected_message
        assert AMBIENT_NOTE not in message

    @pytest.mark.filesystem
    @pytest.mark.parametrize("content", ["[1]", "null", '"text"', "7", "not json", "{}", "[]"])
    def test_a_record_that_is_not_a_checkpoint_matches_pydantic(
        self, only_context: AbstractDataContext, content: str
    ) -> None:
        store = only_context.checkpoint_store

        from_store = self._errors_of(lambda: store.deserialize(content))
        from_pydantic = self._errors_of(lambda: Checkpoint.parse_raw(content))

        assert from_store == from_pydantic

    @pytest.mark.filesystem
    @pytest.mark.parametrize("content", ["[1]", "null", '"text"', "7"])
    def test_all_skips_a_record_that_is_not_an_object(
        self,
        tmp_path: pathlib.Path,
        restore_current_context: None,
        caplog: pytest.LogCaptureFixture,
        content: str,
    ) -> None:
        c1 = gx.get_context(mode="file", project_root_dir=tmp_path / "one")
        _add_checkpoint(c1, "kept_cp", ("kept",))
        _add_checkpoint(c1, "broken_cp", ("broken",))
        _record_path(c1, "checkpoints", "broken_cp").write_text(content)
        gx.get_context(mode="file", project_root_dir=tmp_path / "two")

        with pytest.raises(ValidationError) as exc_info:
            c1.checkpoints.get("broken_cp")
        with caplog.at_level(logging.WARNING):
            checkpoints = list(c1.checkpoints.all())

        assert exc_info.value.model is Checkpoint
        assert [cp.name for cp in checkpoints] == ["kept_cp"]
        assert "Skipping Bad Configs" in caplog.text


class TestACloudCheckpointRecordThatIsNotAnObject:
    @pytest.mark.unit
    def test_it_raises_the_validation_error_pydantic_raises(
        self, restore_current_context: None, mocker: MockerFixture
    ) -> None:
        c1 = gx.get_context(mode="ephemeral")
        store = c1.checkpoint_store
        mocker.patch.object(
            type(store), "cloud_mode", new_callable=mocker.PropertyMock
        ).return_value = True
        assert store.cloud_mode is True

        with pytest.raises(ValidationError) as exc_info:
            store.deserialize([1])
        with pytest.raises(ValidationError) as expected:
            Checkpoint.parse_obj([1])

        assert exc_info.value.model is Checkpoint
        assert exc_info.value.errors() == expected.value.errors()
        assert [error["loc"] for error in exc_info.value.errors()] == [("__root__",)]


class TestCheckpointsBuiltOutsideAStoreResolveThroughTheCurrentContext:
    @pytest.mark.unit
    def test_parse_obj_still_resolves_through_a_mock_current_context(
        self, mocker: MockerFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        definition = ValidationDefinition(
            name="vd",
            data=BatchDefinition(name="batch_definition"),
            suite=ExpectationSuite(name="s"),
        )
        context = mocker.Mock(spec=AbstractDataContext)
        context.validation_definition_store.get.return_value = definition
        monkeypatch.setattr(project_manager, "_ProjectManager__project", context)

        loaded = Checkpoint.parse_obj(
            {"name": "cp", "validation_definitions": [{"name": "vd", "id": "vd-id"}]}
        )

        assert [vd.name for vd in loaded.validation_definitions] == ["vd"]
        context.validation_definition_store.get_key.assert_called_once_with(name="vd", id="vd-id")
        context.validation_definition_store.get.assert_called_once()

    @pytest.mark.unit
    def test_a_direct_miss_names_the_current_context(self, restore_current_context: None) -> None:
        context = gx.get_context(mode="ephemeral")

        with pytest.raises(ValidationError) as exc_info:
            Checkpoint.parse_obj(
                {"name": "cp", "validation_definitions": [A_DEFINITION_THAT_IS_NOT_STORED]}
            )

        assert MISSING_DEFINITION_TEXT in str(exc_info.value)
        assert AMBIENT_NOTE in str(exc_info.value)
        assert project_manager.get_current_project() is context

    @pytest.mark.unit
    def test_a_store_built_directly_has_no_context(self) -> None:
        store = CheckpointStore()

        assert store.data_context is None

    @pytest.mark.unit
    def test_a_store_built_by_a_context_has_that_context(
        self, restore_current_context: None
    ) -> None:
        c1 = gx.get_context(mode="ephemeral")

        assert c1.checkpoint_store.data_context is c1


def _replace_suite_with_a_wider_one(definition: ValidationDefinition) -> None:
    """Give `definition` a suite of the same name with one more expectation than is stored."""
    definition.suite = ExpectationSuite(
        name=definition.suite.name,
        expectations=[
            gxe.ExpectColumnValuesToNotBeNull(column="a"),
            gxe.ExpectColumnValuesToBeInSet(column="a", value_set=[1, 2]),
        ],
    )


class TestFactoriesStayInsideTheContextOfTheirStore:
    """A factory writes the objects it adds through the context that owns its store.

    The suite behind a Validation Definition and the Validation Definitions behind a Checkpoint
    are written by the factory that belongs to the same context as the store being written to,
    whichever context is current.
    """

    @both_context_kinds
    def test_a_validation_definition_persists_its_suite_in_its_own_context(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        definition, _ = _add_validation_definition(c1, "vd")
        _replace_suite_with_a_wider_one(definition)
        assert len(c1.suites.get("vd_suite").expectations) == 1
        c2 = make_c2()

        c1.validation_definitions.add_or_update(definition)

        assert len(c1.suites.get("vd_suite").expectations) == 2
        assert [suite.name for suite in c2.suites.all()] == []
        assert [vd.name for vd in c2.validation_definitions.all()] == []

    @both_context_kinds
    def test_a_checkpoint_persists_its_validation_definitions_in_its_own_context(
        self, c1: AbstractDataContext, make_c2: Callable[[], AbstractDataContext]
    ) -> None:
        checkpoint = _add_checkpoint(c1, "cp")
        _replace_suite_with_a_wider_one(checkpoint.validation_definitions[0])
        assert len(c1.suites.get("vd_suite").expectations) == 1
        c2 = make_c2()

        c1.checkpoints.add_or_update(checkpoint)

        assert len(c1.suites.get("vd_suite").expectations) == 2
        assert [vd.name for vd in c1.validation_definitions.all()] == ["vd"]
        assert [suite.name for suite in c2.suites.all()] == []
        assert [vd.name for vd in c2.validation_definitions.all()] == []
        assert [cp.name for cp in c2.checkpoints.all()] == []

    @pytest.mark.unit
    def test_a_validation_definition_factory_with_no_context_uses_the_current_one(
        self, mocker: MockerFixture, restore_current_context: None
    ) -> None:
        other = gx.get_context(mode="ephemeral")
        current = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is current
        definition, _ = _add_validation_definition(current, "vd")
        factory = ValidationDefinitionFactory(store=ValidationDefinitionStore())
        assert factory._store.data_context is None
        other_spy = mocker.spy(other.suites, "add_or_update")
        current_spy = mocker.spy(current.suites, "add_or_update")

        factory.add_or_update(definition)

        current_spy.assert_called_once()
        other_spy.assert_not_called()

    @pytest.mark.unit
    def test_a_checkpoint_factory_with_no_context_uses_the_current_one(
        self, mocker: MockerFixture, restore_current_context: None
    ) -> None:
        other = gx.get_context(mode="ephemeral")
        current = gx.get_context(mode="ephemeral")
        assert project_manager.get_current_project() is current
        definition, _ = _add_validation_definition(current, "vd")
        factory = CheckpointFactory(store=CheckpointStore())
        assert factory._store.data_context is None
        other_spy = mocker.spy(other.validation_definitions, "add_or_update")
        current_spy = mocker.spy(current.validation_definitions, "add_or_update")

        factory.add_or_update(Checkpoint(name="cp", validation_definitions=[definition]))

        current_spy.assert_called_once()
        other_spy.assert_not_called()


class TestObjectsLoadedFromAReopenedContextRunThroughIt:
    """A file-backed context reopened from disk owns what it loads, whichever context is current."""

    @pytest.mark.filesystem
    def test_loaded_definition_and_checkpoint_run_and_write_to_the_reopened_context(
        self, tmp_path: pathlib.Path, restore_current_context: None
    ) -> None:
        project = tmp_path / "project"
        original = gx.get_context(mode="file", project_root_dir=project)
        added, _ = _add_validation_definition(original, "vd")
        original.checkpoints.add(Checkpoint(name="cp", validation_definitions=[added]))

        # Reopen the project, then create a second context so that it, not the reopened one,
        # is current while the loaded objects run.
        reopened = gx.get_context(mode="file", project_root_dir=project)
        c2 = gx.get_context(mode="ephemeral")
        assert reopened is not original
        assert project_manager.get_current_project() is c2

        definition = reopened.validation_definitions.get("vd")
        checkpoint = reopened.checkpoints.get("cp")
        assert definition.suite._owner is reopened
        assert owner_from_batch_definition(definition.data) is reopened

        batch_parameters = {"dataframe": pd.DataFrame({"a": [1, 2, 3]})}
        reopened_results_before = len(reopened.validation_results_store.list_keys())
        c2_results_before = len(c2.validation_results_store.list_keys())

        assert definition.run(batch_parameters=batch_parameters).success is True
        assert len(reopened.validation_results_store.list_keys()) == reopened_results_before + 1
        assert checkpoint.run(batch_parameters=batch_parameters).success is True
        assert len(reopened.validation_results_store.list_keys()) == reopened_results_before + 2
        assert len(c2.validation_results_store.list_keys()) == c2_results_before
