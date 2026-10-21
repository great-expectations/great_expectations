"""An object resolves through the Data Context that owns it (#12209).

With more than one Data Context in a process, an object keeps working against the context it
was created in after another context becomes current. An object that belongs to no context
resolves through the current one.
"""

from __future__ import annotations

import copy
import gc
import pathlib
import threading
import weakref
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.context_factory import (
    project_manager,
    set_context,
)
from great_expectations.datasource.fluent import PandasFilesystemDatasource
from great_expectations.exceptions import DataContextRequiredError
from great_expectations.exceptions.resource_freshness import (
    BatchDefinitionNotAddedError,
    CheckpointNotAddedError,
    CheckpointRelatedResourcesFreshnessError,
    ExpectationSuiteNotAddedError,
    ResourceFreshnessAggregateError,
    ValidationDefinitionNotAddedError,
    ValidationDefinitionRelatedResourcesFreshnessError,
)
from great_expectations.validator.v1_validator import Validator as V1Validator

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.core.suite_parameters import SuiteParameterDict
    from great_expectations.data_context import AbstractDataContext

DATA_SOURCE_NAME = "shared_name"
ASSET_NAME = "shared_asset"
FILE_NAME = "data.csv"


@pytest.fixture
def restore_current_context() -> Iterator[None]:
    """Put the process-wide current Data Context back as the test found it."""
    try:
        previous: AbstractDataContext | None = project_manager.get_current_project()
    except DataContextRequiredError:
        previous = None
    yield
    set_context(previous)


def _add_csv_batch_definition(
    context: AbstractDataContext, directory: pathlib.Path, contents: str
) -> BatchDefinition:
    """Write a CSV into `directory` and add a data source, asset and batch definition over it."""
    directory.mkdir()
    (directory / FILE_NAME).write_text(contents)
    datasource = context.data_sources.add_pandas_filesystem(
        DATA_SOURCE_NAME, base_directory=directory
    )
    asset = datasource.add_csv_asset(ASSET_NAME)
    return asset.add_batch_definition_path("batch_definition", path=pathlib.Path(FILE_NAME))


@pytest.mark.filesystem
def test_batch_validates_against_its_own_context_while_another_is_current(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    c1_data = tmp_path / "c1"
    batch_definition = _add_csv_batch_definition(c1, c1_data, "a\n1\n2\n3\n")
    batch = batch_definition.get_batch()

    # C2 becomes current and holds a data source of the same name over different data.
    c2 = gx.get_context(mode="ephemeral")
    c2_data = tmp_path / "c2"
    _add_csv_batch_definition(c2, c2_data, "a\n10\n20\n30\n")
    assert project_manager.get_current_project() is c2
    assert c1.data_sources.get(DATA_SOURCE_NAME) is not c2.data_sources.get(DATA_SOURCE_NAME)

    suite = ExpectationSuite(
        name="max_is_three",
        expectations=[gxe.ExpectColumnMaxToBeBetween(column="a", min_value=3, max_value=3)],
    )
    result = batch.validate(suite)

    # The observed value tells the two contexts' data apart; the batch spec names C1's file.
    assert result.success is True
    assert result.results[0].result["observed_value"] == 3
    assert result.meta["batch_spec"]["path"] == str(c1_data / FILE_NAME)
    assert (
        result.meta["batch_markers"]["pandas_data_fingerprint"]
        == batch.batch_markers["pandas_data_fingerprint"]
    )


@pytest.mark.unit
def test_batch_definition_without_an_owner_validates_through_the_current_context(
    mocker: MockerFixture, restore_current_context: None
) -> None:
    other = gx.get_context(mode="ephemeral")
    current = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is current
    current_spy = mocker.patch.object(current, "get_validator")
    other_spy = mocker.patch.object(other, "get_validator")
    mocker.patch.object(BatchDefinition, "build_batch_request")

    validator = V1Validator(batch_definition=BatchDefinition(name="hand_assembled"))
    built = validator._wrapped_validator

    current_spy.assert_called_once()
    other_spy.assert_not_called()
    assert built is current_spy.return_value


SUITE_NAME = "max_is_three"


def _max_is_three_suite(name: str = SUITE_NAME) -> ExpectationSuite:
    return ExpectationSuite(
        name=name,
        expectations=[gxe.ExpectColumnMaxToBeBetween(column="a", min_value=3, max_value=3)],
    )


def _add_validation_definition(context: AbstractDataContext) -> ValidationDefinition:
    """Add a data source, asset, batch definition, suite and validation definition to `context`."""
    batch_definition = (
        context.data_sources.add_pandas("source")
        .add_dataframe_asset("asset")
        .add_batch_definition_whole_dataframe("whole")
    )
    return context.validation_definitions.add(
        ValidationDefinition(
            name="validation",
            data=batch_definition,
            suite=context.suites.add(_max_is_three_suite()),
        )
    )


def _run(validation_definition: ValidationDefinition) -> tuple[bool, object]:
    """Run over a dataframe whose maximum is three; return the outcome and observed value."""
    result = validation_definition.run(
        batch_parameters={"dataframe": pd.DataFrame({"a": [1, 2, 3]})}
    )
    return result.success, result.results[0].result["observed_value"]


@pytest.mark.unit
def test_validation_definition_runs_after_another_context_is_created(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_validation_definition(c1)
    outcome_alone = _run(validation_definition)
    assert outcome_alone == (True, 3)

    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2

    assert _run(validation_definition) == outcome_alone


@pytest.mark.unit
def test_validation_definition_runs_after_the_other_context_is_collected(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_validation_definition(c1)
    c2 = gx.get_context(mode="ephemeral")
    collected = weakref.ref(c2)
    c3 = gx.get_context(mode="ephemeral")  # the current context no longer refers to C2
    assert project_manager.get_current_project() is c3

    del c2
    gc.collect()

    assert collected() is None
    assert _run(validation_definition) == (True, 3)


@pytest.mark.unit
def test_validation_definition_runs_after_set_context_selects_the_other_context(
    restore_current_context: None,
) -> None:
    c2 = gx.get_context(mode="ephemeral")
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_validation_definition(c1)
    assert project_manager.get_current_project() is c1

    set_context(c2)  # the stimulus: the other context becomes current

    assert project_manager.get_current_project() is c2
    assert _run(validation_definition) == (True, 3)


@pytest.mark.unit
def test_validation_definition_keeps_its_stores_while_the_current_context_changes(
    restore_current_context: None,
) -> None:
    c2 = gx.get_context(mode="ephemeral")
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_validation_definition(c1)
    assert project_manager.get_current_project() is c1

    def stores() -> tuple[object, ...]:
        return (
            validation_definition._validation_results_store,
            validation_definition.suite._store,
            validation_definition._resolve_context().context.validation_definition_store,
        )

    before = stores()
    assert before == (
        c1.validation_results_store,
        c1.expectations_store,
        c1.validation_definition_store,
    )

    set_context(c2)  # the stimulus: the other context becomes current
    assert project_manager.get_current_project() is c2
    after_selected = stores()
    c3 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c3
    after_created = stores()

    for after in (after_selected, after_created):
        assert [id(store) for store in after] == [id(store) for store in before]
    assert c2.validation_results_store is not before[0]


@pytest.mark.unit
def test_validation_definition_added_while_another_context_is_current_is_bound(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    batch_definition = (
        c1.data_sources.add_pandas("source")
        .add_dataframe_asset("asset")
        .add_batch_definition_whole_dataframe("whole")
    )
    suite = c1.suites.add(_max_is_three_suite())
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2

    added = c1.validation_definitions.add(
        ValidationDefinition(name="validation", data=batch_definition, suite=suite)
    )

    resolved = added._resolve_context()
    assert resolved.bound is True
    assert resolved.context is c1
    assert [v.name for v in c1.validation_definitions.all()] == ["validation"]
    assert c2.validation_definitions.all() == []
    assert _run(added) == (True, 3)


@pytest.mark.unit
def test_suite_copy_held_by_a_bound_validation_definition_resolves_through_its_context(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_validation_definition(c1)
    suite_copy = copy.deepcopy(validation_definition.suite)
    assert suite_copy._owner is None
    validation_definition.suite = suite_copy
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2

    assert _run(validation_definition) == (True, 3)

    assert suite_copy._owner is c1


@pytest.mark.unit
def test_suite_copy_added_to_a_validation_definition_while_another_context_is_current(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    batch_definition = (
        c1.data_sources.add_pandas("source")
        .add_dataframe_asset("asset")
        .add_batch_definition_whole_dataframe("whole")
    )
    suite_copy = copy.deepcopy(c1.suites.add(_max_is_three_suite()))
    assert suite_copy._owner is None
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2

    added = c1.validation_definitions.add(
        ValidationDefinition(name="validation", data=batch_definition, suite=suite_copy)
    )

    assert suite_copy._owner is c1
    assert _run(added) == (True, 3)


@pytest.mark.unit
def test_running_an_unadded_validation_definition_adds_it_to_its_own_context(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    batch_definition = (
        c1.data_sources.add_pandas("source")
        .add_dataframe_asset("asset")
        .add_batch_definition_whole_dataframe("whole")
    )
    unadded = ValidationDefinition(
        name="validation", data=batch_definition, suite=c1.suites.add(_max_is_three_suite())
    )
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2

    assert _run(unadded) == (True, 3)

    assert [v.name for v in c1.validation_definitions.all()] == ["validation"]
    assert c2.validation_definitions.all() == []


@pytest.mark.unit
def test_batch_definition_saves_to_its_own_context_while_another_is_current(
    restore_current_context: None,
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    batch_definition = (
        c1.data_sources.add_pandas("source")
        .add_dataframe_asset("asset")
        .add_batch_definition_whole_dataframe("whole")
    )
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2
    assert "source" not in c2.data_sources.all()

    batch_definition.save()

    assert "source" in c1.data_sources.all()
    assert "source" not in c2.data_sources.all()


@pytest.mark.unit
def test_hand_built_validation_definition_reports_freshness_without_a_current_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_definition = BatchDefinition(name="hand_assembled")
    validation_definition = ValidationDefinition(
        name="validation", data=batch_definition, suite=_max_is_three_suite()
    )
    monkeypatch.setattr(project_manager, "_ProjectManager__project", None)

    diagnostics = validation_definition.is_fresh()

    assert diagnostics.success is False
    assert {type(e) for e in diagnostics.errors} == {
        BatchDefinitionNotAddedError,
        ExpectationSuiteNotAddedError,
        ValidationDefinitionNotAddedError,
    }
    with pytest.raises(ResourceFreshnessAggregateError):
        validation_definition.json()
    with pytest.raises(ValidationDefinitionRelatedResourcesFreshnessError):
        validation_definition.run()


C1_ROWS = "a\n1\n2\n3\n"
C2_ROWS = "a\n10\n20\n30\n"


def _add_csv_validation_definition(
    context: AbstractDataContext, directory: pathlib.Path, contents: str
) -> ValidationDefinition:
    """Add a CSV-backed data source, batch definition, suite and validation definition."""
    batch_definition = _add_csv_batch_definition(context, directory, contents)
    return context.validation_definitions.add(
        ValidationDefinition(
            name="validation",
            data=batch_definition,
            suite=context.suites.add(_max_is_three_suite()),
        )
    )


def _result_count(context: AbstractDataContext) -> int:
    return len(context.validation_results_store.list_keys())


@pytest.mark.filesystem
def test_validation_definition_saves_to_its_own_context_while_another_is_current(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_csv_validation_definition(c1, tmp_path / "c1", C1_ROWS)
    validation_definition.suite = c1.suites.add(_max_is_three_suite("replacement"))
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2
    assert c2.data_sources.all() == {}
    assert c2.validation_definitions.all() == []
    assert c1.validation_definitions.get("validation").suite.name == SUITE_NAME

    validation_definition.save()

    assert c1.validation_definitions.get("validation").suite.name == "replacement"
    assert c2.validation_definitions.all() == []
    assert c2.data_sources.all() == {}


@pytest.mark.filesystem
def test_batch_definition_save_leaves_a_same_named_data_source_in_the_other_context_alone(
    tmp_path: pathlib.Path, mocker: MockerFixture, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    batch_definition = _add_csv_batch_definition(c1, tmp_path / "c1", C1_ROWS)
    c2 = gx.get_context(mode="ephemeral")
    c2_batch_definition = _add_csv_batch_definition(c2, tmp_path / "c2", C2_ROWS)
    c2_data_source = c2.data_sources.get(DATA_SOURCE_NAME)
    c1_data_source = c1.data_sources.get(DATA_SOURCE_NAME)
    assert project_manager.get_current_project() is c2
    set_datasource = mocker.spy(type(c1.data_sources.all()), "set_datasource")

    batch_definition.save()

    # The save is a write to C1's data sources; the C2-side assertions show it left C2 alone.
    set_datasource.assert_called_once_with(
        c1.data_sources.all(), name=DATA_SOURCE_NAME, ds=c1_data_source
    )
    assert set_datasource.call_args.args[0] is c1.data_sources.all()

    assert c2.data_sources.get(DATA_SOURCE_NAME) is c2_data_source
    assert c2_batch_definition.data_asset.datasource is c2_data_source
    assert isinstance(c1_data_source, PandasFilesystemDatasource)
    assert isinstance(c2_data_source, PandasFilesystemDatasource)
    assert c1_data_source.base_directory == tmp_path / "c1"
    assert c2_data_source.base_directory == tmp_path / "c2"


@pytest.mark.filesystem
def test_run_validates_the_batch_of_its_own_context_when_another_holds_the_same_names(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    c1_data = tmp_path / "c1"
    validation_definition = _add_csv_validation_definition(c1, c1_data, C1_ROWS)
    c1_fingerprint = validation_definition.batch_definition.get_batch().batch_markers[
        "pandas_data_fingerprint"
    ]

    # C2 becomes current and holds a data source and asset of the same names over other data.
    c2 = gx.get_context(mode="ephemeral")
    c2_data = tmp_path / "c2"
    c2_batch_definition = _add_csv_batch_definition(c2, c2_data, C2_ROWS)
    assert project_manager.get_current_project() is c2
    c2_fingerprint = c2_batch_definition.get_batch().batch_markers["pandas_data_fingerprint"]
    assert c2_fingerprint != c1_fingerprint

    result = validation_definition.run()

    # The batch identity is the same in both contexts; the file and its content tell them apart.
    assert result.success is True
    assert result.results[0].result["observed_value"] == 3
    assert result.meta["batch_spec"]["path"] == str(c1_data / FILE_NAME)
    assert result.meta["batch_markers"]["pandas_data_fingerprint"] == c1_fingerprint


@pytest.mark.filesystem
def test_run_writes_its_result_to_its_own_context_while_another_is_current(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_csv_validation_definition(c1, tmp_path / "c1", C1_ROWS)
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2
    assert (_result_count(c1), _result_count(c2)) == (0, 0)

    result = validation_definition.run()

    assert (result.success, result.results[0].result["observed_value"]) == (True, 3)
    assert (_result_count(c1), _result_count(c2)) == (1, 0)


@pytest.mark.filesystem
def test_result_lands_in_its_own_context_when_the_current_context_changes_mid_run(
    tmp_path: pathlib.Path, mocker: MockerFixture, restore_current_context: None
) -> None:
    c1 = gx.get_context(mode="ephemeral")
    validation_definition = _add_csv_validation_definition(c1, tmp_path / "c1", C1_ROWS)
    c3 = gx.get_context(mode="ephemeral")
    c2 = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is c2
    validate_expectation_suite = V1Validator.validate_expectation_suite
    switched: list[AbstractDataContext] = []

    def switch_then_validate(
        validator: V1Validator,
        expectation_suite: ExpectationSuite,
        expectation_parameters: Optional[SuiteParameterDict] = None,
    ) -> ExpectationSuiteValidationResult:
        # The freshness check has passed; the stimulus changes the current context before the
        # result is written.
        set_context(c3)
        switched.append(project_manager.get_current_project())
        return validate_expectation_suite(validator, expectation_suite, expectation_parameters)

    mocker.patch.object(V1Validator, "validate_expectation_suite", switch_then_validate)

    result = validation_definition.run()

    assert (result.success, result.results[0].result["observed_value"]) == (True, 3)
    assert switched == [c3]
    assert (_result_count(c1), _result_count(c2), _result_count(c3)) == (1, 0, 0)


CHECKPOINT_NAME = "checkpoint"


def _add_checkpoint(
    context: AbstractDataContext, validation_definition: ValidationDefinition
) -> Checkpoint:
    return context.checkpoints.add(
        Checkpoint(name=CHECKPOINT_NAME, validation_definitions=[validation_definition])
    )


def _run_checkpoint(checkpoint: Checkpoint) -> tuple[bool, object]:
    """Run over a dataframe whose maximum is three; return the outcome and observed value."""
    result = checkpoint.run(batch_parameters={"dataframe": pd.DataFrame({"a": [1, 2, 3]})})
    (validation_result,) = result.run_results.values()
    return result.success is True, validation_result.results[0].result["observed_value"]


def _build_pandas_resources(
    tag: str,
) -> tuple[AbstractDataContext, ValidationDefinition, Checkpoint]:
    """Build an ephemeral context, with the context becoming current, and its own resources."""
    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_pandas(f"source_{tag}")
        .add_dataframe_asset(f"asset_{tag}")
        .add_batch_definition_whole_dataframe(f"whole_{tag}")
    )
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name=f"validation_{tag}",
            data=batch_definition,
            suite=context.suites.add(_max_is_three_suite(f"suite_{tag}")),
        )
    )
    checkpoint = context.checkpoints.add(
        Checkpoint(name=f"checkpoint_{tag}", validation_definitions=[validation_definition])
    )
    return context, validation_definition, checkpoint


@pytest.mark.unit
def test_two_contexts_in_one_thread_are_independent_projects(
    restore_current_context: None,
) -> None:
    """Two contexts, one thread, pandas: the objects of the first keep working after the second.

    Seven run attempts and two store-identity checks, with no call that selects a context.
    """
    c1, vd1, cp1 = _build_pandas_resources("1")
    assert _run(vd1) == (True, 3)  # [1] with only C1 alive
    assert _run_checkpoint(cp1) == (True, 3)  # [2]
    results_store_before = vd1._validation_results_store
    suite_store_before = vd1.suite._store

    c2, vd2, cp2 = _build_pandas_resources("2")
    assert project_manager.get_current_project() is c2
    assert c1.suites.get("suite_1").name == "suite_1"
    assert c1.data_sources.get("source_1").name == "source_1"

    assert _run(vd1) == (True, 3)  # [3] after C2 was created
    assert _run_checkpoint(cp1) == (True, 3)  # [4]
    assert _run(vd2) == (True, 3)  # [5] the newest context's own objects
    c2_reference = weakref.ref(c2)
    del c2, vd2, cp2
    gc.collect()
    assert _run(vd1) == (True, 3)  # [6] after C2's names were dropped
    # [7] runs the checkpoint as well: no call re-selects the first context, which is the point.
    assert _run_checkpoint(cp1) == (True, 3)  # [7]

    # The stores of an object are a property of the context that owns it; creating another
    # context does not change which store object it reads.
    assert vd1._validation_results_store is results_store_before
    assert vd1.suite._store is suite_store_before
    assert results_store_before is c1.validation_results_store
    assert suite_store_before is c1.expectations_store
    assert c2_reference() is not None  # the current context still holds it, as it did before


@pytest.mark.unit
def test_checkpoint_runs_after_another_context_is_created_with_its_former_outcome(
    restore_current_context: None,
) -> None:
    c1, _, checkpoint = _build_pandas_resources("1")
    outcome_alone = _run_checkpoint(checkpoint)
    assert outcome_alone == (True, 3)

    c2, _, _ = _build_pandas_resources("2")
    assert project_manager.get_current_project() is c2

    assert checkpoint.is_fresh().success is True
    assert _run_checkpoint(checkpoint) == outcome_alone
    assert [c.name for c in c1.checkpoints.all()] == ["checkpoint_1"]
    assert [c.name for c in c2.checkpoints.all()] == ["checkpoint_2"]


@pytest.mark.unit
def test_every_contexts_objects_run_through_their_own_context(
    restore_current_context: None,
) -> None:
    c2, vd2, cp2 = _build_pandas_resources("2")
    c1, _, cp1 = _build_pandas_resources("1")  # C1 is the current context
    assert project_manager.get_current_project() is c1
    assert (_result_count(c1), _result_count(c2)) == (0, 0)

    assert _run(vd2) == (True, 3)
    assert _run_checkpoint(cp2) == (True, 3)

    assert (_result_count(c1), _result_count(c2)) == (0, 2)
    assert cp2.is_fresh().success is True
    assert _run_checkpoint(cp1) == (True, 3)
    assert (_result_count(c1), _result_count(c2)) == (1, 2)


@pytest.mark.filesystem
def test_objects_of_a_context_opened_twice_on_one_directory_run(
    tmp_path: pathlib.Path, restore_current_context: None
) -> None:
    project = tmp_path / "project"
    first = gx.get_context(mode="file", project_root_dir=project)
    added_validation_definition = _add_csv_validation_definition(first, tmp_path / "data", C1_ROWS)
    added_checkpoint = _add_checkpoint(first, added_validation_definition)
    validation_definition = first.validation_definitions.get("validation")
    checkpoint = first.checkpoints.get(CHECKPOINT_NAME)

    # A second context over the same directory loads its own copy of every stored object.
    second = gx.get_context(mode="file", project_root_dir=project)
    assert project_manager.get_current_project() is second
    assert first is not second
    assert second.data_sources.get(DATA_SOURCE_NAME) is not first.data_sources.get(DATA_SOURCE_NAME)

    for fresh_validation_definition in (added_validation_definition, validation_definition):
        assert fresh_validation_definition.is_fresh().success is True
        result = fresh_validation_definition.run()
        assert (result.success, result.results[0].result["observed_value"]) == (True, 3)
    for fresh_checkpoint in (added_checkpoint, checkpoint):
        assert fresh_checkpoint.is_fresh().success is True
        assert fresh_checkpoint.run().success is True


@pytest.mark.unit
def test_checkpoint_save_persists_to_its_own_context_by_value(
    restore_current_context: None,
) -> None:
    c1, vd1, _ = _build_pandas_resources("1")
    checkpoint = _add_checkpoint(c1, vd1)
    c2, vd2, _ = _build_pandas_resources("2")
    c2_checkpoint = _add_checkpoint(c2, vd2)
    assert project_manager.get_current_project() is c2
    assert checkpoint.result_format == "SUMMARY"
    assert c1.checkpoints.get(CHECKPOINT_NAME).result_format == "SUMMARY"
    assert c2.checkpoints.get(CHECKPOINT_NAME).result_format == "SUMMARY"

    checkpoint.result_format = "COMPLETE"  # changed between add and save
    assert checkpoint.is_fresh().success is False
    checkpoint.save()

    assert c1.checkpoints.get(CHECKPOINT_NAME).result_format == "COMPLETE"
    assert c2.checkpoints.get(CHECKPOINT_NAME).result_format == "SUMMARY"
    assert c2_checkpoint.id != checkpoint.id
    assert checkpoint.is_fresh().success is True


@pytest.mark.unit
def test_running_an_unadded_checkpoint_adds_it_to_its_own_context(
    restore_current_context: None,
) -> None:
    c1, vd1, _ = _build_pandas_resources("1")
    unadded = Checkpoint(name="unadded", validation_definitions=[vd1])
    c2, _, _ = _build_pandas_resources("2")
    assert project_manager.get_current_project() is c2

    assert _run_checkpoint(unadded) == (True, 3)

    assert sorted(c.name for c in c1.checkpoints.all()) == ["checkpoint_1", "unadded"]
    assert [c.name for c in c2.checkpoints.all()] == ["checkpoint_2"]
    assert _result_count(c1) == 1
    assert _result_count(c2) == 0


@pytest.mark.unit
def test_checkpoint_prepares_its_run_through_its_own_context(
    mocker: MockerFixture, restore_current_context: None
) -> None:
    c1, _, checkpoint = _build_pandas_resources("1")
    c2, _, _ = _build_pandas_resources("2")
    assert project_manager.get_current_project() is c2
    c1_prepare = mocker.spy(c1, "prepare_checkpoint_run")
    c2_prepare = mocker.spy(c2, "prepare_checkpoint_run")

    assert _run_checkpoint(checkpoint) == (True, 3)

    c1_prepare.assert_called_once()
    c2_prepare.assert_not_called()


@pytest.mark.unit
def test_checkpoint_without_an_owner_prepares_its_run_through_the_current_context(
    mocker: MockerFixture, restore_current_context: None
) -> None:
    current = gx.get_context(mode="ephemeral")
    assert project_manager.get_current_project() is current
    prepare = mocker.patch.object(current, "prepare_checkpoint_run")
    checkpoint = Checkpoint(
        name="hand_assembled",
        validation_definitions=[
            ValidationDefinition(
                name="validation",
                data=BatchDefinition(name="hand_assembled"),
                suite=_max_is_three_suite(),
            )
        ],
    )

    checkpoint._prepare_checkpoint_run_for_context({}, {})

    prepare.assert_called_once_with(checkpoint, {}, {})


@pytest.mark.unit
def test_hand_built_checkpoint_reports_freshness_without_a_current_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validation_definition = ValidationDefinition(
        name="validation",
        data=BatchDefinition(name="hand_assembled"),
        suite=_max_is_three_suite(),
    )
    checkpoint = Checkpoint(name="hand_assembled", validation_definitions=[validation_definition])
    monkeypatch.setattr(project_manager, "_ProjectManager__project", None)

    diagnostics = checkpoint.is_fresh()

    assert diagnostics.success is False
    assert {type(e) for e in diagnostics.errors} == {
        BatchDefinitionNotAddedError,
        ExpectationSuiteNotAddedError,
        ValidationDefinitionNotAddedError,
        CheckpointNotAddedError,
    }
    with pytest.raises(ResourceFreshnessAggregateError):
        checkpoint.json()
    with pytest.raises(CheckpointRelatedResourcesFreshnessError):
        checkpoint.run()
    with pytest.raises(DataContextRequiredError):
        checkpoint.save()
    with pytest.raises(DataContextRequiredError):
        checkpoint._add_to_store()


@pytest.mark.unit
def test_two_threads_each_run_their_own_context_objects_while_the_other_is_current(
    restore_current_context: None,
) -> None:
    """Two worker threads, each with its own context, run their own checkpoint at the same time.

    Each worker obtains its context through `get_context()` and registers its data source,
    suite, validation definition and checkpoint while holding a lock, so no two registrations
    overlap and the last-created context is current when both workers pass the barrier. Only the
    two `run()` calls overlap. No call selects a context.

    This does not exercise registering resources from two threads at once: resolving an object
    through its owning context makes no concurrency guarantee, and this test makes no claim
    about concurrent registration.
    """
    registration_lock = threading.Lock()
    both_registered = threading.Barrier(2, timeout=60)
    registration_order: list[int] = []
    outcomes: dict[int, tuple[bool, object]] = {}
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            with registration_lock:
                _, _, checkpoint = _build_pandas_resources(str(index))
                registration_order.append(index)
            both_registered.wait()
            outcomes[index] = _run_checkpoint(checkpoint)
        except BaseException as e:  # collected so the test can assert on it
            errors.append(e)
            both_registered.abort()

    threads = [threading.Thread(target=worker, args=(i,)) for i in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(registration_order) == [1, 2]
    assert outcomes == {1: (True, 3), 2: (True, 3)}
