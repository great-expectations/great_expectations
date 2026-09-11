"""Unit coverage for the validation-result-schema matrix's case table.

Three things are checked here, and the third is the one that carries weight:

- the table is internally consistent (unique ids, every case dispatchable to a schema family);
- the case record enforces its own engine-restriction invariants at construction;
- the published case set equals the set of expectations the shipped package actually registers,
  in both directions.

That last one used to be a count against a filesystem glob of ``expect_*.py``. A glob over file
names is not the registry: it missed ``unexpected_rows_expectation``, whose module does not match
that pattern, so the one expectation with no case still satisfied the guard -- and it counted five
modules whose classes never register at all, so the table had to carry placeholder entries for
them to make the arithmetic work. Comparing sets against the live registry, filtered on each
implementation's defining module, is the same question asked of the thing that actually answers it.
"""

from __future__ import annotations

from typing import FrozenSet

import pytest

import great_expectations.expectations as gxe
from great_expectations.core.validation_result_schemas.dispatcher import (
    family_for,
)
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from tests.integration.data_sources_and_expectations.expectations._validation_result_schemas_cases import (  # noqa: E501
    ALL_ENGINES,
    EXPECTATION_CASES,
    ExpectationCase,
)

# The package the shipped core expectations are defined under. Registration is a side effect of
# defining an `Expectation` subclass, so importing a contributed expectation package -- or
# defining one inside a test module -- adds registry entries the shipped package never ships.
# Filtering on the defining module keeps those out without hand-listing anything.
_CORE_MODULE_ROOT = "great_expectations.expectations.core"


def _is_core_module(module_name: str) -> bool:
    return module_name == _CORE_MODULE_ROOT or module_name.startswith(_CORE_MODULE_ROOT + ".")


def core_expectation_types() -> FrozenSet[str]:
    """The expectation type names the shipped package registers from its own core package.

    Reads the live registry through its only two public accessors -- there is no bulk class
    accessor -- so an expectation added upstream appears here with no edit to this module.

    Returns a `frozenset` rather than an ordered collection, so no consumer can come to depend on
    the registry's insertion order.
    """
    return frozenset(
        name
        for name in list_registered_expectation_implementations()
        if _is_core_module(get_expectation_impl(name).__module__)
    )


# ---------------------------------------------------------------------------
# Table consistency
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_case_ids_are_unique() -> None:
    ids = [c.id for c in EXPECTATION_CASES]
    assert len(ids) == len(set(ids)), f"Duplicate ids: {sorted(i for i in ids if ids.count(i) > 1)}"


@pytest.mark.unit
def test_case_id_matches_its_expectation_type() -> None:
    """A case's id is the key the completeness guard compares against the registry, so it has to
    name the expectation the case actually validates -- not merely be unique and plausible."""
    mismatched = [
        (case.id, case.expectation.expectation_type)
        for case in EXPECTATION_CASES
        if case.id != case.expectation.expectation_type
    ]
    assert not mismatched, f"Case ids that do not match their expectation's type: {mismatched}"


@pytest.mark.unit
def test_every_case_dispatches_to_a_known_family() -> None:
    for case in EXPECTATION_CASES:
        exp_type = case.expectation.expectation_type
        family = family_for(exp_type)
        assert family in ("map", "aggregate"), f"{exp_type!r} returned unexpected family {family!r}"


# ---------------------------------------------------------------------------
# Engine restrictions
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_every_engine_restricted_case_states_a_reason() -> None:
    """The runner turns a restriction into a skipped cell, and a skip with no reason is
    indistinguishable from missing coverage. The record enforces this at construction; this guard
    states it over the published table, so the invariant is asserted about the real cases and not
    only about the constructor."""
    unexplained = [
        case.id
        for case in EXPECTATION_CASES
        if case.engines != ALL_ENGINES and not case.engine_restriction_reason
    ]
    assert not unexplained, (
        f"Engine-restricted cases with no stated reason: {sorted(unexplained)}. Say what does not "
        "apply on the excluded engines."
    )


@pytest.mark.unit
def test_every_empty_result_declaration_states_a_reason_not_a_flag() -> None:
    """`empty_result_reason` is a reason rather than a boolean precisely so a case cannot switch
    the runner's empty-result gate off without saying why. A blank or whitespace-only string would
    be a flag wearing a reason's name."""
    blank = [
        case.id
        for case in EXPECTATION_CASES
        if case.empty_result_reason is not None and not case.empty_result_reason.strip()
    ]
    assert not blank, f"Cases declaring an empty `empty_result_reason`: {sorted(blank)}"


@pytest.mark.unit
def test_every_case_names_only_known_engines() -> None:
    unknown = {
        case.id: sorted(case.engines - ALL_ENGINES)
        for case in EXPECTATION_CASES
        if case.engines - ALL_ENGINES
    }
    assert not unknown, f"Cases naming engines outside the harness vocabulary: {unknown}"


@pytest.mark.unit
def test_case_defaults_to_every_engine() -> None:
    case = ExpectationCase(
        id="expect_column_values_to_not_be_null",
        expectation=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
    )
    assert case.engines == ALL_ENGINES
    assert case.engine_restriction_reason is None


@pytest.mark.unit
def test_case_restriction_without_a_reason_is_rejected() -> None:
    with pytest.raises(ValueError, match="without an `engine_restriction_reason`"):
        ExpectationCase(
            id="expect_column_values_to_not_be_null",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
            engines=frozenset({"sql"}),
        )


@pytest.mark.unit
def test_case_reason_without_a_restriction_is_rejected() -> None:
    """A reason left behind on a case that no longer restricts anything reads as a live
    constraint while constraining nothing."""
    with pytest.raises(ValueError, match="while applying to every"):
        ExpectationCase(
            id="expect_column_values_to_not_be_null",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
            engines=ALL_ENGINES,
            engine_restriction_reason="stale reason",
        )


@pytest.mark.unit
def test_case_with_an_empty_engine_set_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty engine set"):
        ExpectationCase(
            id="expect_column_values_to_not_be_null",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
            engines=frozenset(),
            engine_restriction_reason="deliberately empty for this test",
        )


@pytest.mark.unit
def test_case_with_an_unknown_engine_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown execution engine"):
        ExpectationCase(
            id="expect_column_values_to_not_be_null",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
            engines=frozenset({"dask"}),
            engine_restriction_reason="dask is not an engine this harness knows",
        )


# ---------------------------------------------------------------------------
# The registry-derived set the completeness guard compares against
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_core_expectation_types_is_not_empty() -> None:
    """A vacuously-true completeness guard is exactly the failure mode this proves against: if the
    registry were empty, or the module filter matched nothing, the guard below would pass over
    nothing at all."""
    assert core_expectation_types(), (
        "core_expectation_types() returned an empty set. Either the live registry is empty or the "
        "module filter no longer matches the shipped core package."
    )


@pytest.mark.unit
def test_core_expectation_types_filter_is_exact() -> None:
    """Filter exactness, asserted rather than assumed: every name this derivation keeps really is
    defined under the core package. If the package were ever renamed, the filter would quietly
    match nothing and the completeness guard would shrink to match, undetected."""
    for name in core_expectation_types():
        module_name = get_expectation_impl(name).__module__
        assert _is_core_module(module_name), (
            f"{name!r} is defined in {module_name!r}, which is not under {_CORE_MODULE_ROOT!r}."
        )


@pytest.mark.unit
def test_core_expectation_types_includes_a_non_conventional_module_name() -> None:
    """``unexpected_rows_expectation``'s module name does not match the ``expect_*.py`` pattern the
    previous filesystem-glob derivation used, which is why it had no case at all. The
    registry-based derivation includes it, because it filters on the registered implementation's
    module rather than on a file name."""
    assert "unexpected_rows_expectation" in core_expectation_types()


@pytest.mark.unit
def test_core_expectation_types_excludes_an_unregistered_abstract_export() -> None:
    """The core package exports ``ExpectMulticolumnValuesToBeUnique``, but it declares no map
    metric and so never registers. A derivation built off the package's export list, or off its
    file names, overcounts by this one; the registry-based derivation cannot, because it never
    looks at either."""
    assert "expect_multicolumn_values_to_be_unique" not in core_expectation_types()


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------


def _completeness_check(published_ids: FrozenSet[str], registered: FrozenSet[str]) -> None:
    """The published case-id set must equal the registered set in both directions.

    Factored out of the guard below so its failure path can be exercised directly against a
    deliberately incomplete set -- proving this check can fail, rather than merely being green
    over two sets that happen to agree today.
    """
    missing = registered - published_ids
    assert not missing, (
        f"The following registered core expectations have no case: {sorted(missing)}. Add an "
        "ExpectationCase for each to EXPECTATION_CASES."
    )
    unrecognized = published_ids - registered
    assert not unrecognized, (
        f"The following case ids do not name a registered core expectation: "
        f"{sorted(unrecognized)}. Either the expectation was removed from the shipped package "
        "(delete the case) or the id is misspelled (fix it to match the registered "
        "expectation_type) -- check both."
    )


@pytest.mark.unit
def test_case_ids_match_the_registered_core_expectations_exactly() -> None:
    """An expectation registered with no case, and a case naming an unregistered expectation, are
    both defects: the first is a silent coverage hole, the second is a case that can never run."""
    _completeness_check(frozenset(case.id for case in EXPECTATION_CASES), core_expectation_types())


@pytest.mark.unit
def test_completeness_check_fails_in_each_direction() -> None:
    """``_completeness_check`` actually fails, in each direction, naming the offending id."""
    registered = frozenset({"expect_kept_case", "expect_missing_case"})

    with pytest.raises(AssertionError, match=r"have no case.*expect_missing_case"):
        _completeness_check(frozenset({"expect_kept_case"}), registered)

    with pytest.raises(AssertionError, match=r"do not name a registered.*expect_stale_case"):
        _completeness_check(
            frozenset({"expect_kept_case", "expect_missing_case", "expect_stale_case"}),
            registered,
        )


@pytest.mark.unit
def test_every_unsupported_data_source_is_a_registered_test_id_with_a_reason() -> None:
    """A declared gap must name a data source the harness actually has, and say why."""
    from tests.integration.test_utils.data_source_config import ALL_DATA_SOURCES

    known_test_ids = {config.test_id for config in ALL_DATA_SOURCES}
    declared = [
        (case.id, test_id, reason)
        for case in EXPECTATION_CASES
        for test_id, reason in case.unsupported_data_sources.items()
    ]
    assert declared, "the guard is armed on real declarations; none exist"
    for case_id, test_id, reason in declared:
        assert test_id in known_test_ids, (
            f"{case_id} declares {test_id!r} unsupported, but no canonical data source has "
            f"that test id; known: {sorted(known_test_ids)}"
        )
        assert reason.strip(), f"{case_id} declares {test_id!r} unsupported without a reason"


@pytest.mark.unit
def test_case_unsupported_data_source_without_a_reason_is_rejected() -> None:
    with pytest.raises(ValueError, match="without both a test id and a reason"):
        ExpectationCase(
            id="expect_column_to_exist",
            expectation=gxe.ExpectColumnToExist(column="increasing_key"),
            unsupported_data_sources={"mssql": "  "},
        )
