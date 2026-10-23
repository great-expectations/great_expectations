"""The public tier taxonomy: which declarations satisfy which criterion, and the tier each
combination of criteria yields.

Expected values are written out by hand. Deriving them with the logic under test could not
fail.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from dataclasses import replace
from enum import Enum
from itertools import chain, combinations, zip_longest
from pathlib import Path
from typing import AbstractSet, Any, Callable, FrozenSet, Mapping, Optional, Tuple

import pytest

from tests.compatibility_reference import compatibility_reference_table as table
from tests.compatibility_reference import upstream_declarations
from tests.compatibility_reference.compatibility_reference_table import (
    CRITERIA,
    GENERATED_NOTICE,
    GENERATED_PARTIAL_PATH,
    REGENERATION_COMMAND,
    PublicTier,
    PublishedRow,
    _escape_cell,
    assemble_rows,
    criteria_met_by,
    recorded_exclusion_notes,
    render_table,
    tier_for,
    uncovered_connection_paths_note,
)
from tests.compatibility_reference.upstream_declarations import (
    SupportTier,
    UpstreamDeclarationError,
    UpstreamFacts,
    load_upstream_facts,
)
from tests.datasource.fluent.crud_contract import (
    FLUENT_TYPES_NAMED_BY_NO_RECORD,
    RECORDS_COVERED_BUT_UNABLE_TO_CLAIM,
)
from tests.integration.test_utils.data_source_config import (
    ClickHouseDatasourceTestConfig,
    DataSourceSpec,
    OracleDatasourceTestConfig,
    isolated_registry,
    iter_data_source_specs,
    register_data_source,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
)

pytestmark = pytest.mark.project

GALLERY = "gallery"
SUITE = "expectation_suite"
API = "datasource_api"

# Every subset of the three criteria, with the tier it must yield. Written out in full so a
# reader checks each row against the stated rule, not against code.
EXPECTED_TIER_BY_COMBINATION = {
    frozenset(): PublicTier.BEST_EFFORT,
    frozenset({API}): PublicTier.BEST_EFFORT,
    frozenset({SUITE}): PublicTier.TESTED,
    frozenset({SUITE, API}): PublicTier.TESTED,
    frozenset({GALLERY}): PublicTier.TESTED,
    frozenset({GALLERY, SUITE}): PublicTier.TESTED,
    frozenset({GALLERY, API}): PublicTier.FULLY_SUPPORTED,
    frozenset({GALLERY, SUITE, API}): PublicTier.FULLY_SUPPORTED,
}


def _powerset(keys):
    items = sorted(keys)
    return [
        frozenset(subset)
        for subset in chain.from_iterable(combinations(items, n) for n in range(len(items) + 1))
    ]


def _name(combination: FrozenSet[str]) -> str:
    return "+".join(sorted(combination)) or "nothing"


def test_criteria_are_the_three_the_expected_table_covers():
    assert [criterion.key for criterion in CRITERIA] == [GALLERY, SUITE, API]


def test_expected_table_covers_every_subset_of_the_criteria():
    keys = {criterion.key for criterion in CRITERIA}
    assert set(EXPECTED_TIER_BY_COMBINATION) == set(_powerset(keys))
    assert len(EXPECTED_TIER_BY_COMBINATION) == 8


@pytest.mark.parametrize(
    "combination",
    list(EXPECTED_TIER_BY_COMBINATION),
    ids=[_name(c) for c in EXPECTED_TIER_BY_COMBINATION],
)
def test_tier_for_each_combination_of_criteria(combination):
    assert tier_for(combination) is EXPECTED_TIER_BY_COMBINATION[combination]


def test_datasource_api_alone_stays_in_the_lowest_tier_while_displaying_the_criterion():
    met = criteria_met_by({SupportTier.FLUENT_API})
    assert met == frozenset({API})
    assert tier_for(met) is PublicTier.BEST_EFFORT


def test_full_gallery_without_the_datasource_api_resolves_to_the_middle_tier():
    met = criteria_met_by({SupportTier.GALLERY})
    assert met == frozenset({GALLERY})
    assert tier_for(met) is PublicTier.TESTED


def test_each_declaration_satisfies_exactly_the_criteria_the_table_names():
    expected = {
        SupportTier.GALLERY: frozenset({GALLERY}),
        SupportTier.CANONICAL_EXPECTATIONS: frozenset({SUITE}),
        SupportTier.CURATED_SQL: frozenset({SUITE}),
        SupportTier.FLUENT_API: frozenset({API}),
    }
    assert set(expected) == set(SupportTier)
    for declaration, criteria in expected.items():
        assert criteria_met_by({declaration}) == criteria


def test_either_expectation_suite_declaration_alone_satisfies_the_suite_criterion():
    for declaration in (SupportTier.CANONICAL_EXPECTATIONS, SupportTier.CURATED_SQL):
        met = criteria_met_by({declaration})
        assert met == frozenset({SUITE})
        assert tier_for(met) is PublicTier.TESTED


def test_curated_suite_without_the_canonical_suite_reaches_the_top_tier():
    met = criteria_met_by({SupportTier.CURATED_SQL, SupportTier.FLUENT_API, SupportTier.GALLERY})
    assert met == frozenset({GALLERY, SUITE, API})
    assert tier_for(met) is PublicTier.FULLY_SUPPORTED


def test_no_declarations_meet_no_criteria():
    assert criteria_met_by(set()) == frozenset()


def test_a_declaration_the_table_does_not_name_leaves_the_result_unchanged(monkeypatch):
    # Simulate the enumeration gaining a member the table does not name: un-name a real member
    # from every criterion, then hold it alongside others.
    unnamed = SupportTier.FLUENT_API
    monkeypatch.setattr(
        table,
        "CRITERIA",
        tuple(
            replace(criterion, declarations=criterion.declarations - {unnamed})
            for criterion in table.CRITERIA
        ),
    )
    base = {SupportTier.GALLERY, SupportTier.CURATED_SQL}
    without = criteria_met_by(base)
    assert without == frozenset({GALLERY, SUITE})
    assert criteria_met_by(base | {unnamed}) == without
    assert tier_for(criteria_met_by(base | {unnamed})) is tier_for(without)
    assert tier_for(without) is PublicTier.TESTED
    assert criteria_met_by({unnamed}) == frozenset()
    assert tier_for(criteria_met_by({unnamed})) is PublicTier.BEST_EFFORT


def test_an_enumeration_member_added_before_the_table_loads_leaves_results_unchanged(
    monkeypatch: pytest.MonkeyPatch,
):
    # A member present when the module is first imported can be captured in import-time state
    # that patching the loaded module afterwards never reaches. Model that: extend the enumeration,
    # then execute a fresh copy of the module from its own source so it loads against the
    # extended enumeration.
    make_enum: Any = Enum  # members are computed, which the static enum check cannot follow
    extended: Any = make_enum(
        "SupportTier",
        [(member.name, member.value) for member in SupportTier] + [("NEWLY_ADDED", "newly_added")],
    )
    monkeypatch.setattr(upstream_declarations, "SupportTier", extended)
    module_name = "fresh_table_with_an_added_enumeration_member"
    spec = importlib.util.spec_from_file_location(module_name, table.__file__)
    assert spec is not None and spec.loader is not None
    fresh = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, fresh)
    spec.loader.exec_module(fresh)

    base = {extended.GALLERY, extended.CURATED_SQL, extended.FLUENT_API}
    added = extended.NEWLY_ADDED
    without = fresh.criteria_met_by(base)
    assert without == frozenset({GALLERY, SUITE, API})
    assert fresh.criteria_met_by(base | {added}) == without
    assert fresh.tier_for(fresh.criteria_met_by(base | {added})) is fresh.tier_for(without)
    assert fresh.criteria_met_by({added}) == frozenset()
    assert fresh.tier_for(fresh.criteria_met_by({added})).name == PublicTier.BEST_EFFORT.name


def test_a_criterion_key_the_table_does_not_name_does_not_raise_or_lift():
    assert tier_for(frozenset({"unplanned"})) is PublicTier.BEST_EFFORT
    assert tier_for(frozenset({"unplanned", GALLERY})) is PublicTier.TESTED


def test_tier_names_do_not_reuse_declaration_names_or_assert_authorship():
    values = {tier.value for tier in PublicTier}
    assert values == {"Fully supported", "Tested", "Best effort"}
    declaration_names = {m.name.lower().replace("_", " ") for m in SupportTier} | {
        m.value.replace("_", " ") for m in SupportTier
    }
    assert not {v.lower() for v in values} & declaration_names


def test_criterion_labels_are_the_public_phrases():
    assert [criterion.label for criterion in CRITERIA] == [
        "Every shipped expectation",
        "Expectation suite",
        "Datasource API contract",
    ]


# ---------------------------------------------------------------------------------------------
# Row assembly
#
# Throwaway records are registered inside the registry's isolation seam, which empties the
# registry on entry. Anything reading the real registry from inside it would see nothing and pass
# vacuously, so the real-registry assertions are made outside it, each beside a throwaway twin.
# ---------------------------------------------------------------------------------------------

LANE = CiLaneRef(workflow_job="marker-tests", marker_token="throwaway")
ORACLE_SPEC = OracleDatasourceTestConfig.DATA_SOURCE_SPEC
CLICKHOUSE_SPEC = ClickHouseDatasourceTestConfig.DATA_SOURCE_SPEC

PANDAS_DESCRIPTION = "in-memory pandas DataFrames"
CSV_DESCRIPTION = "CSV and other files read with pandas"
S3_PANDAS_DESCRIPTION = "Amazon S3 objects read with pandas"
S3_SPARK_DESCRIPTION = "Amazon S3 objects read with Spark"
BOTH_S3 = f"{S3_SPARK_DESCRIPTION} and {S3_PANDAS_DESCRIPTION}"  # sorted by text

ALL_THREE = frozenset(
    {SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API, SupportTier.GALLERY}
)


def _record(
    label: str,
    public_name: str,
    *,
    tiers: FrozenSet[SupportTier] = frozenset(),
    fluent_types: FrozenSet[str] = frozenset({"pandas"}),
    lane: Optional[CiLaneRef] = LANE,
    exclusions: Optional[Mapping[SupportTier, Mapping[str, str]]] = None,
    provisioning: DataSourceProvisioning = DataSourceProvisioning.IN_PROCESS,
    provisioning_note: Optional[str] = None,
) -> DataSourceSpec:
    return DataSourceSpec(
        label=label,
        public_name=public_name,
        provisioning=provisioning,
        provisioning_note=provisioning_note,
        fluent_types=fluent_types,
        marker=label,
        tiers=tiers,
        tier_case_exclusions=exclusions or {},
        ci_lane=lane,
    )


def _facts(
    *specs: DataSourceSpec,
    fluent_exclusions: Optional[Mapping[str, Mapping[str, str]]] = None,
    covered: FrozenSet[str] = frozenset(),
    uncovered_paths: FrozenSet[str] = frozenset(),
) -> UpstreamFacts:
    return UpstreamFacts(
        specs=specs,
        covered_but_unable_to_claim=covered,
        fluent_types_named_by_no_record=uncovered_paths,
        fluent_case_exclusions=fluent_exclusions or {},
    )


def register_real_records_the_loader_checks_against() -> None:
    """Register the two real records whose declarations the loader's own maps are checked against.

    ``TESTED_VERSION_NOTES`` names Oracle, and ``CASE_DESCRIPTIONS`` describes the one case
    only ClickHouse's record excludes. Inside an emptied registry the loader fails on each
    unless its record is present.
    """
    register_data_source(ORACLE_SPEC)
    register_data_source(CLICKHOUSE_SPEC)


def _rows_in_seam(*records: DataSourceSpec) -> Tuple[PublishedRow, ...]:
    """Register the records in an emptied registry and assemble what the loader returns.

    The loader raises about the Oracle version note unless an Oracle record with a lane is
    registered, and about a described case no record excludes unless the one record that
    excludes a case of its own is registered, so both real records go in beside the throwaways.
    """
    with isolated_registry():
        register_real_records_the_loader_checks_against()
        for record in records:
            register_data_source(record)
        # The real fluent suite's recorded exclusions would add notes to any throwaway row that
        # names the `pandas` type; they are asserted on the real row, not on these.
        return assemble_rows(replace(load_upstream_facts(), fluent_case_exclusions={}))


# Descriptions for the case keys the throwaway records below exclude. They are written to sort
# differently from their keys (alpha < beta < mid < zeta, but Z < Y < M < A), so a note that
# ordered by description instead of by key would not match an expected value written by hand.
THROWAWAY_CASE_DESCRIPTIONS = {
    "some_case": "the shared check",
    "a_case": "the first-A check",
    "z_case": "the last-Z check",
    "m_case": "the middle-M check",
    "canonical_case": "the canonical check",
    "curated_case": "the curated check",
    "suite_case": "the suite check",
    "api_case": "the API check",
    "gallery_case": "the gallery check",
    "alpha": "the Z-sorting check",
    "beta": "the Y-sorting check",
    "mid": "the M-sorting check",
    "zeta": "the A-sorting check",
}


@pytest.fixture
def throwaway_case_descriptions(monkeypatch):
    for case, description in THROWAWAY_CASE_DESCRIPTIONS.items():
        monkeypatch.setitem(upstream_declarations.CASE_DESCRIPTIONS, case, description)


WITH_CASE_DESCRIPTIONS = pytest.mark.usefixtures("throwaway_case_descriptions")


def _row(rows: Tuple[PublishedRow, ...], public_name: str) -> PublishedRow:
    matching = [row for row in rows if row.public_name == public_name]
    assert len(matching) == 1, [row.public_name for row in rows]
    return matching[0]


def test_two_records_sharing_a_name_make_one_row_and_a_criterion_one_lacks_is_not_met():
    rows = _rows_in_seam(
        _record(
            "variant-a", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"pandas"})
        ),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas_filesystem"}),
        ),
    )
    row = _row(rows, "Throwaway Source")
    assert [spec.label for spec in row.specs] == ["variant-a", "variant-b"]
    assert row.criteria_met == frozenset({SUITE, API})
    assert row.tier is PublicTier.TESTED
    assert row.notes == (f"Every shipped expectation: not met for {CSV_DESCRIPTION}.",)


def test_a_criterion_only_the_union_of_the_records_reaches_is_not_met():
    # Neither record alone reaches the top tier. Pooling their declarations would.
    rows = _rows_in_seam(
        _record(
            "variant-a",
            "Throwaway Source",
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas"}),
        ),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.GALLERY, SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas_filesystem"}),
        ),
    )
    row = _row(rows, "Throwaway Source")
    assert row.criteria_met == frozenset({API})
    assert row.tier is PublicTier.BEST_EFFORT
    assert row.notes == (
        f"Every shipped expectation: not met for {PANDAS_DESCRIPTION}.",
        f"Expectation suite: not met for {CSV_DESCRIPTION}.",
    )


def test_records_that_agree_produce_no_disagreement_note():
    rows = _rows_in_seam(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"sqlite"})
        ),
    )
    row = _row(rows, "Throwaway Source")
    assert row.criteria_met == frozenset({GALLERY, SUITE, API})
    assert row.tier is PublicTier.FULLY_SUPPORTED
    assert row.notes == ()


def test_a_criterion_no_record_declares_is_unmet_without_a_disagreement_note():
    rows = _rows_in_seam(
        _record("variant-a", "Throwaway Source", tiers=frozenset({SupportTier.FLUENT_API})),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sqlite"}),
        ),
    )
    row = _row(rows, "Throwaway Source")
    assert row.criteria_met == frozenset({API})
    assert row.notes == ()


def test_a_shortfall_names_the_variant_by_what_it_connects_to_not_by_label_or_type():
    rows = _rows_in_seam(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "internal-label-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas_filesystem"}),
        ),
    )
    (note,) = [n for n in _row(rows, "Throwaway Source").notes if n.startswith("Expectation suite")]
    assert note == f"Expectation suite: not met for {CSV_DESCRIPTION}."
    for internal in ("internal-label-b", "pandas_filesystem"):
        assert internal not in note


def test_a_variant_reached_through_several_types_is_named_by_all_of_them():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        ),
    )
    (row,) = assemble_rows(facts)
    assert row.notes == (
        f"Every shipped expectation: not met for {BOTH_S3}.",
        f"Expectation suite: not met for {BOTH_S3}.",
    )


def test_several_variants_falling_short_are_all_named_in_a_fixed_order():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas_filesystem"}),
        ),
        _record(
            "variant-c",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sqlite"}),
        ),
    )
    (row,) = assemble_rows(facts)
    assert row.notes == (
        f"Every shipped expectation: not met for {CSV_DESCRIPTION}; SQLite databases.",
        f"Expectation suite: not met for {CSV_DESCRIPTION}; SQLite databases.",
    )


def test_a_shortfall_that_cannot_be_named_for_a_reader_fails_rather_than_printing_an_identifier():
    undescribed = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b", "Throwaway Source", tiers=frozenset(), fluent_types=frozenset({"made_up"})
        ),
    )
    with pytest.raises(UpstreamDeclarationError, match="made_up"):
        assemble_rows(undescribed)
    typeless = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record("variant-b", "Throwaway Source", tiers=frozenset(), fluent_types=frozenset()),
    )
    with pytest.raises(UpstreamDeclarationError, match="variant-b"):
        assemble_rows(typeless)


def test_a_lane_with_no_tier_is_evidence_of_nothing():
    rows = _rows_in_seam(_record("laned", "Throwaway Laned", tiers=frozenset(), lane=LANE))
    row = _row(rows, "Throwaway Laned")
    assert row.specs[0].ci_lane is not None
    assert row.specs[0].tiers == frozenset()
    assert row.criteria_met == frozenset()
    assert row.tier is PublicTier.BEST_EFFORT


def test_the_lane_adds_nothing_to_a_record_that_claims_a_tier():
    laned = _record(
        "laned", "Throwaway Laned", tiers=frozenset({SupportTier.FLUENT_API}), lane=LANE
    )
    (row,) = assemble_rows(_facts(laned))
    assert row.criteria_met == frozenset({API})
    assert row.tier is PublicTier.BEST_EFFORT


def test_the_real_object_stores_have_a_lane_and_no_expectation_suite_and_stay_lowest():
    rows = {row.public_name: row for row in assemble_rows(load_upstream_facts())}
    for name in ("Amazon S3", "Google Cloud Storage"):
        row = rows[name]
        assert all(spec.ci_lane is not None for spec in row.specs)
        assert row.criteria_met == frozenset({API})
        assert row.tier is PublicTier.BEST_EFFORT


def test_the_real_pandas_records_make_one_row_and_agree():
    real = [spec for spec in iter_data_source_specs() if spec.public_name == "Pandas"]
    assert len(real) == 2
    rows = [row for row in assemble_rows(load_upstream_facts()) if row.public_name == "Pandas"]
    assert len(rows) == 1
    assert rows[0].specs == tuple(sorted(real, key=lambda spec: spec.label))
    assert rows[0].tier is PublicTier.FULLY_SUPPORTED
    # The variants agree, so no disagreement note; the one note is the recorded exclusion.
    assert [note for note in rows[0].notes if ": not met for " in note] == []


def test_an_empty_registry_fails_in_row_assembly_rather_than_returning_no_rows():
    with isolated_registry():
        # The loader would stop first, on the Oracle version note, with its own error type.
        with pytest.raises(UpstreamDeclarationError):
            load_upstream_facts()
        # What the loader would hand over for an empty registry, read live from the seam.
        facts = _facts(*iter_data_source_specs())
        assert facts.specs == ()
        with pytest.raises(ValueError, match="yields no record") as raised:
            assemble_rows(facts)
    assert not isinstance(raised.value, UpstreamDeclarationError)


def test_a_record_with_no_public_name_fails_instead_of_being_dropped():
    nameless = _record("nameless", "")
    with isolated_registry():
        # Registration rejects it, so assembly can only meet one built outside the registry.
        with pytest.raises(ValueError, match="empty public data source name"):
            register_data_source(nameless)
    named = _record("a-named", "Throwaway Source", tiers=ALL_THREE)
    for blank in ("", "   "):
        with pytest.raises(ValueError, match=r"'nameless' carries no public name"):
            assemble_rows(_facts(named, _record("nameless", blank)))


def test_a_newly_registered_record_appears_as_a_row_without_any_other_edit():
    before = _rows_in_seam()
    after = _rows_in_seam(_record("fresh", "Entirely New Source", tiers=ALL_THREE))
    assert "Entirely New Source" not in [row.public_name for row in before]
    row = _row(after, "Entirely New Source")
    assert row.public_name == "Entirely New Source"
    assert row.tier is PublicTier.FULLY_SUPPORTED


def test_rows_are_ordered_by_public_name_ignoring_case_not_by_label():
    # The registry orders by label, which here runs opposite to the names; a plain code-point sort
    # of the names would put "Zed" before "alpha2".
    rows = _rows_in_seam(
        _record("a-label", "Zed"),
        _record("b-label", "Alpha"),
        _record("c-label", "beta"),
        _record("d-label", "alpha2"),
    )
    assert [row.public_name for row in rows] == [
        "Alpha",
        "alpha2",
        "beta",
        "ClickHouse",
        "Oracle",
        "Zed",
    ]


def test_the_records_within_a_row_are_ordered_by_label_whatever_order_they_arrive_in():
    facts = _facts(_record("zz", "Throwaway Source"), _record("aa", "Throwaway Source"))
    (row,) = assemble_rows(facts)
    assert [spec.label for spec in row.specs] == ["aa", "zz"]


def test_a_name_missing_from_the_shipped_vocabulary_is_printed_exactly_as_declared():
    (row,) = assemble_rows(_facts(_record("odd", "Odd-Name (beta) v2")))
    assert row.public_name == "Odd-Name (beta) v2"


def test_a_record_that_declares_no_criterion_leaves_every_criterion_unmet_and_is_named_for_each():
    # One record holding nothing must not be dropped from the conjunction: the row is the lowest
    # tier and each criterion its sibling meets reports the empty record as falling short.
    (row,) = assemble_rows(
        _facts(
            _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
            _record(
                "variant-b",
                "Throwaway Source",
                tiers=frozenset(),
                fluent_types=frozenset({"pandas_filesystem"}),
            ),
        )
    )
    assert row.criteria_met == frozenset()
    assert row.criteria_partial == frozenset()
    assert row.tier is PublicTier.BEST_EFFORT
    assert row.notes == (
        f"Every shipped expectation: not met for {CSV_DESCRIPTION}.",
        f"Expectation suite: not met for {CSV_DESCRIPTION}.",
        f"Datasource API contract: not met for {CSV_DESCRIPTION}.",
    )


def test_a_shortfall_naming_the_same_type_as_a_variant_that_meets_it_fails():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"sql"})),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sql"}),
        ),
    )
    with pytest.raises(UpstreamDeclarationError) as raised:
        assemble_rows(facts)
    assert str(raised.value) == (
        "Records under 'Throwaway Source' disagree about 'Every shipped expectation', but the "
        "record labelled 'variant-b' shares fluent datasource type(s) ['sql'] with a record that "
        "meets it, so no public-facing description distinguishes the variant that falls short "
        "from the ones that do not. Give the records distinct fluent types, or declare the "
        "criterion consistently across them."
    )


def test_a_shortfall_whose_types_overlap_those_of_a_variant_that_meets_it_fails():
    facts = _facts(
        _record(
            "variant-a", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"pandas"})
        ),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas", "pandas_filesystem"}),
        ),
    )
    with pytest.raises(UpstreamDeclarationError) as raised:
        assemble_rows(facts)
    assert str(raised.value) == (
        "Records under 'Throwaway Source' disagree about 'Every shipped expectation', but the "
        "record labelled 'variant-b' shares fluent datasource type(s) ['pandas'] with a record "
        "that meets it, so no public-facing description distinguishes the variant that falls "
        "short from the ones that do not. Give the records distinct fluent types, or declare the "
        "criterion consistently across them."
    )


def _shares_pandas_with_a_variant_that_meets_it(label: str) -> str:
    return (
        "Records under 'Throwaway Source' disagree about 'Every shipped expectation', but the "
        f"record labelled {label!r} shares fluent datasource type(s) ['pandas'] with a record "
        "that meets it, so no public-facing description distinguishes the variant that falls "
        "short from the ones that do not. Give the records distinct fluent types, or declare the "
        "criterion consistently across them."
    )


@pytest.mark.parametrize(
    "ambiguous_label",
    ["variant-a", "variant-b", "variant-d"],
    ids=["first-shortfall", "middle-shortfall", "last-shortfall"],
)
def test_every_variant_that_falls_short_is_checked_for_ambiguity_wherever_it_sorts(
    ambiguous_label: str,
):
    # variant-c meets the criterion through pandas. Three variants fall short; exactly one also
    # declares pandas and the other two declare types variant-c does not. The ambiguous one is
    # placed first, in the middle and last of the shortfalls in label order, so a check that
    # skips any position, or stops at the first shortfall it finds unambiguous, fails a case.
    clean_types = iter([frozenset({"pandas_filesystem"}), frozenset({"sqlite"})])
    records = [
        _record(
            "variant-c", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"pandas"})
        )
    ]
    for label in ("variant-a", "variant-b", "variant-d"):
        records.append(
            _record(
                label,
                "Throwaway Source",
                tiers=frozenset({SupportTier.FLUENT_API}),
                fluent_types=(
                    frozenset({"pandas"}) if label == ambiguous_label else next(clean_types)
                ),
            )
        )
    with pytest.raises(UpstreamDeclarationError) as raised:
        assemble_rows(_facts(*records))
    assert str(raised.value) == _shares_pandas_with_a_variant_that_meets_it(ambiguous_label)


@pytest.mark.parametrize(
    "overlapping_label",
    ["variant-a", "variant-c", "variant-d"],
    ids=["first-meeting", "middle-meeting", "last-meeting"],
)
def test_a_shortfall_is_compared_against_every_variant_that_meets_the_criterion_wherever_it_sorts(
    overlapping_label: str,
):
    # variant-b falls short through pandas. Three variants meet the criterion; exactly one also
    # declares pandas and the other two declare types variant-b does not. That one is placed
    # first, in the middle and last of the meeting variants in label order, so a comparison
    # that leaves out any position, or stops at the first meeting variant it shares nothing
    # with, fails a case.
    clean_types = iter([frozenset({"sqlite"}), frozenset({"pandas_s3"})])
    records = [
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"pandas"}),
        )
    ]
    for label in ("variant-a", "variant-c", "variant-d"):
        records.append(
            _record(
                label,
                "Throwaway Source",
                tiers=ALL_THREE,
                fluent_types=(
                    frozenset({"pandas"}) if label == overlapping_label else next(clean_types)
                ),
            )
        )
    with pytest.raises(UpstreamDeclarationError) as raised:
        assemble_rows(_facts(*records))
    assert str(raised.value) == _shares_pandas_with_a_variant_that_meets_it("variant-b")


def test_variants_that_share_a_type_only_where_both_fall_short_are_not_ambiguous():
    # The type is shared by the two variants that fall short, not with one that meets the
    # criterion, so the note names it once and nothing is ambiguous.
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sqlite"}),
        ),
        _record(
            "variant-c",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sqlite"}),
        ),
    )
    (row,) = assemble_rows(facts)
    assert row.notes == (
        "Every shipped expectation: not met for SQLite databases.",
        "Expectation suite: not met for SQLite databases.",
    )


REASON = "a recorded reason"


@WITH_CASE_DESCRIPTIONS
def test_a_met_criterion_is_partial_when_a_record_excludes_a_case_under_a_declaration_for_it():
    (row,) = assemble_rows(
        _facts(
            _record(
                "variant-a",
                "Throwaway Source",
                tiers=ALL_THREE,
                exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"a_case": REASON}},
            )
        )
    )
    assert row.criteria_met == frozenset({GALLERY, SUITE, API})
    assert row.criteria_partial == frozenset({SUITE})
    assert row.tier is PublicTier.FULLY_SUPPORTED
    assert row.notes == (
        "Expectation suite: not run for the first-A check. Recorded reason: a recorded reason",
    )


@WITH_CASE_DESCRIPTIONS
def test_every_declaration_that_satisfies_a_criterion_can_make_it_partial():
    cases = {
        SupportTier.CANONICAL_EXPECTATIONS: SUITE,
        SupportTier.CURATED_SQL: SUITE,
        SupportTier.FLUENT_API: API,
        SupportTier.GALLERY: GALLERY,
    }
    for declared, expected in cases.items():
        (row,) = assemble_rows(
            _facts(
                _record(
                    "variant-a",
                    "Throwaway Source",
                    tiers=ALL_THREE | {SupportTier.CURATED_SQL},
                    exclusions={declared: {"a_case": REASON}},
                )
            )
        )
        assert row.criteria_partial == frozenset({expected}), declared


@WITH_CASE_DESCRIPTIONS
def test_a_met_criterion_is_partial_when_a_fluent_type_a_record_names_excludes_a_case():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        fluent_exclusions={"pandas": {"a_case": REASON}},
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_met == frozenset({GALLERY, SUITE, API})
    assert row.criteria_partial == frozenset({API})
    assert row.notes == (
        "Datasource API contract: not run for the first-A check. "
        "Recorded reason: a recorded reason",
    )


@WITH_CASE_DESCRIPTIONS
def test_one_variants_exclusion_makes_the_criterion_partial_for_the_whole_row():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"sqlite"}),
            exclusions={SupportTier.GALLERY: {"a_case": REASON}},
        ),
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_partial == frozenset({GALLERY})


@WITH_CASE_DESCRIPTIONS
def test_a_later_variants_fluent_type_exclusion_makes_the_datasource_api_criterion_partial():
    facts = _facts(
        _record("variant-a", "Throwaway Source", tiers=ALL_THREE),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"sqlite"}),
        ),
        fluent_exclusions={"sqlite": {"a_case": REASON}},
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_partial == frozenset({API})


@WITH_CASE_DESCRIPTIONS
def test_a_criterion_that_is_not_met_is_never_partial():
    # Exclusions are recorded under the expectation suite and datasource API criteria, yet the
    # record declares neither, so there is no coverage for an exception to qualify.
    facts = _facts(
        _record(
            "variant-a",
            "Throwaway Source",
            tiers=frozenset({SupportTier.GALLERY}),
            exclusions={
                SupportTier.CANONICAL_EXPECTATIONS: {"a_case": REASON},
                SupportTier.FLUENT_API: {"a_case": REASON},
            },
        ),
        fluent_exclusions={"pandas": {"a_case": REASON}},
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_met == frozenset({GALLERY})
    assert row.criteria_partial == frozenset()


@WITH_CASE_DESCRIPTIONS
def test_a_criterion_one_variant_lacks_is_not_partial_through_the_other_variants_exclusion():
    facts = _facts(
        _record(
            "variant-a",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.GALLERY: {"a_case": REASON}},
        ),
        _record(
            "variant-b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.FLUENT_API}),
            fluent_types=frozenset({"sqlite"}),
        ),
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_met == frozenset({API})
    assert row.criteria_partial == frozenset()


@WITH_CASE_DESCRIPTIONS
def test_no_exclusion_means_no_partial_criterion():
    facts = _facts(
        _record(
            "variant-a",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.GALLERY: {}},
        ),
        fluent_exclusions={"pandas": {}, "sqlite": {"a_case": REASON}},
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_met == frozenset({GALLERY, SUITE, API})
    assert row.criteria_partial == frozenset()


def test_the_real_registry_marks_exactly_the_recorded_exclusions_partial():
    # Read outside the isolation seam. The throwaway twins above show the same marking can fail
    # to appear, so these are not vacuous.
    rows = {row.public_name: row for row in assemble_rows(load_upstream_facts())}
    partial = {name: row.criteria_partial for name, row in rows.items() if row.criteria_partial}
    assert partial == {"ClickHouse": frozenset({SUITE}), "Pandas": frozenset({API})}
    assert rows["ClickHouse"].criteria_met == frozenset({SUITE, API})
    assert rows["Pandas"].criteria_met == frozenset({GALLERY, SUITE, API})


def test_a_public_name_with_surrounding_whitespace_fails_rather_than_publishing_a_lookalike():
    facts = _facts(
        _record("a", "Pandas", tiers=ALL_THREE),
        _record("b", "Pandas ", tiers=ALL_THREE),
    )
    with pytest.raises(ValueError) as raised:
        assemble_rows(facts)
    assert str(raised.value) == (
        "The public name 'Pandas ' (record labelled 'b') has leading or trailing whitespace, so "
        "it would publish a row that looks identical to one without. Remove the whitespace from "
        "the declaration."
    )
    with pytest.raises(ValueError, match=r"' Pandas' \(record labelled 'c'\)"):
        assemble_rows(_facts(_record("c", " Pandas")))


def test_two_public_names_differing_only_in_case_fail_rather_than_publishing_a_lookalike():
    facts = _facts(
        _record("a", "pandas", tiers=ALL_THREE),
        _record("b", "Pandas", tiers=ALL_THREE),
    )
    with pytest.raises(ValueError) as raised:
        assemble_rows(facts)
    assert str(raised.value) == (
        "The public names 'Pandas' and 'pandas' differ only in letter case, so they would "
        "publish two rows that look like one. Declare one spelling for the data source."
    )


# ---------------------------------------------------------------------------------------------
# Notes that qualify what a row's tier means.
#
# Expected notes are written out in full. Deriving them with the code under test could not fail.
# ---------------------------------------------------------------------------------------------

COVERED_NOTE = (
    "The datasource API contract is verified for this data source, but that criterion is not "
    "shown as met because no continuous-integration lane is declared for it; what is missing "
    "is that declaration, not test evidence."
)
MANAGED_NOTE = (
    "No continuous-integration lane exercises the real service: reaching it needs credentials "
    "this repository does not provision."
)
ORACLE_NOTE = "Tested against Oracle 21c. 19c expected, not verified in CI."
CREDENTIALS = DataSourceProvisioning.EXTERNAL_CREDENTIALS
CONTAINER = DataSourceProvisioning.LOCAL_CONTAINER

ALL_LABELS = ("a", "b", "c")
POSITIONS = pytest.mark.parametrize("decisive", [0, 1, 2], ids=["first", "middle", "last"])
FLUENT_TYPES_BY_POSITION = ("pandas", "pandas_filesystem", "pandas_s3")
DESCRIPTION_BY_POSITION = (PANDAS_DESCRIPTION, CSV_DESCRIPTION, S3_PANDAS_DESCRIPTION)

# Every character the producers must treat as ending a line.
LINE_BREAKS = pytest.mark.parametrize(
    "character",
    ["\n", "\r", "\r\n", "\x0b", "\x0c", "\x85", "\N{LINE SEPARATOR}", "\N{PARAGRAPH SEPARATOR}"],
    ids=["lf", "cr", "crlf", "vt", "ff", "nel", "ls", "ps"],
)


def _notes_of(facts: UpstreamFacts, public_name: str) -> Tuple[str, ...]:
    return _row(assemble_rows(facts), public_name).notes


def _three_variants(decisive: int, **overrides_for_decisive) -> Tuple[DataSourceSpec, ...]:
    """Three records sharing one name that agree on every criterion; only ``decisive`` differs."""
    records = []
    for position, label in enumerate(ALL_LABELS):
        kwargs = overrides_for_decisive if position == decisive else {}
        records.append(
            _record(
                label,
                "Throwaway Source",
                tiers=ALL_THREE,
                fluent_types=frozenset({FLUENT_TYPES_BY_POSITION[position]}),
                **kwargs,
            )
        )
    return tuple(records)


# --- covered by a suite the record cannot claim ----------------------------------------------


def test_a_record_covered_but_unable_to_claim_gets_a_note_stating_both_halves():
    facts = _facts(_record("only", "Throwaway Source"), covered=frozenset({"only"}))
    assert _notes_of(facts, "Throwaway Source") == (COVERED_NOTE,)


def test_the_covered_note_disappears_with_the_declaration_it_came_from():
    record = _record("only", "Throwaway Source")
    covered = _facts(record, covered=frozenset({"only"}))
    assert _notes_of(covered, "Throwaway Source") == (COVERED_NOTE,)
    assert _notes_of(_facts(record, covered=frozenset()), "Throwaway Source") == ()
    other = _facts(record, covered=frozenset({"someone-else"}))
    assert _notes_of(other, "Throwaway Source") == ()


@POSITIONS
def test_the_covered_note_is_found_whichever_record_of_the_row_is_listed(decisive):
    facts = _facts(*_three_variants(decisive), covered=frozenset({ALL_LABELS[decisive]}))
    assert _notes_of(facts, "Throwaway Source") == (COVERED_NOTE,)


def test_the_covered_note_is_given_once_when_several_records_of_a_row_are_listed():
    facts = _facts(*_three_variants(0), covered=frozenset(ALL_LABELS))
    assert _notes_of(facts, "Throwaway Source") == (COVERED_NOTE,)


# --- a recorded exclusion inside a criterion's suite -----------------------------------------

BACKTICKED = (
    "Insert keys rows by the bind name, raising a `KeyError`. An issue still needs to be filed."
)


def test_a_recorded_case_exclusion_is_named_with_its_reason_printed_exactly_as_recorded():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=frozenset({SupportTier.CURATED_SQL, SupportTier.FLUENT_API}),
        exclusions={SupportTier.CURATED_SQL: {"quoted_identifiers": BACKTICKED}},
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for column names that need quoting. "
        f"Recorded reason: {BACKTICKED}",
    )


@WITH_CASE_DESCRIPTIONS
@pytest.mark.parametrize(
    ("declaration", "label"),
    [
        (SupportTier.GALLERY, "Every shipped expectation"),
        (SupportTier.CANONICAL_EXPECTATIONS, "Expectation suite"),
        (SupportTier.CURATED_SQL, "Expectation suite"),
        (SupportTier.FLUENT_API, "Datasource API contract"),
    ],
    ids=lambda value: getattr(value, "name", value),
)
def test_an_exclusion_under_any_declaration_is_attributed_to_the_criterion_it_satisfies(
    declaration, label
):
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={declaration: {"some_case": "Not run."}},
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        f"{label}: not run for the shared check. Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
def test_the_exclusion_note_disappears_with_the_exclusion():
    with_it = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
    )
    without = _record("only", "Throwaway Source", tiers=ALL_THREE)
    assert len(_notes_of(_facts(with_it), "Throwaway Source")) == 1
    assert _notes_of(_facts(without), "Throwaway Source") == ()


@WITH_CASE_DESCRIPTIONS
@POSITIONS
def test_a_variants_exclusion_is_found_and_scoped_to_it_wherever_it_sorts(decisive):
    facts = _facts(
        *_three_variants(
            decisive,
            exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
        )
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Expectation suite, {DESCRIPTION_BY_POSITION[decisive]}: not run for the shared check. "
        f"Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
@POSITIONS
def test_a_fluent_types_exclusion_is_found_and_scoped_to_it_wherever_it_sorts(decisive):
    facts = _facts(
        *_three_variants(decisive),
        fluent_exclusions={FLUENT_TYPES_BY_POSITION[decisive]: {"some_case": "Not run."}},
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Datasource API contract, {DESCRIPTION_BY_POSITION[decisive]}: "
        f"not run for the shared check. Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
def test_a_single_record_with_a_single_type_scopes_nothing():
    facts = _facts(
        _record("only", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"pandas_s3"})),
        fluent_exclusions={"pandas_s3": {"some_case": "Not run."}},
    )
    assert _notes_of(facts, "Throwaway Source") == (
        "Datasource API contract: not run for the shared check. Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
def test_a_single_record_reached_through_several_types_scopes_to_the_type_that_excludes():
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        ),
        fluent_exclusions={"pandas_s3": {"some_case": "Not run."}},
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Datasource API contract, {S3_PANDAS_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
@pytest.mark.parametrize(
    ("excluding", "description"),
    [("pandas_s3", S3_PANDAS_DESCRIPTION), ("spark_s3", S3_SPARK_DESCRIPTION)],
    ids=["first-type", "last-type"],
)
def test_each_type_of_a_multi_type_record_is_checked_for_exclusions(excluding, description):
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        ),
        fluent_exclusions={excluding: {"some_case": "Not run."}},
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Datasource API contract, {description}: not run for the shared check. "
        f"Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
def test_exclusions_on_every_type_of_a_multi_type_record_each_get_a_note_in_a_fixed_order():
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        ),
        fluent_exclusions={
            "spark_s3": {"some_case": "Not run."},
            "pandas_s3": {"some_case": "Not run."},
        },
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Datasource API contract, {S3_SPARK_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Not run.",
        f"Datasource API contract, {S3_PANDAS_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Not run.",
    )


@WITH_CASE_DESCRIPTIONS
def test_exclusions_under_both_declarations_of_one_criterion_are_both_noted():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE | {SupportTier.CURATED_SQL},
        exclusions={
            SupportTier.CANONICAL_EXPECTATIONS: {"canonical_case": "Canonical reason."},
            SupportTier.CURATED_SQL: {"curated_case": "Curated reason."},
        },
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the canonical check. Recorded reason: Canonical reason.",
        "Expectation suite: not run for the curated check. Recorded reason: Curated reason.",
    )


@WITH_CASE_DESCRIPTIONS
def test_a_suites_exclusions_and_a_fluent_types_exclusions_are_kept_to_their_own_criteria():
    # One record, one decisive exclusion per criterion: neither may leak into the other's note.
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"suite_case": "Suite reason."}},
        ),
        fluent_exclusions={"pandas": {"api_case": "Api reason."}},
    )
    assert _notes_of(facts, "Throwaway Source") == (
        "Expectation suite: not run for the suite check. Recorded reason: Suite reason.",
        "Datasource API contract: not run for the API check. Recorded reason: Api reason.",
    )


@WITH_CASE_DESCRIPTIONS
def test_the_exclusion_producer_follows_the_criteria_assembly_marked_partial():
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
        )
    )
    (row,) = assemble_rows(facts)
    assert row.criteria_partial == frozenset({SUITE})
    assert len(recorded_exclusion_notes(row, facts)) == 1
    unmarked = replace(row, criteria_partial=frozenset())
    assert recorded_exclusion_notes(unmarked, facts) == ()


@WITH_CASE_DESCRIPTIONS
def test_groups_are_ordered_by_their_first_case_not_their_last():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={
            SupportTier.CANONICAL_EXPECTATIONS: {
                "a_case": "Wide reason.",
                "z_case": "Wide reason.",
                "m_case": "Narrow reason.",
            }
        },
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the first-A check and the last-Z check. "
        "Recorded reason: Wide reason.",
        "Expectation suite: not run for the middle-M check. Recorded reason: Narrow reason.",
    )


@WITH_CASE_DESCRIPTIONS
def test_a_reason_is_printed_exactly_as_recorded_including_its_surrounding_spaces():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "  Padded.  "}},
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the shared check. Recorded reason:   Padded.  ",
    )


@WITH_CASE_DESCRIPTIONS
def test_several_cases_sharing_a_reason_are_named_together_in_sorted_order():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={
            SupportTier.CANONICAL_EXPECTATIONS: {"zeta": "Same.", "alpha": "Same.", "mid": "Same."}
        },
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the Z-sorting check, the M-sorting check and the "
        "A-sorting check. Recorded reason: Same.",
    )


@WITH_CASE_DESCRIPTIONS
def test_two_cases_are_joined_with_and_alone():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"beta": "Same.", "alpha": "Same."}},
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the Z-sorting check and the Y-sorting check. "
        "Recorded reason: Same.",
    )


@WITH_CASE_DESCRIPTIONS
def test_cases_with_different_reasons_get_separate_notes_ordered_by_first_case():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={
            SupportTier.CANONICAL_EXPECTATIONS: {"zeta": "Zed reason.", "alpha": "Alpha reason."}
        },
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Expectation suite: not run for the Z-sorting check. Recorded reason: Alpha reason.",
        "Expectation suite: not run for the A-sorting check. Recorded reason: Zed reason.",
    )


@WITH_CASE_DESCRIPTIONS
def test_two_variants_with_the_same_reason_are_not_merged_across_variants():
    shared = {SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Same."}}
    facts = _facts(
        _record("a", "Throwaway Source", tiers=ALL_THREE, exclusions=shared),
        _record(
            "b",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_filesystem"}),
            exclusions=shared,
        ),
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Expectation suite, {CSV_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Same.",
        f"Expectation suite, {PANDAS_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Same.",
    )


@WITH_CASE_DESCRIPTIONS
def test_exclusion_notes_follow_criterion_order_whatever_order_they_were_declared_in():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={
            SupportTier.FLUENT_API: {"api_case": "Api reason."},
            SupportTier.CANONICAL_EXPECTATIONS: {"suite_case": "Suite reason."},
            SupportTier.GALLERY: {"gallery_case": "Gallery reason."},
        },
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (
        "Every shipped expectation: not run for the gallery check. "
        "Recorded reason: Gallery reason.",
        "Expectation suite: not run for the suite check. Recorded reason: Suite reason.",
        "Datasource API contract: not run for the API check. Recorded reason: Api reason.",
    )


@WITH_CASE_DESCRIPTIONS
def test_an_exclusion_inside_a_criterion_the_row_does_not_meet_is_not_noted():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=frozenset({SupportTier.FLUENT_API}),
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
    )
    assert _notes_of(_facts(record), "Throwaway Source") == ()


@WITH_CASE_DESCRIPTIONS
def test_a_fluent_type_with_an_exclusion_that_no_description_names_fails_rather_than_printing_it():
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_s3", "mystery_type"}),
        ),
        fluent_exclusions={"mystery_type": {"some_case": "Not run."}},
    )
    with pytest.raises(UpstreamDeclarationError, match="'mystery_type'"):
        assemble_rows(facts)


@WITH_CASE_DESCRIPTIONS
@LINE_BREAKS
def test_a_line_break_in_a_recorded_reason_fails_instead_of_mangling_a_row(character):
    reason = f"First line.{character}Second line."
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": reason}},
    )
    with pytest.raises(ValueError, match="recorded reason for case 'some_case'") as raised:
        assemble_rows(_facts(record))
    assert "line break" in str(raised.value)


@WITH_CASE_DESCRIPTIONS
@LINE_BREAKS
def test_a_trailing_or_leading_line_break_in_a_reason_also_fails(character):
    for reason in (f"Reason.{character}", f"{character}Reason."):
        record = _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": reason}},
        )
        with pytest.raises(ValueError, match="line break"):
            assemble_rows(_facts(record))


@WITH_CASE_DESCRIPTIONS
def test_a_line_break_in_a_fluent_types_recorded_reason_fails_too():
    facts = _facts(
        _record("only", "Throwaway Source", tiers=ALL_THREE),
        fluent_exclusions={"pandas": {"some_case": "One.\nTwo."}},
    )
    with pytest.raises(ValueError, match="recorded reason for case 'some_case'"):
        assemble_rows(facts)


def test_a_case_key_with_a_line_break_and_no_description_fails_without_printing_the_key():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"two\nlines": "Reason."}},
    )
    with pytest.raises(UpstreamDeclarationError, match="CASE_DESCRIPTIONS does not describe it"):
        assemble_rows(_facts(record))


@LINE_BREAKS
def test_a_line_break_in_a_case_description_fails_instead_of_mangling_a_row(monkeypatch, character):
    monkeypatch.setitem(
        upstream_declarations.CASE_DESCRIPTIONS, "some_case", f"first{character}second"
    )
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
    )
    with pytest.raises(ValueError, match="description of case 'some_case'") as raised:
        assemble_rows(_facts(record))
    assert type(raised.value) is ValueError
    assert "line break" in str(raised.value)


@pytest.mark.parametrize("decisive", [0, 1, 2], ids=["first", "middle", "last"])
def test_every_case_description_in_a_group_is_checked_for_a_line_break(monkeypatch, decisive):
    cases = ("alpha", "beta", "mid")  # one reason, so one group of three; sorted by key
    monkeypatch.setitem(upstream_declarations.CASE_DESCRIPTIONS, cases[decisive], "first\nsecond")
    for case in cases:
        if case != cases[decisive]:
            monkeypatch.setitem(upstream_declarations.CASE_DESCRIPTIONS, case, f"fine {case}")
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: dict.fromkeys(cases, "Same.")},
    )
    with pytest.raises(ValueError, match=f"description of case '{cases[decisive]}'"):
        assemble_rows(_facts(record))


def test_a_case_without_a_description_fails_naming_the_key_rather_than_printing_it():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CURATED_SQL: {"undescribed_case": "Not run."}},
    )
    with pytest.raises(UpstreamDeclarationError, match="'undescribed_case'"):
        assemble_rows(_facts(record))
    fluent = _facts(
        _record("only", "Throwaway Source", tiers=ALL_THREE),
        fluent_exclusions={"pandas": {"undescribed_case": "Not run."}},
    )
    with pytest.raises(UpstreamDeclarationError, match="'undescribed_case'"):
        assemble_rows(fluent)


def test_a_note_names_the_case_by_its_description_and_never_by_its_key():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CURATED_SQL: {"quoted_identifiers": "Not run."}},
    )
    (note,) = _notes_of(_facts(record), "Throwaway Source")
    assert note == (
        "Expectation suite: not run for column names that need quoting. Recorded reason: Not run."
    )
    assert "quoted_identifiers" not in note


@WITH_CASE_DESCRIPTIONS
def test_no_note_prints_a_case_key_for_a_group_of_several_cases():
    record = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"zeta": "Same.", "alpha": "Same."}},
    )
    (note,) = _notes_of(_facts(record), "Throwaway Source")
    assert note == (
        "Expectation suite: not run for the Z-sorting check and the A-sorting check. "
        "Recorded reason: Same."
    )
    assert "alpha" not in note
    assert "zeta" not in note


def test_a_line_break_in_a_variant_description_fails_instead_of_mangling_a_heading(monkeypatch):
    monkeypatch.setitem(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, "pandas", "a\nb")
    facts = _facts(
        *_three_variants(
            0, exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}}
        )
    )
    with pytest.raises(ValueError) as raised:
        assemble_rows(facts)
    assert type(raised.value) is ValueError
    assert str(raised.value) == (
        "The variant description for 'Throwaway Source' contains a line break, which would "
        "break the table row it is printed in. Rewrite it as a single line where it is "
        "declared: 'a\\nb'"
    )


@WITH_CASE_DESCRIPTIONS
def test_a_line_break_in_a_fluent_types_description_fails_instead_of_mangling_a_heading(
    monkeypatch,
):
    monkeypatch.setitem(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, "pandas_s3", "a\nb")
    facts = _facts(
        _record(
            "only",
            "Throwaway Source",
            tiers=ALL_THREE,
            fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        ),
        fluent_exclusions={"pandas_s3": {"some_case": "Not run."}},
    )
    with pytest.raises(ValueError, match="variant description for 'Throwaway Source'"):
        assemble_rows(facts)


@WITH_CASE_DESCRIPTIONS
def test_a_single_line_reason_is_accepted_where_the_same_reason_with_a_break_is_not():
    # The control for the failing cases above: nothing but the break differs.
    ok = _record(
        "only",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "First. Second."}},
    )
    assert len(_notes_of(_facts(ok), "Throwaway Source")) == 1


# --- the descriptions of excluded cases, pinned to the exclusions both ways --------------------

REAL_CASE_KEYS = (
    "quoted_identifiers",
    "update_replaces_configuration",
    "create_or_update_replaces_when_present",
    "create_or_update_persists_one_entry",
)


def test_the_declared_case_descriptions_are_exactly_the_four_recorded_exclusions():
    assert set(upstream_declarations.CASE_DESCRIPTIONS) == set(REAL_CASE_KEYS)
    assert upstream_declarations.CASE_DESCRIPTIONS == {
        "quoted_identifiers": "column names that need quoting",
        "update_replaces_configuration": "replacing a datasource's configuration on update",
        "create_or_update_replaces_when_present": (
            "replacing an existing datasource on create-or-update"
        ),
        "create_or_update_persists_one_entry": (
            "keeping a single saved entry after create-or-update"
        ),
    }


def test_the_real_exclusions_are_the_four_the_descriptions_cover():
    facts = load_upstream_facts()
    recorded = {
        case
        for spec in facts.specs
        for cases in spec.tier_case_exclusions.values()
        for case in cases
    }
    fluent = {case for cases in facts.fluent_case_exclusions.values() for case in cases}
    assert recorded == {"quoted_identifiers"}
    assert fluent == set(REAL_CASE_KEYS) - {"quoted_identifiers"}


def _excluding_record() -> DataSourceSpec:
    return _record(
        "excluding",
        "Throwaway Source",
        tiers=ALL_THREE,
        exclusions={SupportTier.CURATED_SQL: {"record_case": "Not run."}},
    )


def _guard(descriptions, *, specs=None, fluent=None) -> None:
    upstream_declarations.check_case_descriptions(
        (_excluding_record(),) if specs is None else specs,
        {"pandas": {"fluent_case": "Not run."}} if fluent is None else fluent,
        descriptions,
    )


def test_the_case_guard_accepts_descriptions_that_are_exactly_the_excluded_cases():
    _guard({"record_case": "a record case", "fluent_case": "a fluent case"})


@pytest.mark.parametrize("missing", ["record_case", "fluent_case"])
def test_the_case_guard_fails_naming_an_excluded_case_that_has_no_description(missing):
    described = {"record_case": "a record case", "fluent_case": "a fluent case"}
    del described[missing]
    with pytest.raises(UpstreamDeclarationError) as raised:
        _guard(described)
    assert str(raised.value).startswith(f"A declaration excludes case(s) ['{missing}'] ")


def test_the_case_guard_names_every_undescribed_case_in_sorted_order():
    with pytest.raises(UpstreamDeclarationError) as raised:
        _guard({})
    assert str(raised.value).startswith(
        "A declaration excludes case(s) ['fluent_case', 'record_case'] "
    )


@pytest.mark.parametrize("stale", ["one_extra", "another_extra"])
def test_the_case_guard_fails_naming_a_description_no_declaration_excludes(stale):
    described = {"record_case": "a record case", "fluent_case": "a fluent case", stale: "stale"}
    with pytest.raises(UpstreamDeclarationError) as raised:
        _guard(described)
    assert str(raised.value).startswith(f"CASE_DESCRIPTIONS describes case(s) ['{stale}'] that ")


def test_the_case_guard_notices_a_description_whose_exclusion_was_removed_from_either_source():
    described = {"record_case": "a record case", "fluent_case": "a fluent case"}
    with pytest.raises(UpstreamDeclarationError, match=r"\['record_case'\]"):
        _guard(described, specs=(_record("quiet", "Throwaway Source"),))
    with pytest.raises(UpstreamDeclarationError, match=r"\['fluent_case'\]"):
        _guard(described, fluent={})


SPREAD_CASES = ("case_one", "case_two", "case_three")
SPREAD_DESCRIPTIONS = {case: f"description of {case}" for case in SPREAD_CASES}
SPREAD_TIERS = (SupportTier.CURATED_SQL, SupportTier.GALLERY, SupportTier.FLUENT_API)


def _spread_over_records() -> Tuple[DataSourceSpec, ...]:
    return tuple(
        _record(label, "Throwaway Source", exclusions={SPREAD_TIERS[0]: {case: "Not run."}})
        for label, case in zip("abc", SPREAD_CASES, strict=True)
    )


def _spread_over_tiers() -> Tuple[DataSourceSpec, ...]:
    return (
        _record(
            "only",
            "Throwaway Source",
            exclusions={
                tier: {case: "Not run."}
                for tier, case in zip(SPREAD_TIERS, SPREAD_CASES, strict=True)
            },
        ),
    )


def _spread_over_types() -> Tuple[Mapping[str, Mapping[str, str]], ...]:
    return (
        {
            "pandas": {SPREAD_CASES[0]: "x"},
            "spark": {SPREAD_CASES[1]: "x"},
            "sql": {SPREAD_CASES[2]: "x"},
        },
    )


SPREAD_SOURCES = pytest.mark.parametrize(
    ("specs", "fluent"),
    [
        (_spread_over_records(), {}),
        (_spread_over_tiers(), {}),
        ((), _spread_over_types()[0]),
    ],
    ids=["over-records", "over-tiers-of-one-record", "over-fluent-types"],
)


@SPREAD_SOURCES
def test_the_case_guard_accepts_cases_spread_over_every_record_tier_and_type(specs, fluent):
    # The control for the failing cases below: a description for each of the three is not stale.
    _guard(SPREAD_DESCRIPTIONS, specs=specs, fluent=fluent)


@SPREAD_SOURCES
@pytest.mark.parametrize("decisive", [0, 1, 2], ids=["first", "middle", "last"])
def test_the_case_guard_checks_every_record_tier_and_type_for_an_undescribed_case(
    specs, fluent, decisive
):
    described = {k: v for k, v in SPREAD_DESCRIPTIONS.items() if k != SPREAD_CASES[decisive]}
    with pytest.raises(UpstreamDeclarationError) as raised:
        _guard(described, specs=specs, fluent=fluent)
    assert str(raised.value).startswith(
        f"A declaration excludes case(s) ['{SPREAD_CASES[decisive]}'] "
    )


@pytest.mark.parametrize("key", REAL_CASE_KEYS)
def test_the_loader_fails_when_a_declared_exclusion_has_no_description(monkeypatch, key):
    load_upstream_facts()  # control: the declarations as they stand load
    monkeypatch.delitem(upstream_declarations.CASE_DESCRIPTIONS, key)
    with pytest.raises(UpstreamDeclarationError) as raised:
        load_upstream_facts()
    assert str(raised.value).startswith(f"A declaration excludes case(s) ['{key}'] ")


def test_the_loader_fails_when_a_description_names_a_case_no_declaration_excludes(monkeypatch):
    monkeypatch.setitem(upstream_declarations.CASE_DESCRIPTIONS, "extra_case", "an extra case")
    with pytest.raises(UpstreamDeclarationError) as raised:
        load_upstream_facts()
    assert str(raised.value).startswith("CASE_DESCRIPTIONS describes case(s) ['extra_case'] ")


def test_the_loader_fails_when_the_registry_no_longer_excludes_a_described_record_case():
    with isolated_registry():
        register_data_source(ORACLE_SPEC)  # no record excludes quoted_identifiers
        with pytest.raises(UpstreamDeclarationError) as raised:
            load_upstream_facts()
    assert str(raised.value).startswith(
        "CASE_DESCRIPTIONS describes case(s) ['quoted_identifiers'] "
    )


def test_the_loader_reads_the_fluent_exclusions_it_checks_from_the_accessor(monkeypatch):
    monkeypatch.setattr(upstream_declarations, "case_exclusions_by_type", lambda: {})
    with pytest.raises(UpstreamDeclarationError) as raised:
        load_upstream_facts()
    assert str(raised.value).startswith(
        "CASE_DESCRIPTIONS describes case(s) ['create_or_update_persists_one_entry', "
        "'create_or_update_replaces_when_present', 'update_replaces_configuration'] "
    )


def test_the_loader_hands_over_the_fluent_exclusions_it_checked(monkeypatch):
    altered = {"pandas": {"update_replaces_configuration": "x"}}
    monkeypatch.setattr(upstream_declarations, "case_exclusions_by_type", lambda: altered)
    monkeypatch.setattr(
        upstream_declarations,
        "CASE_DESCRIPTIONS",
        {"quoted_identifiers": "q", "update_replaces_configuration": "u"},
    )
    assert load_upstream_facts().fluent_case_exclusions == altered


SHORT_TYPES = ("pandas", "pandas_s3", "pandas_gcs")
GCS_PANDAS_DESCRIPTION = "Google Cloud Storage objects read with pandas"


def _short_variants() -> Tuple[DataSourceSpec, ...]:
    """Three records fall short of every criterion a fourth meets, told apart by their types."""
    shorts = [
        _record(label, "Throwaway Source", fluent_types=frozenset({fluent_type}))
        for label, fluent_type in zip("abc", SHORT_TYPES, strict=True)
    ]
    meeting = _record(
        "d", "Throwaway Source", tiers=ALL_THREE, fluent_types=frozenset({"pandas_filesystem"})
    )
    return (*shorts, meeting)


def test_variants_falling_short_are_all_named_when_each_description_is_a_single_line():
    # The control for the failing cases below.
    (row,) = assemble_rows(_facts(*_short_variants()))
    assert row.notes[0] == (
        "Every shipped expectation: not met for Amazon S3 objects read with pandas; "
        "Google Cloud Storage objects read with pandas; in-memory pandas DataFrames."
    )


@pytest.mark.parametrize(
    ("position", "fluent_type"),
    list(enumerate(SHORT_TYPES)),
    ids=["first", "middle", "last"],
)
def test_a_line_break_in_any_variant_description_of_a_disagreement_note_fails(
    monkeypatch, position, fluent_type
):
    monkeypatch.setitem(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, fluent_type, "a\nb")
    with pytest.raises(ValueError) as raised:
        assemble_rows(_facts(*_short_variants()))
    assert type(raised.value) is ValueError
    assert str(raised.value) == (
        "The variant description in a disagreement note for 'Throwaway Source' contains a line "
        "break, which would break the table row it is printed in. Rewrite it as a single line "
        "where it is declared: 'a\\nb'"
    )


# --- connection paths no record names --------------------------------------------------------


def test_the_footnote_lists_each_uncovered_path_by_description_in_sorted_order():
    facts = _facts(
        _record("only", "Throwaway Source"),
        uncovered_paths=frozenset({"spark", "pandas_dbfs", "fabric_powerbi", "spark_dbfs"}),
    )
    assert uncovered_connection_paths_note(facts) == (
        "Connection paths GX ships that no row above covers: Databricks File System paths read "
        "with Spark; Databricks File System paths read with pandas; Power BI semantic models; "
        "Spark in-memory DataFrames."
    )


def test_the_footnote_names_a_single_path_without_separators():
    facts = _facts(_record("only", "x"), uncovered_paths=frozenset({"spark"}))
    assert uncovered_connection_paths_note(facts) == (
        "Connection paths GX ships that no row above covers: Spark in-memory DataFrames."
    )


def test_the_footnote_fails_when_a_description_spans_more_than_one_line(monkeypatch):
    # "spark" is an uncovered path here, so its description is printed in the footnote.
    monkeypatch.setitem(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, "spark", "a\nb")
    facts = _facts(_record("only", "x"), uncovered_paths=frozenset({"spark"}))
    with pytest.raises(ValueError) as raised:
        uncovered_connection_paths_note(facts)
    assert type(raised.value) is ValueError
    assert str(raised.value) == (
        "The uncovered connection paths footnote contains a line break, which would break the "
        "table row it is printed in. Rewrite it as a single line where it is declared: "
        "'Connection paths GX ships that no row above covers: a\\nb.'"
    )


def test_the_footnote_disappears_when_no_path_is_uncovered():
    assert uncovered_connection_paths_note(_facts(_record("only", "x"))) is None


def test_the_footnote_fails_naming_a_path_nothing_describes_rather_than_printing_it():
    facts = _facts(
        _record("only", "x"), uncovered_paths=frozenset({"spark", "mystery_path", "another"})
    )
    with pytest.raises(UpstreamDeclarationError) as raised:
        uncovered_connection_paths_note(facts)
    assert "['another', 'mystery_path']" in str(raised.value)


def test_the_real_footnote_names_the_four_paths_no_record_names():
    footnote = uncovered_connection_paths_note(load_upstream_facts())
    assert footnote == (
        "Connection paths GX ships that no row above covers: Databricks File System paths read "
        "with Spark; Databricks File System paths read with pandas; Power BI semantic models; "
        "Spark in-memory DataFrames."
    )


def test_the_loader_fails_when_the_pinned_uncovered_path_literal_names_an_undescribed_path(
    monkeypatch,
):
    # Through the loader, not the pure guard: a loader that stopped passing the literal to the
    # guard would leave the guard's own tests green.
    load_upstream_facts()  # control: the declarations as they stand load
    altered = upstream_declarations.FLUENT_TYPES_NAMED_BY_NO_RECORD | {"mystery_path"}
    monkeypatch.setattr(upstream_declarations, "FLUENT_TYPES_NAMED_BY_NO_RECORD", altered)
    with pytest.raises(UpstreamDeclarationError, match="mystery_path"):
        load_upstream_facts()


def test_the_loader_hands_the_pinned_uncovered_path_literal_to_the_facts_unaltered():
    assert load_upstream_facts().fluent_types_named_by_no_record == frozenset(
        {"spark", "pandas_dbfs", "spark_dbfs", "fabric_powerbi"}
    )


# --- managed-service provisioning ------------------------------------------------------------


def test_external_credentials_with_a_recorded_note_produces_the_managed_service_note():
    record = _record(
        "only", "Throwaway Source", provisioning=CREDENTIALS, provisioning_note="Needs a tenant."
    )
    assert _notes_of(_facts(record), "Throwaway Source") == (MANAGED_NOTE,)


def test_external_credentials_alone_produces_no_managed_service_note():
    record = _record("only", "Throwaway Source", provisioning=CREDENTIALS, lane=LANE)
    assert _notes_of(_facts(record), "Throwaway Source") == ()


def test_a_recorded_note_alone_produces_no_managed_service_note():
    record = _record(
        "only",
        "Throwaway Source",
        provisioning=CONTAINER,
        provisioning_note="Costs seven surfaces.",
    )
    assert _notes_of(_facts(record), "Throwaway Source") == ()


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_a_blank_recorded_note_does_not_count_as_a_recorded_note(blank):
    record = _record("only", "Throwaway Source", provisioning=CREDENTIALS, provisioning_note=blank)
    assert _notes_of(_facts(record), "Throwaway Source") == ()


def test_the_two_halves_must_hold_on_one_record_not_across_a_row():
    facts = _facts(
        _record(
            "a", "Throwaway Source", provisioning=CREDENTIALS, fluent_types=frozenset({"pandas"})
        ),
        _record(
            "b",
            "Throwaway Source",
            provisioning=CONTAINER,
            provisioning_note="Costs seven surfaces.",
            fluent_types=frozenset({"pandas_filesystem"}),
        ),
    )
    assert _notes_of(facts, "Throwaway Source") == ()


@POSITIONS
def test_the_managed_service_note_is_found_whichever_record_of_the_row_holds_both_halves(decisive):
    facts = _facts(
        *_three_variants(decisive, provisioning=CREDENTIALS, provisioning_note="Needs a tenant.")
    )
    assert _notes_of(facts, "Throwaway Source") == (MANAGED_NOTE,)


def test_the_managed_service_note_is_given_once_when_every_record_of_the_row_qualifies():
    facts = _facts(
        *(
            _record(
                label,
                "Throwaway Source",
                provisioning=CREDENTIALS,
                provisioning_note="Needs a tenant.",
                fluent_types=frozenset({FLUENT_TYPES_BY_POSITION[position]}),
            )
            for position, label in enumerate(ALL_LABELS)
        )
    )
    assert _notes_of(facts, "Throwaway Source") == (MANAGED_NOTE,)


def _live_notes() -> dict:
    return {row.public_name: row.notes for row in assemble_rows(load_upstream_facts())}


def test_against_the_real_declarations_the_managed_service_note_is_microsoft_fabrics_alone():
    # Read outside the isolation seam; the throwaway twins above show each half can fail.
    carrying = {name for name, notes in _live_notes().items() if MANAGED_NOTE in notes}
    assert carrying == {"Microsoft Fabric"}


def test_against_the_real_declarations_citus_whose_note_is_about_cost_produces_none():
    live = _live_notes()
    citus_specs = [s for s in load_upstream_facts().specs if s.public_name == "Citus"]
    assert [s.provisioning for s in citus_specs] == [CONTAINER]
    assert citus_specs[0].provisioning_note  # the recorded note is real, which is the point
    assert MANAGED_NOTE not in live["Citus"]


def test_against_the_real_declarations_credential_gated_data_sources_with_lanes_produce_none():
    live = _live_notes()
    by_name = {s.public_name: s for s in load_upstream_facts().specs}
    for name in ("BigQuery", "Databricks (SQL)", "Redshift", "Snowflake"):
        assert by_name[name].provisioning is CREDENTIALS, name
        assert by_name[name].ci_lane is not None, name
        assert by_name[name].provisioning_note is None, name
        assert MANAGED_NOTE not in live[name], name


def test_the_externally_provisioned_data_sources_with_no_note_and_no_lane_get_no_such_note():
    # The residual, stated from the declarations rather than counted by hand: credentials this
    # repository does not hold, no note to key on, and no lane. The remedy is upstream.
    specs = load_upstream_facts().specs
    residual = {
        s.public_name
        for s in specs
        if s.provisioning is CREDENTIALS and not s.provisioning_note and s.ci_lane is None
    }
    assert residual == {"AlloyDB", "Amazon Aurora PostgreSQL", "Azure Blob Storage", "Neon"}
    live = _live_notes()
    for name in residual:
        assert MANAGED_NOTE not in live[name], name


# --- the version bound -----------------------------------------------------------------------


def test_a_row_named_in_the_version_map_carries_its_note_verbatim(monkeypatch):
    note = "Tested against Throwaway 9.2. 9.0 expected, not verified in CI."
    monkeypatch.setitem(upstream_declarations.TESTED_VERSION_NOTES, "Throwaway Source", note)
    facts = _facts(
        _record("a", "Aardvark Source"),
        _record("b", "Throwaway Source"),
        _record("c", "Zebra Source"),
    )
    rows = {row.public_name: row.notes for row in assemble_rows(facts)}
    assert rows == {"Aardvark Source": (), "Throwaway Source": (note,), "Zebra Source": ()}


def test_the_version_note_is_looked_up_when_rows_are_built_not_when_the_module_loads(monkeypatch):
    monkeypatch.setattr(
        upstream_declarations, "TESTED_VERSION_NOTES", {"Throwaway Source": "Replaced map."}
    )
    assert _notes_of(_facts(_record("a", "Throwaway Source")), "Throwaway Source") == (
        "Replaced map.",
    )


def test_the_version_note_is_keyed_by_the_whole_public_name(monkeypatch):
    monkeypatch.setitem(upstream_declarations.TESTED_VERSION_NOTES, "Throwaway", "Bound.")
    assert _notes_of(_facts(_record("a", "Throwaway Source")), "Throwaway Source") == ()


def test_a_line_break_in_a_version_note_fails(monkeypatch):
    monkeypatch.setitem(
        upstream_declarations.TESTED_VERSION_NOTES, "Throwaway Source", "One.\nTwo."
    )
    with pytest.raises(ValueError, match="version note for 'Throwaway Source'"):
        assemble_rows(_facts(_record("a", "Throwaway Source")))


def test_the_real_oracle_row_carries_the_settled_wording_and_no_other_row_carries_it():
    live = _live_notes()
    assert live["Oracle"] == (ORACLE_NOTE,)
    assert [name for name, notes in live.items() if ORACLE_NOTE in notes] == ["Oracle"]


def test_the_loader_fails_when_a_version_key_resolves_to_no_published_row(monkeypatch):
    # Through the loader, so a loader that skipped the guard fails here by name.
    assert load_upstream_facts().specs  # control: the map as declared loads
    monkeypatch.setattr(
        upstream_declarations, "TESTED_VERSION_NOTES", {"No Such Source": "Tested against 1."}
    )
    with pytest.raises(
        UpstreamDeclarationError, match=r"'No Such Source'.*resolves to no published row"
    ):
        load_upstream_facts()


def test_the_loader_fails_when_the_named_data_source_declares_no_lane():
    with isolated_registry():
        register_data_source(replace(ORACLE_SPEC, ci_lane=None, tiers=frozenset()))
        with pytest.raises(
            UpstreamDeclarationError, match="declare no continuous-integration lane"
        ):
            load_upstream_facts()


def test_the_loader_accepts_the_same_data_source_once_its_record_declares_a_lane():
    # The control for the failing case above: only the lane differs.
    with isolated_registry():
        register_data_source(replace(ORACLE_SPEC, ci_lane=LANE))
        register_data_source(CLICKHOUSE_SPEC)
        names = {spec.public_name for spec in load_upstream_facts().specs}
        assert names == {"Oracle", "ClickHouse"}


def _oracle_variant(label: str, *, lane: bool) -> DataSourceSpec:
    return replace(
        ORACLE_SPEC,
        label=label,
        marker=label,
        ci_lane=LANE if lane else None,
        tiers=ORACLE_SPEC.tiers if lane else frozenset(),
    )


@POSITIONS
def test_the_loader_fails_when_any_one_of_several_records_under_the_name_has_no_lane(decisive):
    labels = ("oracle-a", "oracle-b", "oracle-c")
    with isolated_registry():
        for position, label in enumerate(labels):
            register_data_source(_oracle_variant(label, lane=position != decisive))
        with pytest.raises(UpstreamDeclarationError, match=rf"\['{labels[decisive]}'\]"):
            load_upstream_facts()


def test_the_loader_accepts_several_records_under_the_name_when_every_one_has_a_lane():
    # The control for the failing cases above.
    with isolated_registry():
        for label in ("oracle-a", "oracle-b", "oracle-c"):
            register_data_source(_oracle_variant(label, lane=True))
        register_data_source(CLICKHOUSE_SPEC)
        assert len(load_upstream_facts().specs) == 4


KEYED_SOURCES = ("Alpha Source", "Oracle", "Zulu Source")  # sorted, so first, middle and last


def _register_keyed_sources(*, unpublished: str = "", laneless: str = "") -> None:
    for name in KEYED_SOURCES:
        if name == unpublished:
            continue
        if name == "Oracle":
            record = _oracle_variant("oracle", lane=name != laneless)
        else:
            record = _record(
                name.lower().replace(" ", "-"),
                name,
                lane=None if name == laneless else LANE,
                tiers=frozenset() if name == laneless else frozenset({SupportTier.FLUENT_API}),
            )
        register_data_source(record)


@pytest.fixture
def three_version_keys(monkeypatch):
    monkeypatch.setattr(
        upstream_declarations,
        "TESTED_VERSION_NOTES",
        {name: f"Tested against {name} 1." for name in KEYED_SOURCES},
    )


def test_the_loader_accepts_a_version_map_whose_every_key_is_published_with_a_lane(
    three_version_keys,
):
    # The control for the failing cases below.
    with isolated_registry():
        _register_keyed_sources()
        register_data_source(CLICKHOUSE_SPEC)
        assert len(load_upstream_facts().specs) == 4


@pytest.mark.parametrize("name", KEYED_SOURCES, ids=["first-key", "middle-key", "last-key"])
def test_the_loader_checks_every_key_of_the_version_map_for_a_published_row(
    three_version_keys, name
):
    with isolated_registry():
        _register_keyed_sources(unpublished=name)
        with pytest.raises(
            UpstreamDeclarationError, match="resolves to no published row"
        ) as raised:
            load_upstream_facts()
    assert f"names {name!r}" in str(raised.value)


@pytest.mark.parametrize("name", KEYED_SOURCES, ids=["first-key", "middle-key", "last-key"])
def test_the_loader_checks_every_key_of_the_version_map_for_a_lane(three_version_keys, name):
    with isolated_registry():
        _register_keyed_sources(laneless=name)
        with pytest.raises(
            UpstreamDeclarationError, match="declare no continuous-integration lane"
        ) as raised:
            load_upstream_facts()
    assert f"names {name!r}" in str(raised.value)


# --- order, composition, and the live rows ---------------------------------------------------


@WITH_CASE_DESCRIPTIONS
def test_every_note_kind_is_appended_after_the_disagreement_notes_in_a_fixed_order(monkeypatch):
    monkeypatch.setitem(
        upstream_declarations.TESTED_VERSION_NOTES, "Throwaway Source", "Version bound."
    )
    facts = _facts(
        _record(
            "a",
            "Throwaway Source",
            tiers=ALL_THREE,
            exclusions={SupportTier.CANONICAL_EXPECTATIONS: {"some_case": "Not run."}},
        ),
        _record(
            "b",
            "Throwaway Source",
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS}),
            fluent_types=frozenset({"pandas_filesystem"}),
            provisioning=CREDENTIALS,
            provisioning_note="Needs a tenant.",
        ),
        covered=frozenset({"b"}),
    )
    assert _notes_of(facts, "Throwaway Source") == (
        f"Every shipped expectation: not met for {CSV_DESCRIPTION}.",
        f"Datasource API contract: not met for {CSV_DESCRIPTION}.",
        COVERED_NOTE,
        f"Expectation suite, {PANDAS_DESCRIPTION}: not run for the shared check. "
        f"Recorded reason: Not run.",
        MANAGED_NOTE,
        "Version bound.",
    )


def test_the_notes_every_real_row_carries_are_exactly_these():
    # Printed from the live declarations and pinned by hand. A row not listed carries none.
    live = {name: notes for name, notes in _live_notes().items() if notes}
    assert live == {
        "AlloyDB": (COVERED_NOTE,),
        "Amazon Aurora PostgreSQL": (COVERED_NOTE,),
        "Azure Blob Storage": (COVERED_NOTE,),
        "Citus": (COVERED_NOTE,),
        "ClickHouse": (
            "Expectation suite: not run for column names that need quoting. Recorded reason: This "
            "dialect's SQLAlchemy/driver insert path keys each row by the sanitized "
            "bind-parameter name instead of the real column name for identifiers requiring "
            "quoting, raising a `KeyError` at insert time and leaving the table empty. An issue "
            "still needs to be filed for this defect.",
        ),
        "Microsoft Fabric": (COVERED_NOTE, MANAGED_NOTE),
        "Neon": (COVERED_NOTE,),
        "Oracle": (ORACLE_NOTE,),
        "Pandas": (
            f"Datasource API contract, {PANDAS_DESCRIPTION}: not run for keeping a single saved "
            "entry after create-or-update, replacing an existing datasource on create-or-update "
            "and replacing a datasource's configuration on update. Recorded reason: "
            "PandasDatasource "
            "declares no field beyond name, type, identifier and assets, so no update of its "
            "configuration can be observed to have replaced anything.",
        ),
    }


# ---------------------------------------------------------------------------------------------
# Rendering the table.
#
# Expected text is written out by hand. Rendering it with the code under test could not fail.
# ---------------------------------------------------------------------------------------------

NOTICE = "{/* Generated file. Do not edit by hand. Regenerate with: invoke docs-tables --sync */}"
HEADER = "| Data source | Support tier | Criteria met | Notes |"
SEPARATOR = "| --- | --- | --- | --- |"
FOOTNOTE = (
    "Connection paths GX ships that no row above covers: Databricks File System paths read "
    "with Spark; Databricks File System paths read with pandas; Power BI semantic models; "
    "Spark in-memory DataFrames."
)
ALL_CRITERIA_CELL = "Every shipped expectation<br/>Expectation suite<br/>Datasource API contract"
LIVE_ROWS = (
    ("AlloyDB", "Best effort"),
    ("Amazon Aurora PostgreSQL", "Best effort"),
    ("Amazon S3", "Best effort"),
    ("Azure Blob Storage", "Best effort"),
    ("BigQuery", "Fully supported"),
    ("Citus", "Best effort"),
    ("ClickHouse", "Tested"),
    ("Databricks (SQL)", "Fully supported"),
    ("Google Cloud Storage", "Best effort"),
    ("Microsoft Fabric", "Best effort"),
    ("MySQL", "Fully supported"),
    ("Neon", "Best effort"),
    ("Oracle", "Tested"),
    ("Pandas", "Fully supported"),
    ("PostgreSQL", "Fully supported"),
    ("Redshift", "Fully supported"),
    ("SingleStore", "Tested"),
    ("Snowflake", "Tested"),
    ("Spark", "Tested"),
    ("SQL Server", "Tested"),
    ("SQLite", "Fully supported"),
    ("Trino", "Fully supported"),
)
BIGQUERY_LINE = f"| BigQuery | Fully supported | {ALL_CRITERIA_CELL} |  |"
ORACLE_LINE = (
    "| Oracle | Tested | Expectation suite<br/>Datasource API contract | "
    "Tested against Oracle 21c. 19c expected, not verified in CI. |"
)
FABRIC_LINE = (
    f"| Microsoft Fabric | Best effort | No criteria met | {COVERED_NOTE}<br/>{MANAGED_NOTE} |"
)
CLICKHOUSE_LINE = (
    "| ClickHouse | Tested | Expectation suite (with exceptions)<br/>Datasource API contract | "
    "Expectation suite: not run for column names that need quoting. Recorded reason: This "
    "dialect's SQLAlchemy/driver insert path keys each row by the sanitized bind-parameter "
    "name instead of the real column name for identifiers requiring quoting, raising a "
    "\\`KeyError\\` at insert time and leaving the table empty. An issue still needs to be "
    "filed for this defect. |"
)
PANDAS_LINE = (
    "| Pandas | Fully supported | Every shipped expectation<br/>Expectation suite<br/>"
    "Datasource API contract (with exceptions) | Datasource API contract, "
    f"{PANDAS_DESCRIPTION}: not run for keeping a single saved entry after create-or-update, "
    "replacing an existing datasource on create-or-update and replacing a datasource's "
    "configuration on update. Recorded reason: PandasDatasource declares no field beyond "
    "name, type, identifier and assets, so no update of its configuration can be observed "
    "to have replaced anything. |"
)


def _table_lines(rendered: str) -> Tuple[str, ...]:
    return tuple(line for line in rendered.split("\n") if line.startswith("|"))


def _cells(line: str) -> Tuple[str, ...]:
    assert line.startswith("| ") and line.endswith(" |"), line
    return tuple(line[2:-2].split(" | "))


def _simple_facts(*names: str, **kwargs) -> UpstreamFacts:
    return _facts(
        *(_record(f"label-{i}", name, tiers=ALL_THREE) for i, name in enumerate(names)), **kwargs
    )


# --- the generated-file notice ---------------------------------------------------------------


def test_the_notice_is_the_documentation_builds_comment_form_naming_the_command():
    assert GENERATED_NOTICE == NOTICE
    assert REGENERATION_COMMAND == "invoke docs-tables --sync"


def test_the_notice_leads_the_output_and_is_followed_by_one_blank_line():
    rendered = render_table(_simple_facts("Only"))
    assert rendered.split("\n")[:3] == [NOTICE, "", HEADER]


def test_the_notice_is_not_an_html_comment_and_the_output_holds_no_html_comment():
    rendered = render_table(_simple_facts("Only"))
    assert "<!--" not in rendered
    assert "-->" not in rendered
    assert rendered.count(NOTICE) == 1


def test_the_notice_quotes_the_one_command_string():
    # Catches a notice that disagrees with the constant (a stale or retyped command). It cannot
    # tell whether the notice is built from the constant or merely happens to equal it.
    assert REGENERATION_COMMAND in GENERATED_NOTICE
    assert GENERATED_NOTICE.count(REGENERATION_COMMAND) == 1


# --- columns -----------------------------------------------------------------------------------


def test_the_header_and_separator_are_exactly_the_four_columns_in_order():
    lines = _table_lines(render_table(_simple_facts("Only")))
    assert lines[0] == HEADER
    assert lines[1] == SEPARATOR


def test_a_row_prints_name_tier_criteria_and_notes_in_that_order():
    rendered = render_table(_simple_facts("Only"))
    assert _table_lines(rendered)[2] == f"| Only | Fully supported | {ALL_CRITERIA_CELL} |  |"
    assert _cells(_table_lines(rendered)[2]) == (
        "Only",
        "Fully supported",
        ALL_CRITERIA_CELL,
        "",
    )


def test_every_tier_label_is_printed_as_declared():
    facts = _facts(
        _record("a", "Top", tiers=ALL_THREE),
        _record("b", "Middle", tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS})),
        _record("c", "Low", tiers=frozenset()),
    )
    rows = [_cells(line) for line in _table_lines(render_table(facts))[2:]]
    assert rows == [
        ("Low", "Best effort", "No criteria met", ""),
        ("Middle", "Tested", "Expectation suite", ""),
        ("Top", "Fully supported", ALL_CRITERIA_CELL, ""),
    ]


@pytest.mark.parametrize(
    ("tiers", "expected"),
    [
        (frozenset({SupportTier.GALLERY}), "Every shipped expectation"),
        (frozenset({SupportTier.CANONICAL_EXPECTATIONS}), "Expectation suite"),
        (frozenset({SupportTier.FLUENT_API}), "Datasource API contract"),
        (
            frozenset({SupportTier.GALLERY, SupportTier.CANONICAL_EXPECTATIONS}),
            "Every shipped expectation<br/>Expectation suite",
        ),
        (
            frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
            "Expectation suite<br/>Datasource API contract",
        ),
        (
            frozenset({SupportTier.GALLERY, SupportTier.FLUENT_API}),
            "Every shipped expectation<br/>Datasource API contract",
        ),
        (ALL_THREE, ALL_CRITERIA_CELL),
        (frozenset(), "No criteria met"),
    ],
    ids=["gallery", "suite", "api", "gallery+suite", "suite+api", "gallery+api", "all", "none"],
)
def test_the_criteria_cell_lists_the_met_criteria_in_one_fixed_order(tiers, expected):
    facts = _facts(_record("a", "Only", tiers=tiers))
    assert _cells(_table_lines(render_table(facts))[2])[2] == expected


@WITH_CASE_DESCRIPTIONS
@pytest.mark.parametrize(
    ("tier", "case", "expected"),
    [
        (
            SupportTier.GALLERY,
            "gallery_case",
            "Every shipped expectation (with exceptions)<br/>Expectation suite"
            "<br/>Datasource API contract",
        ),
        (
            SupportTier.CANONICAL_EXPECTATIONS,
            "suite_case",
            "Every shipped expectation<br/>Expectation suite (with exceptions)"
            "<br/>Datasource API contract",
        ),
        (
            SupportTier.FLUENT_API,
            "api_case",
            "Every shipped expectation<br/>Expectation suite"
            "<br/>Datasource API contract (with exceptions)",
        ),
    ],
    ids=["first", "middle", "last"],
)
def test_a_partial_criterion_is_marked_where_it_stands_and_only_there(tier, case, expected):
    facts = _facts(_record("a", "Only", tiers=ALL_THREE, exclusions={tier: {case: REASON}}))
    assert _cells(_table_lines(render_table(facts))[2])[2] == expected


@WITH_CASE_DESCRIPTIONS
def test_a_row_whose_single_met_criterion_is_partial_carries_the_suffix():
    facts = _facts(
        _record(
            "a",
            "Only",
            tiers=frozenset({SupportTier.GALLERY}),
            exclusions={SupportTier.GALLERY: {"gallery_case": REASON}},
        )
    )
    assert _cells(_table_lines(render_table(facts))[2])[2] == (
        "Every shipped expectation (with exceptions)"
    )


def test_several_notes_share_one_cell_joined_by_a_line_break_in_their_fixed_order(monkeypatch):
    notes = ("first note", "second note", "third note")
    row = PublishedRow(
        public_name="Only",
        specs=(),
        criteria_met=frozenset(),
        criteria_partial=frozenset(),
        tier=PublicTier.BEST_EFFORT,
        notes=notes,
    )
    monkeypatch.setattr(table, "assemble_rows", lambda facts: (row,))
    cells = _cells(_table_lines(render_table(_simple_facts("Only")))[2])
    assert cells[3] == "first note<br/>second note<br/>third note"


def test_a_single_note_has_no_separator_and_no_notes_leaves_the_cell_empty():
    facts = _facts(_record("a", "Only", tiers=ALL_THREE, lane=None), covered=frozenset({"a"}))
    cells = _cells(_table_lines(render_table(facts))[2])
    assert cells[3] == COVERED_NOTE  # one note, nothing appended
    assert "<br/>" not in cells[3]
    assert _cells(_table_lines(render_table(_simple_facts("Only")))[2])[3] == ""


# --- names, order and positions -------------------------------------------------------------


@pytest.mark.parametrize(
    ("declared", "printed"),
    [
        ("dBase (Legacy)", "dBase (Legacy)"),
        ("iPhone-DB", "iPhone-DB"),
        ("d  Base", "d  Base"),
        ("ÉCOLE db", "ÉCOLE db"),
        ("lower", "lower"),
        ("UPPER", "UPPER"),
        ("Foo|Bar & <Baz>", "Foo\\|Bar \\& \\<Baz\\>"),
    ],
    ids=["mixed-case", "hyphen", "inner-spaces", "accents", "lower", "upper", "markup"],
)
def test_a_name_prints_as_declared_with_only_the_escaping_the_site_needs(declared, printed):
    facts = _facts(_record("a", declared, tiers=ALL_THREE))
    assert _cells(_table_lines(render_table(facts))[2])[0] == printed


def test_rows_are_ordered_by_public_name_ignoring_case_whatever_order_records_arrive_in():
    names = ("b", "Zed", "A", "c", "a1")
    for order in (names, tuple(reversed(names)), names[2:] + names[:2]):
        rendered = render_table(_simple_facts(*order))
        assert [_cells(line)[0] for line in _table_lines(rendered)[2:]] == [
            "A",
            "a1",
            "b",
            "c",
            "Zed",
        ]


@pytest.mark.parametrize("position", [0, 1, 2], ids=["first", "middle", "last"])
def test_every_row_has_its_markup_neutralized_wherever_it_sorts(position):
    names = ["alpha", "beta", "gamma"]
    names[position] = "a<b>{c}"
    facts = _simple_facts(*names)
    rendered = _table_lines(render_table(facts))[2:]
    assert [_cells(line)[0] for line in rendered] == sorted(
        [n.replace("a<b>{c}", "a\\<b\\>\\{c\\}") for n in names], key=str.casefold
    )


# --- escaping ---------------------------------------------------------------------------------

MARKUP = {
    "\\": "\\\\",
    "`": "\\`",
    "*": "\\*",
    "_": "\\_",
    "~": "\\~",
    "[": "\\[",
    "]": "\\]",
    "<": "\\<",
    ">": "\\>",
    "{": "\\{",
    "}": "\\}",
    "|": "\\|",
    "&": "\\&",
}
SHAPES = pytest.mark.parametrize(
    "shape",
    ["{c}ab", "a{c}b", "ab{c}", "{c}", "a{c}b{c}c{c}"],
    ids=["first", "middle", "last", "alone", "repeated"],
)


@pytest.mark.parametrize("character", list(MARKUP), ids=[f"U+{ord(c):04X}" for c in MARKUP])
@SHAPES
def test_the_escape_helper_neutralizes_each_markup_character_wherever_it_stands(character, shape):
    escaped = MARKUP[character]
    assert _escape_cell(shape.format(c=character)) == shape.format(c=escaped)


@pytest.mark.parametrize(
    "text",
    [
        "plain words",
        "Digits 0123456789",
        ".,;:!?#$%^()+-=/'\" @",
        "naïve — “quoted” ✓",
        "",
    ],
    ids=["words", "digits", "punctuation", "unicode", "empty"],
)
def test_the_escape_helper_leaves_text_that_is_not_markup_unchanged(text):
    assert _escape_cell(text) == text


def test_the_escape_helper_escapes_a_backslash_before_the_markup_it_precedes():
    # A backslash already in the text must not combine with the one added for the next character.
    assert _escape_cell("\\<") == "\\\\\\<"
    assert _escape_cell("a\\\\b") == "a\\\\\\\\b"


def test_the_escape_helper_covers_every_character_the_table_documents():
    assert set(MARKUP) == set(table._MARKUP_CHARACTERS)


BREAK_SHAPES = pytest.mark.parametrize(
    "shape", ["{c}ab", "a{c}b", "ab{c}"], ids=["first", "middle", "last"]
)


@LINE_BREAKS
@BREAK_SHAPES
def test_the_escape_helper_rejects_a_line_break_wherever_it_stands(character, shape):
    with pytest.raises(ValueError) as raised:
        _escape_cell(shape.format(c=character))
    assert type(raised.value) is ValueError
    assert "contains a line break" in str(raised.value)


@pytest.mark.parametrize("character", ["\t", " ", "\x00"], ids=["tab", "space", "nul"])
def test_the_escape_helper_does_not_reject_what_is_not_a_line_break(character):
    assert _escape_cell(f"a{character}b") == f"a{character}b"


def _crafted_row(
    *,
    criteria_met: FrozenSet[str] = frozenset(),
    notes: Tuple[str, ...] = (),
) -> PublishedRow:
    return PublishedRow(
        public_name="Only",
        specs=(),
        criteria_met=criteria_met,
        criteria_partial=frozenset(),
        tier=PublicTier.BEST_EFFORT,
        notes=notes,
    )


def test_the_tier_label_is_escaped(monkeypatch):
    monkeypatch.setattr(PublicTier.BEST_EFFORT, "_value_", "Best <effort>")
    facts = _simple_facts("Only")
    monkeypatch.setattr(table, "assemble_rows", lambda facts: (_crafted_row(),))
    assert _cells(_table_lines(render_table(facts))[2])[1] == "Best \\<effort\\>"


@pytest.mark.parametrize("position", [0, 1, 2], ids=["first", "middle", "last"])
def test_a_criterion_label_is_escaped_wherever_it_stands(monkeypatch, position):
    labels = [criterion.label for criterion in CRITERIA]
    labels[position] = "A {label}"
    patched = tuple(replace(c, label=label) for c, label in zip(CRITERIA, labels, strict=True))
    monkeypatch.setattr(table, "CRITERIA", patched)
    row = _crafted_row(criteria_met=frozenset(c.key for c in patched))
    monkeypatch.setattr(table, "assemble_rows", lambda facts: (row,))
    expected = [c.label for c in CRITERIA]
    expected[position] = "A \\{label\\}"
    cells = _cells(_table_lines(render_table(_simple_facts("Only")))[2])
    assert cells[2] == "<br/>".join(expected)


@pytest.mark.parametrize("position", [0, 1, 2], ids=["first", "middle", "last"])
def test_a_note_is_escaped_wherever_it_stands(monkeypatch, position):
    notes = ["note one", "note two", "note three"]
    notes[position] = "a <b> {c} | `d`"
    monkeypatch.setattr(table, "assemble_rows", lambda facts: (_crafted_row(notes=tuple(notes)),))
    cells = _cells(_table_lines(render_table(_simple_facts("Only")))[2])
    expected = list(notes)
    expected[position] = "a \\<b\\> \\{c\\} \\| \\`d\\`"
    assert cells[3] == "<br/>".join(expected)


@WITH_CASE_DESCRIPTIONS
def test_a_recorded_reason_with_markup_reaches_the_table_escaped():
    reason = "Uses <T> and {x}, a|b, `c`, *d*, _e_, [f](g), &amp;, ~h~ and \\i"
    facts = _facts(
        _record(
            "a", "Only", tiers=ALL_THREE, exclusions={SupportTier.GALLERY: {"gallery_case": reason}}
        )
    )
    (line,) = _table_lines(render_table(facts))[2:]
    assert _cells(line)[3] == (
        "Every shipped expectation: not run for the gallery check. "
        "Recorded reason: Uses \\<T\\> and \\{x\\}, a\\|b, \\`c\\`, \\*d\\*, \\_e\\_, "
        "\\[f\\](g), \\&amp;, \\~h\\~ and \\\\i"
    )


@WITH_CASE_DESCRIPTIONS
@LINE_BREAKS
def test_a_line_break_in_a_recorded_reason_fails_rendering(character):
    facts = _facts(
        _record(
            "a",
            "Only",
            tiers=ALL_THREE,
            exclusions={SupportTier.GALLERY: {"gallery_case": f"one{character}two"}},
        )
    )
    with pytest.raises(ValueError):
        render_table(facts)


@LINE_BREAKS
def test_a_line_break_in_a_public_name_fails_rendering(character):
    with pytest.raises(ValueError) as raised:
        render_table(_simple_facts(f"One{character}Two"))
    assert type(raised.value) is ValueError
    assert "contains a line break" in str(raised.value)


def test_an_empty_registry_fails_rendering_rather_than_printing_an_empty_table():
    with pytest.raises(ValueError, match="no record"):
        render_table(_facts())


# --- the footnote -----------------------------------------------------------------------------


def test_the_footnote_follows_the_table_after_one_blank_line_and_ends_the_output():
    facts = _simple_facts("Only", uncovered_paths=frozenset({"spark"}))
    assert render_table(facts) == (
        f"{NOTICE}\n\n{HEADER}\n{SEPARATOR}\n"
        f"| Only | Fully supported | {ALL_CRITERIA_CELL} |  |\n\n"
        "Connection paths GX ships that no row above covers: Spark in-memory DataFrames.\n"
    )


@pytest.mark.parametrize("rows", [1, 3, 8], ids=["one-row", "three-rows", "eight-rows"])
def test_the_footnote_is_printed_once_however_many_rows_there_are(rows):
    names = [f"Source {chr(ord('A') + i)}" for i in range(rows)]
    rendered = render_table(_simple_facts(*names, uncovered_paths=frozenset({"spark"})))
    assert rendered.count("Connection paths GX ships") == 1
    assert rendered.count("Spark in-memory DataFrames") == 1
    assert rendered.rstrip("\n").split("\n")[-1].startswith("Connection paths GX ships")


def test_no_footnote_is_printed_when_every_path_is_covered():
    rendered = render_table(_simple_facts("Only"))
    assert "Connection paths" not in rendered
    assert rendered == (
        f"{NOTICE}\n\n{HEADER}\n{SEPARATOR}\n| Only | Fully supported | {ALL_CRITERIA_CELL} |  |\n"
    )


def test_the_footnote_is_escaped(monkeypatch):
    monkeypatch.setitem(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, "spark", "a <b> {c}")
    rendered = render_table(_simple_facts("Only", uncovered_paths=frozenset({"spark"})))
    assert rendered.endswith(
        "Connection paths GX ships that no row above covers: a \\<b\\> \\{c\\}.\n"
    )


def test_an_undescribed_uncovered_path_fails_rendering_and_names_the_path():
    with pytest.raises(UpstreamDeclarationError, match="no_such_path"):
        render_table(_simple_facts("Only", uncovered_paths=frozenset({"no_such_path"})))


# --- one trailing newline ---------------------------------------------------------------------


@pytest.mark.parametrize("with_footnote", [False, True], ids=["no-footnote", "footnote"])
def test_the_output_ends_in_exactly_one_newline(with_footnote):
    paths = frozenset({"spark"}) if with_footnote else frozenset()
    rendered = render_table(_simple_facts("Only", uncovered_paths=paths))
    assert rendered.endswith("\n")
    assert not rendered.endswith("\n\n")
    assert not rendered.endswith(" \n")


def test_the_output_has_no_carriage_return_and_no_blank_line_inside_the_table():
    rendered = render_table(_simple_facts("A", "B", "C", uncovered_paths=frozenset({"spark"})))
    assert "\r" not in rendered
    lines = rendered.split("\n")
    first, last = (
        lines.index(HEADER),
        max(i for i, line in enumerate(lines) if line.startswith("|")),
    )
    assert all(lines[i].startswith("|") for i in range(first, last + 1))


# --- determinism ------------------------------------------------------------------------------


def test_rendering_twice_gives_identical_text_with_throwaway_records():
    facts = _simple_facts("b", "a", uncovered_paths=frozenset({"spark"}))
    first = render_table(facts)
    assert first != ""
    assert render_table(facts) == first


def test_rendering_the_real_declarations_twice_gives_identical_text():
    first = render_table(load_upstream_facts())
    assert len(_table_lines(first)) == 2 + len(LIVE_ROWS)
    assert render_table(load_upstream_facts()) == first


def _permutations(specs: Tuple[DataSourceSpec, ...]):
    rotated = specs[7:] + specs[:7]
    by_label_descending = tuple(sorted(specs, key=lambda spec: spec.label, reverse=True))
    interleaved = specs[::2] + specs[1::2]
    return {
        "reversed": tuple(reversed(specs)),
        "rotated": rotated,
        "label-descending": by_label_descending,
        "interleaved": interleaved,
    }


@pytest.mark.parametrize("order", ["reversed", "rotated", "label-descending", "interleaved"])
def test_the_order_the_registry_returns_records_in_does_not_change_the_output(order):
    facts = load_upstream_facts()
    baseline = render_table(facts)
    assert len(_table_lines(baseline)) == 2 + len(LIVE_ROWS)
    permuted = _permutations(facts.specs)[order]
    assert permuted != facts.specs
    assert render_table(replace(facts, specs=permuted)) == baseline


def test_the_order_records_of_one_row_are_listed_in_does_not_change_the_output():
    variants = _three_variants(1)
    baseline = render_table(_facts(*variants))
    for order in ((2, 1, 0), (1, 2, 0), (2, 0, 1)):
        assert render_table(_facts(*(variants[i] for i in order))) == baseline


# --- the real declarations ------------------------------------------------------------------


def test_the_real_registry_renders_the_notice_one_row_per_data_source_and_the_footnote():
    rendered = render_table(load_upstream_facts())
    lines = rendered.split("\n")
    assert lines[0] == NOTICE
    assert lines[1] == ""
    table_lines = _table_lines(rendered)
    assert table_lines[0] == HEADER
    assert table_lines[1] == SEPARATOR
    assert [(_cells(line)[0], _cells(line)[1]) for line in table_lines[2:]] == list(LIVE_ROWS)
    assert len(LIVE_ROWS) == 22
    assert lines[-3:] == ["", FOOTNOTE, ""]


@pytest.mark.parametrize(
    "line",
    [BIGQUERY_LINE, ORACLE_LINE, FABRIC_LINE, CLICKHOUSE_LINE, PANDAS_LINE],
    ids=["bigquery", "oracle", "fabric", "clickhouse", "pandas"],
)
def test_representative_real_rows_render_exactly_this_text(line):
    assert line in _table_lines(render_table(load_upstream_facts()))


def test_the_real_rendering_has_no_unescaped_markup_in_any_cell():
    for line in _table_lines(render_table(load_upstream_facts()))[2:]:
        for cell in _cells(line):
            stripped = cell.replace("<br/>", "")
            for character in "<>{}":
                assert re.search(rf"(?<!\\){re.escape(character)}", stripped) is None, line
            assert re.search(r"(?<!\\)`", stripped) is None, line


# --- the drift check ---------------------------------------------------------------------------
#
# The checked-in partial is generated, so a hand edit, or a declaration changing without a
# regeneration, leaves the published page stale. These tests regenerate the table in-process and
# compare it with the file. The comparison is content equality whose failure message is computed:
# a bare equality assertion would have pytest print a character-level diff of the whole artifact.
# Every test that makes the check fail does so on an altered copy of its input, never on the
# checked-in file, so the file itself is never touched. Every name in this section starts with
# `drift` so the one continuous-integration step that repeats the check can select it by name.

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKED_IN_PARTIAL = REPO_ROOT / GENERATED_PARTIAL_PATH
END_OF_FILE = "<end of file>"
EMPTY_REGISTRY_MESSAGE = "The registry yields no record"
EMPTY_ARTIFACT_MESSAGE = "is empty"
FEWER_ROWS_MESSAGE = "fewer rows"


def _first_difference(rendered: str, checked_in: str) -> Optional[Tuple[int, str, str]]:
    """Line number (from 1) and both lines at the first divergence, or None.

    A side that ends first is shown as `<end of file>`, so a missing final newline or a missing
    trailing line is a difference rather than being padded away.
    """
    for number, (generated, existing) in enumerate(
        zip_longest(rendered.split("\n"), checked_in.split("\n"), fillvalue=END_OF_FILE), start=1
    ):
        if generated != existing:
            return number, generated, existing
    return None


def _row_count(text: str) -> int:
    return max(len(_table_lines(text)) - 2, 0)


def _drift_failure(
    facts: UpstreamFacts, checked_in: str, render: Callable[[UpstreamFacts], str] = render_table
) -> Optional[str]:
    """The message the check fails with, or None when the file is what the registry derives.

    The vacuity guards come first, because an empty registry would have nothing to render and
    an empty file would differ from anything: each failure says what is empty rather than
    reporting a difference.
    """
    if not facts.specs:
        return f"{EMPTY_REGISTRY_MESSAGE}, so there is nothing to compare the file against."
    if not checked_in.strip():
        return (
            f"{GENERATED_PARTIAL_PATH} {EMPTY_ARTIFACT_MESSAGE}. "
            f"Regenerate it: {REGENERATION_COMMAND}"
        )
    rendered = render(facts)
    distinct = len({spec.public_name for spec in facts.specs})
    if _row_count(rendered) < distinct:
        return (
            f"The rendering has {_row_count(rendered)} rows, {FEWER_ROWS_MESSAGE} than the "
            f"{distinct} distinct public names the registry declares."
        )
    difference = _first_difference(rendered, checked_in)
    if difference is None:
        return None
    number, generated, existing = difference
    return (
        f"{GENERATED_PARTIAL_PATH} does not match what the registry's declarations derive.\n"
        f"Regenerate it with: {REGENERATION_COMMAND}\n"
        f"First difference at line {number}:\n"
        f"  generated:  {generated}\n"
        f"  checked in: {existing}"
    )


def _read_checked_in() -> str:
    # Byte-exact: text mode would translate line endings and hide a CRLF-converted file, and a
    # normalising read would hide a missing or extra final newline.
    return CHECKED_IN_PARTIAL.read_bytes().decode("utf-8")


def _baseline() -> str:
    """What the registry derives, rendered in-process.

    Every test but the core check starts from this rather than from the checked-in file, so a
    file that really drifted fails exactly the core check and not every proof built on it.
    """
    return render_table(load_upstream_facts())


def _drift_failure_for_the_live_registry() -> Optional[str]:
    return _drift_failure(load_upstream_facts(), _read_checked_in())


def _fail_on(message: Optional[str]) -> None:
    if message is not None:
        pytest.fail(message, pytrace=False)


def test_drift_the_checked_in_partial_is_what_the_registry_derives():
    _fail_on(_drift_failure_for_the_live_registry())


def test_drift_the_partial_is_read_from_the_one_path_the_command_writes():
    assert str(GENERATED_PARTIAL_PATH) == "docs/docusaurus/docs/help/_data_source_support_table.md"
    assert CHECKED_IN_PARTIAL == REPO_ROOT / "docs" / "docusaurus" / "docs" / "help" / (
        "_data_source_support_table.md"
    )
    assert CHECKED_IN_PARTIAL.is_file()
    assert (REPO_ROOT / "tasks.py").is_file()


def test_drift_the_regeneration_command_runs_a_task_that_writes_that_path(capsys):
    import invoke
    import tasks

    assert REGENERATION_COMMAND == "invoke docs-tables --sync"
    assert tasks.docs_tables.name == "docs_tables"
    assert "docs-table" in tasks.docs_tables.aliases
    tasks.docs_tables(invoke.Context())
    announced = capsys.readouterr().out.split("\n", 1)[0]
    assert announced == f"Would write {GENERATED_PARTIAL_PATH}:"


def test_drift_the_task_runner_loads_no_test_module():
    import subprocess

    code = (
        "import sys, tasks; "
        "print([m for m in sys.modules if m == 'tests' or m.startswith('tests.')])"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "[]"


# --- the difference helper ---


@pytest.mark.parametrize(
    ("rendered", "checked_in", "expected"),
    [
        ("a\nb\nc\n", "a\nb\nc\n", None),
        ("", "", None),
        ("X\nb\nc\n", "a\nb\nc\n", (1, "X", "a")),
        ("a\nX\nc\n", "a\nb\nc\n", (2, "X", "b")),
        ("a\nb\nX\n", "a\nb\nc\n", (3, "X", "c")),
        ("a\nb\nc\n", "a\nb\nc", (4, "", END_OF_FILE)),
        ("a\nb\nc", "a\nb\nc\n", (4, END_OF_FILE, "")),
        ("a\nb\n", "a\nb\nc\n", (3, "", "c")),
        ("a\nb\nc\n", "a\nb\n", (3, "c", "")),
        ("a\nb \n", "a\nb\n", (2, "b ", "b")),
    ],
    ids=[
        "equal",
        "both-empty",
        "first-line",
        "middle-line",
        "last-line",
        "checked-in-lacks-final-newline",
        "rendering-lacks-final-newline",
        "checked-in-has-an-extra-line",
        "rendering-has-an-extra-line",
        "trailing-space",
    ],
)
def test_drift_the_difference_helper_locates_the_first_divergence(rendered, checked_in, expected):
    assert _first_difference(rendered, checked_in) == expected


def test_drift_the_live_read_is_byte_exact(tmp_path, monkeypatch):
    for text in ("a\nb\n", "a\nb", "a\nb\n\n\n", "a\r\nb\r\n"):
        copy = tmp_path / "partial.md"
        copy.write_bytes(text.encode("utf-8"))
        monkeypatch.setattr(sys.modules[__name__], "CHECKED_IN_PARTIAL", copy)
        assert _read_checked_in() == text


def test_drift_a_converted_line_ending_in_the_file_is_a_difference():
    baseline = _baseline()
    converted = baseline.replace("\n", "\r\n")
    assert _first_difference(baseline, converted) is not None
    assert REGENERATION_COMMAND in (_drift_failure(load_upstream_facts(), converted) or "")


def test_drift_the_difference_helper_reports_the_first_of_several_divergences():
    assert _first_difference("a\nX\nc\nY\n", "a\nb\nc\nd\n") == (2, "X", "b")


def _artifact_lines() -> list:
    return _baseline().split("\n")


def _altered_copy(index: int) -> str:
    lines = _artifact_lines()
    lines[index] = lines[index] + " ALTERED"
    return "\n".join(lines)


def _positions() -> Tuple[int, int, int]:
    last_real_line = len(_artifact_lines()) - 2  # the file ends with a newline
    return 0, last_real_line // 2, last_real_line


@pytest.mark.parametrize("which", [0, 1, 2], ids=["first", "middle", "last"])
def test_drift_an_altered_copy_of_the_artifact_is_located_at_the_altered_line(which):
    index = _positions()[which]
    original = _artifact_lines()[index]
    assert original  # the notice, a table row and the footnote: never a blank line
    found = _first_difference(_baseline(), _altered_copy(index))
    assert found == (index + 1, original, original + " ALTERED")


@pytest.mark.parametrize("which", [0, 1, 2], ids=["first", "middle", "last"])
def test_drift_the_failure_names_the_file_the_command_and_the_first_differing_line(which):
    index = _positions()[which]
    original = _artifact_lines()[index]
    message = _drift_failure(load_upstream_facts(), _altered_copy(index))
    assert message == (
        "docs/docusaurus/docs/help/_data_source_support_table.md does not match what the "
        "registry's declarations derive.\n"
        "Regenerate it with: invoke docs-tables --sync\n"
        f"First difference at line {index + 1}:\n"
        f"  generated:  {original}\n"
        f"  checked in: {original} ALTERED"
    )


def test_drift_the_failure_does_not_print_a_diff_of_the_artifact():
    middle = _positions()[1]
    message = _drift_failure(load_upstream_facts(), _altered_copy(middle))
    assert message is not None
    others = [line for i, line in enumerate(_artifact_lines()) if i != middle and len(line) > 30]
    assert len(others) > 10
    assert [line for line in others if line in message] == []
    with pytest.raises(pytest.fail.Exception) as raised:
        _fail_on(message)
    assert raised.value.msg == message


def test_drift_a_trailing_newline_is_part_of_the_comparison():
    stripped = _baseline().rstrip("\n")
    message = _drift_failure(load_upstream_facts(), stripped)
    assert message is not None
    assert f"First difference at line {len(stripped.split(chr(10))) + 1}:" in message


# --- the check reads the live registry, not a snapshot ---


def test_drift_a_throwaway_record_registered_in_the_seam_changes_the_rendering():
    throwaway = _record("throwaway-drift-label", "Zzz Throwaway")
    expected_row = "| Zzz Throwaway | Best effort | No criteria met |  |"
    with isolated_registry():
        register_real_records_the_loader_checks_against()
        before_facts = load_upstream_facts()
        before = render_table(before_facts)
        register_data_source(throwaway)
        after_facts = load_upstream_facts()
        after = render_table(after_facts)
        failure = _drift_failure(after_facts, before)
    assert expected_row not in before.split("\n")
    assert after != before
    assert set(after.split("\n")) - set(before.split("\n")) == {expected_row}
    assert set(before.split("\n")) - set(after.split("\n")) == set()
    assert failure is not None and REGENERATION_COMMAND in failure


# --- a declaration changing without a regeneration ---

_LIVE_SPECS = iter_data_source_specs()
_WITH_TIERS = [i for i, spec in enumerate(_LIVE_SPECS) if spec.tiers]
_WITHOUT_TIERS = [i for i, spec in enumerate(_LIVE_SPECS) if not spec.tiers]
_THREE = lambda indexes: [indexes[0], indexes[len(indexes) // 2], indexes[-1]]  # noqa: E731


def _facts_with(specs: Tuple[DataSourceSpec, ...]) -> UpstreamFacts:
    return replace(load_upstream_facts(), specs=specs)


@pytest.mark.parametrize(
    "index", _THREE(list(range(len(_LIVE_SPECS)))), ids=["first", "middle", "last"]
)
def test_drift_a_record_removed_without_regenerating_fails(index):
    specs = _LIVE_SPECS[:index] + _LIVE_SPECS[index + 1 :]
    message = _drift_failure(_facts_with(specs), _baseline())
    assert message is not None and REGENERATION_COMMAND in message


@pytest.mark.parametrize("index", _THREE(_WITH_TIERS), ids=["first", "middle", "last"])
def test_drift_a_record_losing_its_tiers_without_regenerating_fails(index):
    specs = list(_LIVE_SPECS)
    specs[index] = replace(specs[index], tiers=frozenset())
    message = _drift_failure(_facts_with(tuple(specs)), _baseline())
    assert message is not None and REGENERATION_COMMAND in message


@pytest.mark.parametrize("index", _THREE(_WITHOUT_TIERS), ids=["first", "middle", "last"])
def test_drift_a_record_gaining_tiers_without_regenerating_fails(index):
    specs = list(_LIVE_SPECS)
    specs[index] = replace(specs[index], tiers=ALL_THREE)
    message = _drift_failure(_facts_with(tuple(specs)), _baseline())
    assert message is not None and REGENERATION_COMMAND in message


def test_drift_a_record_added_without_regenerating_fails():
    added = _record("throwaway-drift-label", "Zzz Throwaway")
    message = _drift_failure(_facts_with(_LIVE_SPECS + (added,)), _baseline())
    assert message is not None and "Zzz Throwaway" in message


# --- the two pinned literals ---

_UNCOVERED = sorted(FLUENT_TYPES_NAMED_BY_NO_RECORD)
_COVERED = sorted(RECORDS_COVERED_BUT_UNABLE_TO_CLAIM)
_CONTROL_LABEL = next(spec.label for spec in _LIVE_SPECS if spec.label not in _COVERED)
_CONTROL_PATH = next(
    t for t in sorted(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS) if t not in _UNCOVERED
)


def _assert_the_literal_change_is_caught(monkeypatch, name: str, changed: FrozenSet[str]) -> None:
    baseline = _baseline()
    # The control: before the change nothing differs from the baseline.
    assert _drift_failure(load_upstream_facts(), baseline) is None
    monkeypatch.setattr(upstream_declarations, name, changed)
    facts = load_upstream_facts()
    assert _first_difference(render_table(facts), baseline) is not None
    message = _drift_failure(facts, baseline)
    assert message is not None and REGENERATION_COMMAND in message


@pytest.mark.parametrize("removed", _UNCOVERED)
def test_drift_each_uncovered_path_dropped_from_its_literal_without_regenerating_fails(
    monkeypatch, removed
):
    changed = frozenset(FLUENT_TYPES_NAMED_BY_NO_RECORD - {removed})
    assert changed != FLUENT_TYPES_NAMED_BY_NO_RECORD
    _assert_the_literal_change_is_caught(monkeypatch, "FLUENT_TYPES_NAMED_BY_NO_RECORD", changed)


def test_drift_an_uncovered_path_added_to_its_literal_without_regenerating_fails(monkeypatch):
    changed = frozenset(FLUENT_TYPES_NAMED_BY_NO_RECORD | {_CONTROL_PATH})
    _assert_the_literal_change_is_caught(monkeypatch, "FLUENT_TYPES_NAMED_BY_NO_RECORD", changed)


@pytest.mark.parametrize("removed", _COVERED)
def test_drift_each_covered_label_dropped_from_its_literal_without_regenerating_fails(
    monkeypatch, removed
):
    changed = frozenset(RECORDS_COVERED_BUT_UNABLE_TO_CLAIM - {removed})
    assert changed != RECORDS_COVERED_BUT_UNABLE_TO_CLAIM
    _assert_the_literal_change_is_caught(
        monkeypatch, "RECORDS_COVERED_BUT_UNABLE_TO_CLAIM", changed
    )


def test_drift_a_covered_label_added_to_its_literal_without_regenerating_fails(monkeypatch):
    changed = frozenset(RECORDS_COVERED_BUT_UNABLE_TO_CLAIM | {_CONTROL_LABEL})
    _assert_the_literal_change_is_caught(
        monkeypatch, "RECORDS_COVERED_BUT_UNABLE_TO_CLAIM", changed
    )


# --- the check cannot pass by comparing nothing ---


def test_drift_an_empty_registry_fails_rather_than_passing():
    message = _drift_failure(_facts(), _baseline())
    assert message is not None and EMPTY_REGISTRY_MESSAGE in message


def test_drift_an_empty_registry_stops_the_live_check_before_any_comparison():
    with isolated_registry():
        assert iter_data_source_specs() == ()
        with pytest.raises(UpstreamDeclarationError):
            _drift_failure_for_the_live_registry()


def test_drift_an_empty_artifact_fails_rather_than_passing(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "_read_checked_in", lambda: "")
    message = _drift_failure_for_the_live_registry()
    assert message is not None
    assert message.startswith(f"{GENERATED_PARTIAL_PATH} {EMPTY_ARTIFACT_MESSAGE}")
    assert REGENERATION_COMMAND in message


@pytest.mark.parametrize("blank", ["\n", "  \n\n"], ids=["newline", "whitespace"])
def test_drift_an_artifact_of_only_whitespace_is_empty_too(blank):
    message = _drift_failure(load_upstream_facts(), blank)
    assert message is not None and EMPTY_ARTIFACT_MESSAGE in message


@pytest.mark.parametrize("which", [0, 1, 2], ids=["first", "middle", "last"])
def test_drift_a_rendering_with_fewer_rows_than_public_names_fails(which):
    facts = load_upstream_facts()
    names = len({spec.public_name for spec in facts.specs})
    lines = render_table(facts).split("\n")
    row_positions = [i for i, line in enumerate(lines) if line.startswith("|")][2:]
    assert len(row_positions) == names
    dropped = row_positions[(0, len(row_positions) // 2, len(row_positions) - 1)[which]]

    def render_without_a_row(_facts: UpstreamFacts) -> str:
        return "\n".join(line for i, line in enumerate(lines) if i != dropped)

    # The checked-in file is compared only after the guard, so give it the short rendering too:
    # the guard, not the comparison, must be what stops the check.
    short = render_without_a_row(facts)
    message = _drift_failure(facts, short, render=render_without_a_row)
    assert message is not None
    assert (
        f"{names - 1} rows, {FEWER_ROWS_MESSAGE} than the {names} distinct public names" in message
    )


def test_drift_a_rendering_with_exactly_one_row_per_public_name_is_not_stopped_by_the_guard():
    facts = load_upstream_facts()
    assert _drift_failure(facts, render_table(facts)) is None


# --- the description map and the declarations agree in both directions ---


def _description_map_failure(
    specs: Tuple[DataSourceSpec, ...],
    uncovered_paths: AbstractSet[str],
    descriptions: Mapping[str, str],
) -> Optional[str]:
    declared = {fluent_type for spec in specs for fluent_type in spec.fluent_types}
    undescribed = sorted(declared - descriptions.keys())
    if undescribed:
        return (
            f"A record declares fluent type(s) {undescribed} the description map does not describe."
        )
    unnamed = sorted(set(uncovered_paths) - descriptions.keys())
    if unnamed:
        return f"The pinned literal names path(s) {unnamed} the description map does not describe."
    stale = sorted(descriptions.keys() - declared - set(uncovered_paths))
    if stale:
        return (
            f"The description map describes {stale}, which no record declares and the pinned "
            f"literal does not name."
        )
    return None


def _live_description_map_failure(descriptions: Mapping[str, str]) -> Optional[str]:
    facts = load_upstream_facts()
    return _description_map_failure(
        facts.specs, facts.fluent_types_named_by_no_record, descriptions
    )


def test_drift_the_description_map_describes_exactly_the_types_the_declarations_name():
    _fail_on(_live_description_map_failure(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS))


def test_drift_a_described_type_that_nothing_declares_or_names_fails():
    extra = {**upstream_declarations.CONNECTION_PATH_DESCRIPTIONS, "not_a_real_type": "x"}
    message = _live_description_map_failure(extra)
    assert message is not None and "['not_a_real_type']" in message
    assert "no record declares" in message


def test_drift_a_type_only_the_pinned_literal_names_is_not_a_stale_description():
    for path in sorted(FLUENT_TYPES_NAMED_BY_NO_RECORD):
        assert path in upstream_declarations.CONNECTION_PATH_DESCRIPTIONS
    assert _live_description_map_failure(upstream_declarations.CONNECTION_PATH_DESCRIPTIONS) is None


_DECLARED_BY_RECORDS = sorted({t for spec in _LIVE_SPECS for t in spec.fluent_types})


@pytest.mark.parametrize("which", [0, 1, 2], ids=["first", "middle", "last"])
def test_drift_a_declared_type_the_map_does_not_describe_fails_in_the_check_and_the_loader(
    monkeypatch, which
):
    removed = _THREE(_DECLARED_BY_RECORDS)[which]
    shrunk = {
        key: value
        for key, value in upstream_declarations.CONNECTION_PATH_DESCRIPTIONS.items()
        if key != removed
    }
    message = _live_description_map_failure(shrunk)
    assert message is not None and f"['{removed}']" in message
    monkeypatch.setattr(upstream_declarations, "CONNECTION_PATH_DESCRIPTIONS", shrunk)
    with pytest.raises(UpstreamDeclarationError, match=removed):
        load_upstream_facts()


@pytest.mark.parametrize("removed", _UNCOVERED)
def test_drift_a_pinned_path_the_map_does_not_describe_fails_in_the_check_and_the_loader(
    monkeypatch, removed
):
    shrunk = {
        key: value
        for key, value in upstream_declarations.CONNECTION_PATH_DESCRIPTIONS.items()
        if key != removed
    }
    message = _live_description_map_failure(shrunk)
    assert message is not None and f"['{removed}']" in message and "pinned literal" in message
    monkeypatch.setattr(upstream_declarations, "CONNECTION_PATH_DESCRIPTIONS", shrunk)
    with pytest.raises(UpstreamDeclarationError, match=removed):
        load_upstream_facts()
