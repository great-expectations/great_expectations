"""Cross-checks between the fluent-type contract table and the tier the registry records claim.

A record claims `SupportTier.FLUENT_API` by asserting that every fluent datasource `type` literal
it corresponds to satisfies the create/update/create-or-update contract. That claim is only as
good as two things lining up: every type a claiming record names must actually have a contract
table entry, and every type it names must actually exist in the fluent datasource registry. This
module checks both directions against the live registry, and pins the two gaps the current
registrations and the current contract table leave: fluent types no record names, and covered
records that stop short of the claim because they declare neither a marker nor a CI lane.
"""

from __future__ import annotations

import pytest

from great_expectations.datasource.fluent.sources import DataSourceManager
from tests.datasource.fluent.crud_contract import (
    FLUENT_TYPES_NAMED_BY_NO_RECORD,
    RECORDS_COVERED_BUT_UNABLE_TO_CLAIM,
    covered_fluent_types,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceSpec,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import (
    isolated_registry,
    iter_data_sources,
    register_data_source,
)

pytestmark = pytest.mark.project


def _fluent_api_records() -> tuple[DataSourceSpec, ...]:
    """Registered records claiming the fluent-API tier, read fresh at call time."""
    return tuple(
        registered.spec
        for registered in iter_data_sources()
        if SupportTier.FLUENT_API in registered.spec.tiers
    )


def _registered_fluent_type_names() -> frozenset[str]:
    """Every `type` literal the fluent datasource registry holds, read fresh at call time."""
    return frozenset(DataSourceManager.type_lookup.type_names())


def _assert_every_claiming_record_names_only_covered_types() -> None:
    """Raise naming the record and the type, the first time a tier claim outruns the table."""
    covered = covered_fluent_types()
    for spec in _fluent_api_records():
        for fluent_type in spec.fluent_types:
            if fluent_type not in covered:
                raise AssertionError(
                    f"{spec.label!r} claims the fluent-API tier and names fluent type "
                    f"{fluent_type!r}, but no CRUD contract parameters are declared for that "
                    f"type"
                )


def _assert_every_claiming_record_names_a_registered_type() -> None:
    """Raise naming the record and the type, the first time a tier claim names a type that
    does not exist in the fluent datasource registry."""
    registered_types = _registered_fluent_type_names()
    for spec in _fluent_api_records():
        for fluent_type in spec.fluent_types:
            if fluent_type not in registered_types:
                raise AssertionError(
                    f"{spec.label!r} claims the fluent-API tier and names fluent type "
                    f"{fluent_type!r}, but the fluent datasource registry holds no such type"
                )


def test_registry_is_not_empty() -> None:
    """An empty registry would let every assertion below pass for lack of anything to check."""
    assert len(iter_data_sources()) > 0


def test_contract_table_is_not_empty() -> None:
    """An empty contract table would let every assertion below pass for lack of anything to
    check."""
    assert len(covered_fluent_types()) > 0


def test_every_fluent_api_record_names_only_covered_types() -> None:
    """Every fluent type a tier-claiming record names must have a contract table entry."""
    _assert_every_claiming_record_names_only_covered_types()


def test_every_fluent_api_record_names_a_registered_type() -> None:
    """Every fluent type a tier-claiming record names must exist in the fluent type registry."""
    _assert_every_claiming_record_names_a_registered_type()


def test_fluent_types_named_by_no_record_matches_the_pinned_set() -> None:
    """The set of registered fluent types no record's `fluent_types` names.

    Pinned in `tests/datasource/fluent/crud_contract.py`, where a compatibility-reference
    generator reads it. Checked here against one snapshot of the live registry, so a drift in
    either direction — a record starting to name one of these types, or a new gap opening up —
    fails loudly here rather than only downstream in the generator.
    """
    named_by_some_record: set[str] = set()
    for registered in iter_data_sources():
        named_by_some_record.update(registered.spec.fluent_types)
    actual_gap = _registered_fluent_type_names() - named_by_some_record

    assert actual_gap == FLUENT_TYPES_NAMED_BY_NO_RECORD


def test_records_unable_to_claim_the_tier_matches_the_pinned_set() -> None:
    """The set of contract-covered records that cannot claim the tier.

    A record can only claim `SupportTier.FLUENT_API` if it declares both a marker and a CI lane,
    which any tier claim requires. A record whose fluent types are all contract-covered but which
    declares neither is stuck short of the claim for a reason that has nothing to do with the
    contract table. Pinned in `crud_contract.py` for the same downstream reader as the sibling
    literal above.
    """
    covered = covered_fluent_types()
    actual_gap = {
        registered.spec.label
        for registered in iter_data_sources()
        if registered.spec.fluent_types
        and registered.spec.fluent_types <= covered
        and SupportTier.FLUENT_API not in registered.spec.tiers
        and (registered.spec.marker is None or registered.spec.ci_lane is None)
    }

    assert actual_gap == RECORDS_COVERED_BUT_UNABLE_TO_CLAIM


def test_a_record_claiming_the_tier_with_an_uncovered_type_is_rejected() -> None:
    """A throwaway record naming a type absent from the contract table fails the check.

    Registered inside the isolation seam so the failing record never reaches the real registry;
    the assertion after the failure proves the seam did its job.
    """
    before = iter_data_sources()
    uncovered_type = "not-a-real-fluent-type-anyone-would-declare"
    assert uncovered_type not in covered_fluent_types()

    with isolated_registry():
        register_data_source(
            DataSourceSpec(
                label="throwaway-uncovered-type",
                public_name="Throwaway Uncovered Type",
                provisioning=DataSourceProvisioning.IN_PROCESS,
                fluent_types=frozenset({uncovered_type}),
                marker="throwaway_uncovered_type",
                ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway"),
                tiers=frozenset({SupportTier.FLUENT_API}),
            )
        )
        with pytest.raises(
            AssertionError,
            match=r"throwaway-uncovered-type.*not-a-real-fluent-type-anyone-would-declare",
        ):
            _assert_every_claiming_record_names_only_covered_types()

    assert iter_data_sources() == before


def test_a_record_naming_an_unregistered_fluent_type_is_rejected() -> None:
    """A throwaway record naming a fluent type the registry does not hold fails the check.

    Registered inside the isolation seam so the failing record never reaches the real registry;
    the assertion after the failure proves the seam did its job.
    """
    before = iter_data_sources()
    unregistered_type = "not-a-registered-fluent-type-anyone-would-declare"
    assert unregistered_type not in _registered_fluent_type_names()

    with isolated_registry():
        register_data_source(
            DataSourceSpec(
                label="throwaway-unregistered-type",
                public_name="Throwaway Unregistered Type",
                provisioning=DataSourceProvisioning.IN_PROCESS,
                fluent_types=frozenset({unregistered_type}),
                marker="throwaway_unregistered_type",
                ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway"),
                tiers=frozenset({SupportTier.FLUENT_API}),
            )
        )
        with pytest.raises(
            AssertionError,
            match=r"throwaway-unregistered-type.*not-a-registered-fluent-type-anyone-would-declare",
        ):
            _assert_every_claiming_record_names_a_registered_type()

    assert iter_data_sources() == before
