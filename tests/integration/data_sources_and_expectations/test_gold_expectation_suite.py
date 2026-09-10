"""The gold-tier suite: the gallery-wide expectation suite every gold-tier data source proves.

This module builds the suite's collection-time parameterization -- the (case, data source) product
that turns the declarative case table in `gold_expectation_case_table.py` into parametrized test
items --
and the measurement switch that lets a candidate data source be run against the suite before it has
declared gold-tier membership.

Every case is parameterized through `data_sources_for_tier_case(SupportTier.GOLD, case.key)` and
through nothing else, so a member's declared `tier_case_exclusions` entry for a case takes effect no
matter which case asks, exactly as the curated-tier suite does in `test_curated_backend_suite.py`. A
case parameterized directly over a raw membership list would silently ignore that exclusion.

The product is built inside `pytest_generate_tests`, at collection time, rather than once at
import: the registry state that matters is the run's, not the import's, so a data source that
registers after this module is first imported is still picked up the next time collection runs.

An empty product is legal and is not an error: an unclaimed tier and a case every member has
excluded are both states this suite must be able to report, not states it crashes on. The existing
`parameterize_batch_for_data_sources` decorator raises on an empty data-source list; the builder
here deliberately does not.

The test functions that consume the generated parameters -- one per fixture shape, asserting what
a case proves -- are `test_standard_case`, `test_extra_table_case`, and `test_comparison_case`,
below. Each receives the batch (or comparison batch) the harness's own fixtures build from the
generated `TestConfig`, and the `GoldCase` carried alongside it, and asserts the three things a
case proves: the passing configuration succeeds, the failing configuration fails with a message
that says the case is not discriminating (never merely that the expectation failed), and neither
validation raised.
"""

from __future__ import annotations

import inspect
import sys
from dataclasses import replace
from types import SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Final,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    cast,
)

import pytest

if TYPE_CHECKING:
    from _pytest.mark.structures import ParameterSet

    from great_expectations.datasource.fluent.interfaces import Batch

import great_expectations.expectations as gxe
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from tests.integration.conftest import MultiSourceBatch
from tests.integration.conftest import TestConfig as _TestConfig
from tests.integration.data_sources_and_expectations.gold_expectation_case_table import (
    GOLD_CASE_KEYS,
    GOLD_CASES,
    _derive_case_keys,
)
from tests.integration.data_sources_and_expectations.gold_expectation_cases import (
    EXTRA_TABLE_SELF_REFERENCE,
    GOLD_EXTRA_TABLE_DATA,
    GOLD_EXTRA_TABLE_NAME,
    GOLD_FIXTURE_DATA,
    CaseFixtureShape,
    GoldCase,
)
from tests.integration.test_utils.data_source_config import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceTestConfig,
    SupportTier,
    data_sources_for_tier_case,
)
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.registry import (
    data_source_configs_for_tier,
    isolated_registry,
    iter_data_source_configs,
    register_sql_config,
)
from tests.integration.test_utils.execution_engine_kind import ExecutionEngineKind

# The pytest option that turns on measurement mode. Declared once here and read by name from
# `metafunc.config` rather than re-declared, so the option string used to register it
# (`tests/conftest.py`) and the string used to read it can never drift apart.
GOLD_MEASUREMENT_OPTION = "--gold-measurement"

# Which fixture shape each case-consuming test function in this module asserts. A later unit of
# work adds the functions themselves; this mapping is the contract they join by name. A function
# not listed here requests no gold parameterization at all, whatever fixtures it declares.
_SHAPE_BY_TEST_FUNCTION_NAME: Dict[str, CaseFixtureShape] = {
    "test_standard_case": CaseFixtureShape.STANDARD,
    "test_extra_table_case": CaseFixtureShape.EXTRA_TABLE,
    "test_comparison_case": CaseFixtureShape.COMPARISON,
}

# The two fixture names a case-consuming test function must request together for this module to
# parametrize it: the harness's own indirect batch-setup fixture, and the gold case itself.
_BATCH_SETUP_FIXTURE_NAME = "_batch_setup_for_datasource"
_GOLD_CASE_FIXTURE_NAME = "gold_case"

# The package the shipped core expectations are defined under. `gallery_expectation_types` keeps
# only registry entries whose implementation's defining module is this package or a submodule of
# it -- registration is a side effect of defining an `Expectation` subclass, so importing the
# community-contributed expectation packages, or defining one inside a test module, adds registry
# entries the shipped package itself never ships.
_GALLERY_MODULE_ROOT: Final[str] = "great_expectations.expectations.core"

# The package prefix that marks a registry entry as shipped by this project at all, core or not.
# A registered expectation whose defining module falls outside this prefix was registered by
# something else entirely -- a test module, a notebook, a downstream project -- and the shipped
# package makes no claim about it one way or the other; the exactness guard below is scoped to
# this prefix precisely so it has no business asserting about those entries.
_SHIPPED_MODULE_PREFIX: Final[str] = "great_expectations."


def _is_core_gallery_module(module_name: str) -> bool:
    return module_name == _GALLERY_MODULE_ROOT or module_name.startswith(_GALLERY_MODULE_ROOT + ".")


def _is_shipped_module(module_name: str) -> bool:
    return module_name == "great_expectations" or module_name.startswith(_SHIPPED_MODULE_PREFIX)


def gallery_expectation_types() -> FrozenSet[str]:
    """The expectation type names the shipped package registers from its own core package.

    Reads the live registry through its only two public accessors --
    `list_registered_expectation_implementations` and `get_expectation_impl`; there is no bulk
    class accessor -- so an expectation added upstream appears here with no edit to this module.
    Filtered on each implementation's defining module rather than on its name, because
    registration is a side effect of defining a class: a filesystem glob of `expect_*.py` module
    names would miss `unexpected_rows_expectation`, whose module does not match that pattern, and
    the core package's own export list overcounts by one, because it exports
    `ExpectMulticolumnValuesToBeUnique`, which declares no map metric and is therefore abstract
    and never registers.

    Returns a `frozenset` rather than any ordered collection, so no consumer can depend on the
    registry's insertion order -- that order is `dict` insertion order over the core package's
    import list, which currently happens to equal sorted order, but nothing guarantees it stays
    that way.
    """
    return frozenset(
        name
        for name in list_registered_expectation_implementations()
        if _is_core_gallery_module(get_expectation_impl(name).__module__)
    )


def _candidate_data_sources(
    case: GoldCase, *, measurement_mode: bool
) -> List[DataSourceTestConfig]:
    """The data sources offered to `case`, before the engine filter.

    Normal operation resolves through the tier-case accessor and through nothing else, so a
    member's declared exclusion for `case.key` takes effect. Measurement mode substitutes every
    registered data source that has a config class, unfiltered by tier and by exclusion, because a
    candidate has declared no membership and so has made no claim to be filtered against. A
    data source with no recorded execution engine is still offered here -- `_engine_applies`, run
    unconditionally over every candidate this function returns, already drops it, in both modes;
    filtering it out a second time here would duplicate that check rather than add one, since
    `GoldCase.engines` is never empty and never contains `None`, so `_engine_applies` can never
    see a case an engineless candidate would otherwise slip past.
    """
    if measurement_mode:
        return [
            cast("DataSourceTestConfig", config_class())
            for config_class in iter_data_source_configs()
        ]
    return data_sources_for_tier_case(SupportTier.GOLD, case.key)


def _engine_applies(config: DataSourceTestConfig, case: GoldCase) -> bool:
    """Whether `config`'s execution engine survives `case`'s engine restriction.

    A data source with no recorded execution engine never applies to any case: it names a storage
    target rather than something a single engine reads a batch from. `None` -- what an unrecorded
    engine reads as -- is never a member of `case.engines` (it is always `GoldCase`'s full default
    set or a proper, non-empty subset of `ExecutionEngineKind`, per `GoldCase.__post_init__`), so
    membership alone already excludes an engineless candidate without a separate `is None` guard.
    Neither drop is treated as an exclusion -- `tier_case_exclusions` is the one mechanism that
    means that, and this filter never consults it and never writes to it.
    """
    engine: Optional[ExecutionEngineKind] = config.DATA_SOURCE_SPEC.execution_engine
    return engine in case.engines


_TYPE_LIST_CASE_KEY: Final[str] = gxe.ExpectColumnValuesToBeInTypeList(
    column="increasing_key", type_list=["INTEGER"]
).expectation_type
_TYPE_CASE_KEY: Final[str] = gxe.ExpectColumnValuesToBeOfType(
    column="increasing_key", type_="INTEGER"
).expectation_type
# The two case keys whose passing/failing configurations assert a SQL dialect type name rather
# than a fixture-shape-independent fact. A type name is a property of the dialect the data source
# under test speaks, not something the case table can hardcode once for every backend, so these
# two are rebuilt per data source from that data source's own declaration below rather than taken
# verbatim from the case table.


def _resolve_case_for_config(case: GoldCase, config: DataSourceTestConfig) -> GoldCase:
    """Rebuild `case`'s passing/failing configurations from `config`'s declared type names, for
    the two cases that assert against a SQL dialect type name.

    Every other case is returned unchanged. For the two type-name cases, `config`'s spec is always
    a `SqlBackendSpec` -- both cases restrict `engines` to SQL -- so its declared
    `integer_column_type_name`/`non_integer_column_type_name` are read directly rather than
    branched on which data source this is. A spec that declares nothing still resolves here,
    to the same names the case table's own literals spell, so this rebuild is a no-op for it.
    """
    if case.key not in (_TYPE_LIST_CASE_KEY, _TYPE_CASE_KEY):
        return case
    spec = config.DATA_SOURCE_SPEC
    assert isinstance(spec, SqlBackendSpec), (
        f"gold case {case.key!r} is restricted to SQL engines, but {config.test_id!r}'s spec is "
        f"{type(spec).__name__}, not SqlBackendSpec"
    )
    integer_name = spec.integer_column_type_name
    non_integer_name = spec.non_integer_column_type_name
    if case.key == _TYPE_LIST_CASE_KEY:
        # BIGINT/SMALLINT stay as fixed fallback alternatives alongside the declared name;
        # de-duplicated so a dialect that declares one of them verbatim doesn't repeat it.
        type_list = list(dict.fromkeys([integer_name, "BIGINT", "SMALLINT"]))
        return replace(
            case,
            passing=gxe.ExpectColumnValuesToBeInTypeList(
                column="increasing_key", type_list=type_list
            ),
            failing=gxe.ExpectColumnValuesToBeInTypeList(
                column="increasing_key", type_list=[non_integer_name]
            ),
        )
    return replace(
        case,
        passing=gxe.ExpectColumnValuesToBeOfType(column="increasing_key", type_=integer_name),
        failing=gxe.ExpectColumnValuesToBeOfType(column="increasing_key", type_=non_integer_name),
    )


def _test_config_for(case: GoldCase, config: DataSourceTestConfig) -> _TestConfig:
    """The `_TestConfig` the existing indirect `_batch_setup_for_datasource` fixture consumes, built
    from `case`'s fixture shape and the shared fixture data `gold_expectation_cases` publishes.

    The `COMPARISON` shape uses the same data source, and the same shared frame, for both the base
    and the comparison side: `gold_expectation_cases` publishes one shared frame, and a case needing
    a comparison source needing different data from the base declares its own frame there when one
    is added, the same way `EXTRA_TABLE` cases share `GOLD_EXTRA_TABLE_DATA` rather than each
    inventing a second table.
    """
    if case.fixture_shape is CaseFixtureShape.STANDARD:
        return _TestConfig(data_source_config=config, data=GOLD_FIXTURE_DATA, extra_data={})
    if case.fixture_shape is CaseFixtureShape.EXTRA_TABLE:
        return _TestConfig(
            data_source_config=config,
            data=GOLD_FIXTURE_DATA,
            extra_data={GOLD_EXTRA_TABLE_NAME: GOLD_EXTRA_TABLE_DATA},
        )
    if case.fixture_shape is CaseFixtureShape.COMPARISON:
        return _TestConfig(
            data_source_config=config,
            data=GOLD_FIXTURE_DATA,
            extra_data={},
            secondary_source_config=config,
            secondary_data=GOLD_FIXTURE_DATA,
        )
    raise ValueError(
        f"Unhandled gold case fixture shape: {case.fixture_shape!r}"
    )  # pragma: no cover


def build_gold_case_params(
    cases: Sequence[GoldCase], *, measurement_mode: bool
) -> List[ParameterSet]:
    """The suite's collection-time (case, data source) product, as a flat list of `pytest.param`.

    For each case, in table order, resolves candidate data sources (through the accessor in normal
    operation, or through the measurement substitution), drops any pair whose data source's
    execution engine is outside the case's restriction or unrecorded, and emits one `pytest.param`
    per surviving pair. Each param's id names both the data source and the case key
    (`<data source test id>-<case key>`); each param's marks are the data source's own mark plus the
    suite marker (`pytest.mark.gold`).

    Legal to return an empty list -- an unclaimed tier or a fully excluded case are both reportable
    states, not errors -- and this never raises for either reason. `pytest.mark.parametrize` (via
    `metafunc.parametrize`) tolerates an empty parameter list on its own: it collects the test as a
    single item automatically skipped for an empty parameter set, rather than raising, which is what
    lets an empty tier and an empty case both survive collection.
    """
    params: List[ParameterSet] = []
    for case in cases:
        candidates = _candidate_data_sources(case, measurement_mode=measurement_mode)
        for config in candidates:
            if not _engine_applies(config, case):
                continue
            resolved_case = _resolve_case_for_config(case, config)
            params.append(
                pytest.param(
                    _test_config_for(resolved_case, config),
                    resolved_case,
                    id=f"{config.test_id}-{case.key}",
                    marks=[config.pytest_mark, pytest.mark.gold],
                )
            )
    return params


# The two skip reasons an empty (case, data source) product can mean, distinguished at the point
# they are told apart in `pytest_generate_tests`. Neither is a defect: both are legal end states,
# but they are different questions a reader would otherwise have to re-derive from the case
# table and the registry -- "is anything of this shape published at all" versus "did any admitted
# data source actually offer to run it".
_NO_CASE_OF_SHAPE_REASON: Final[str] = (
    "no gold case of this fixture shape is published in the case table"
)
_NO_DATA_SOURCE_CLAIMED_REASON: Final[str] = (
    "no data source offered any published case of this shape anything to validate "
    "(the tier may be unclaimed for this shape, or every candidate excluded or engine-restricted "
    "out of every case)"
)


def _empty_product_placeholder(reason: str) -> ParameterSet:
    """The single collected item substituted for a truly empty (case, data source) product.

    `metafunc.parametrize` with an empty `argvalues` list already collects one automatically
    skipped item on its own -- that mechanism is what lets an empty tier and a fully excluded case
    survive collection at all (see `build_gold_case_params`'s docstring) -- but pytest's own
    synthetic item carries none of this repo's required markers, which the marker-coverage check
    (`tests/conftest.py::_verify_marker_coverage`) treats as an uncovered test. This placeholder
    supplies that marker explicitly, plus an explicit (rather than pytest's generic) skip reason,
    naming which of the two ways an empty product can arise the caller has already told apart.

    Its `_batch_setup_for_datasource` and `gold_case` values are never touched: a `pytest.mark.skip`
    on a parametrized item's marks makes pytest skip the item during test *setup*, before any
    fixture (including an indirect one) runs -- verified directly against this repo's fixture, not
    assumed from pytest's docs.
    """
    return pytest.param(
        None,
        None,
        id="no-data-source",
        marks=[
            pytest.mark.project,
            pytest.mark.skip(reason=reason),
        ],
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Build the (case, data source) product for this module's case-shape test functions.

    Runs at collection time, once per matching test function, so the registry state it reads is the
    run's registry state rather than whatever the registry held when this module was first
    imported. A function this module has not declared a shape for, or that does not request both
    `_batch_setup_for_datasource` and `gold_case`, is left untouched.
    """
    shape = _SHAPE_BY_TEST_FUNCTION_NAME.get(metafunc.function.__name__)
    if shape is None:
        return
    if _BATCH_SETUP_FIXTURE_NAME not in metafunc.fixturenames:
        return
    if _GOLD_CASE_FIXTURE_NAME not in metafunc.fixturenames:
        return

    measurement_mode = bool(metafunc.config.getoption(GOLD_MEASUREMENT_OPTION))
    cases = [case for case in GOLD_CASES if case.fixture_shape is shape]
    params = build_gold_case_params(cases, measurement_mode=measurement_mode)
    if not params:
        # `cases` empty means the case table publishes nothing of this shape at all; `cases`
        # non-empty but `params` empty means every published case of this shape found no data
        # source to run it against (an unclaimed tier, or every candidate excluded or
        # engine-restricted out). The two are told apart here, at collection time, because this
        # is the one place both facts -- the case table's own contents and the resolved product --
        # are already in hand together.
        reason = _NO_CASE_OF_SHAPE_REASON if not cases else _NO_DATA_SOURCE_CLAIMED_REASON
        params = [_empty_product_placeholder(reason)]
    metafunc.parametrize(
        [_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
        params,
        indirect=[_BATCH_SETUP_FIXTURE_NAME],
    )


if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult as _ExpectationValidationResult,
    )


def _raised_exception(exception_info: Mapping[str, object]) -> bool:
    """Whether `exception_info` records a raised exception, in either shape
    `ExpectationValidationResult.exception_info` takes.

    The documented, ordinary shape is flat: `{"raised_exception": bool, ...}`. But when metric
    *resolution* itself fails -- for instance, a column that does not exist in the batch -- GX
    instead reports a dict keyed by metric-configuration-id, each value itself the same flat
    shape. `exception_info.get("raised_exception")` alone silently reads `None` (falsy) for that
    second shape, which would let a case whose configuration crashed pass this suite's "neither
    validation raised" assertion instead of failing it.
    """
    direct = exception_info.get("raised_exception")
    if isinstance(direct, bool):
        return direct
    return any(
        isinstance(value, Mapping) and bool(value.get("raised_exception"))
        for value in exception_info.values()
    )


def _describe_observed_value(result: _ExpectationValidationResult) -> str:
    """Name the single fact a failed assertion needs to diagnose: what the expectation actually
    observed.

    Most expectations report `observed_value` under `result.result`, but not all do, and among
    those that do, the value can be a scalar, a list, or a dict. Report whichever is present, and
    say plainly when there isn't one, rather than letting a missing key surface as a `KeyError` or
    a silent `None`.
    """
    result_dict = result.result
    if not isinstance(result_dict, Mapping) or "observed_value" not in result_dict:
        return "observed_value=<not reported by this expectation>"
    return f"observed_value={result_dict['observed_value']!r}"


def _assert_case_proves_its_expectation(
    case: GoldCase,
    *,
    validate_passing: Callable[[], _ExpectationValidationResult],
    validate_failing: Callable[[], _ExpectationValidationResult],
) -> None:
    """Assert the three verdicts every gold case proves, per case-shape test function below.

    `validate_passing` and `validate_failing` run `case.passing`/`case.failing` against this
    test's batch and return the result. Taking callables rather than results keeps this helper
    identical across shapes despite each shape resolving and validating its case differently.
    """
    passing_result = validate_passing()
    assert not _raised_exception(passing_result.exception_info), (
        f"gold case {case.key!r}: the passing configuration raised during "
        f"validation instead of evaluating cleanly: {passing_result.exception_info}"
    )
    assert passing_result.success, (
        f"gold case {case.key!r}: the configuration declared as `passing` reported failure. "
        f"{_describe_observed_value(passing_result)} result={passing_result}"
    )

    failing_result = validate_failing()
    assert not _raised_exception(failing_result.exception_info), (
        f"gold case {case.key!r}: the failing configuration raised during validation instead of "
        f"evaluating to false: {failing_result.exception_info}"
    )
    assert not failing_result.success, (
        f"gold case {case.key!r} is not discriminating: its `failing` configuration reported "
        "success against the shared fixture data, so this case cannot distinguish a working "
        "implementation of the expectation from a broken one. This is not an ordinary expectation "
        "failure -- fix the case's `failing` configuration or fixture data. "
        f"{_describe_observed_value(failing_result)} result={failing_result}"
    )


def test_standard_case(
    batch_for_datasource: Batch,
    gold_case: GoldCase,
) -> None:
    """Assert what a `STANDARD`-shape gold case proves, against the shared fixture alone."""
    _assert_case_proves_its_expectation(
        gold_case,
        validate_passing=lambda: batch_for_datasource.validate(gold_case.passing),
        validate_failing=lambda: batch_for_datasource.validate(gold_case.failing),
    )


def test_extra_table_case(
    batch_for_datasource: Batch,
    _batch_setup_for_datasource: object,
    extra_table_names_for_datasource: Mapping[str, str],
    gold_case: GoldCase,
) -> None:
    """Assert what an `EXTRA_TABLE`-shape gold case proves, resolving each configuration's
    placeholder table reference (`EXTRA_TABLE_SELF_REFERENCE`, or the shared extra table's
    logical name) to the physical table name this test's own batch setup created."""
    from tests.integration.test_utils.data_source_config.sql import SQLBatchTestSetup

    assert isinstance(_batch_setup_for_datasource, SQLBatchTestSetup)
    primary_table_name = _batch_setup_for_datasource.table_name

    def _resolve(
        expectation: gxe.Expectation,
    ) -> gxe.ExpectTableRowCountToEqualOtherTable:
        assert isinstance(expectation, gxe.ExpectTableRowCountToEqualOtherTable)
        placeholder = expectation.other_table_name
        assert isinstance(placeholder, str), (
            f"gold case {gold_case.key!r}: expected a placeholder `other_table_name` string, "
            f"got {placeholder!r}"
        )
        other_table_name = (
            primary_table_name
            if placeholder == EXTRA_TABLE_SELF_REFERENCE
            else extra_table_names_for_datasource[placeholder]
        )
        resolved = expectation.copy(update={"other_table_name": other_table_name})
        assert isinstance(resolved, gxe.ExpectTableRowCountToEqualOtherTable)
        return resolved

    _assert_case_proves_its_expectation(
        gold_case,
        validate_passing=lambda: batch_for_datasource.validate(_resolve(gold_case.passing)),
        validate_failing=lambda: batch_for_datasource.validate(_resolve(gold_case.failing)),
    )


def test_comparison_case(
    multi_source_batch: MultiSourceBatch,
    gold_case: GoldCase,
) -> None:
    """Assert what a `COMPARISON`-shape gold case proves, resolving each configuration's
    placeholder comparison-source reference to the comparison batch this test's own multi-source
    setup created."""

    def _resolve(
        expectation: gxe.Expectation,
    ) -> gxe.ExpectQueryResultsToMatchComparison:
        assert isinstance(expectation, gxe.ExpectQueryResultsToMatchComparison)
        comparison_query = expectation.comparison_query
        assert isinstance(comparison_query, str), (
            f"gold case {gold_case.key!r}: expected a placeholder `comparison_query` string, "
            f"got {comparison_query!r}"
        )
        resolved_query = comparison_query.replace(
            "{source_table}", multi_source_batch.comparison_table_name
        )
        resolved = expectation.copy(
            update={
                "comparison_data_source_name": multi_source_batch.comparison_data_source_name,
                "comparison_query": resolved_query,
            }
        )
        assert isinstance(resolved, gxe.ExpectQueryResultsToMatchComparison)
        return resolved

    _assert_case_proves_its_expectation(
        gold_case,
        validate_passing=lambda: multi_source_batch.base_batch.validate(
            _resolve(gold_case.passing)
        ),
        validate_failing=lambda: multi_source_batch.base_batch.validate(
            _resolve(gold_case.failing)
        ),
    )


@pytest.mark.project
def test_shape_dispatch_names_a_real_function_for_every_key_and_vice_versa() -> None:
    """`_SHAPE_BY_TEST_FUNCTION_NAME` is a bijection with the case-consuming functions in this
    module: every key names a real function here, and every case-consuming function here (one
    named `test_*_case`, taking the `gold_case` fixture) has a key. Without this guard, renaming
    a consumer function silently drops its shape from collection -- `pytest_generate_tests` would
    just never dispatch to it, yielding zero collected cases for that shape with a green suite."""
    module_globals = sys.modules[__name__].__dict__
    for function_name in _SHAPE_BY_TEST_FUNCTION_NAME:
        candidate = module_globals.get(function_name)
        assert callable(candidate), (
            f"_SHAPE_BY_TEST_FUNCTION_NAME names {function_name!r}, but this module defines no "
            "such function."
        )

    case_consuming_names = {
        name
        for name, value in module_globals.items()
        if callable(value)
        and getattr(value, "__module__", None) == __name__
        and _GOLD_CASE_FIXTURE_NAME in inspect.signature(value).parameters
    }
    assert case_consuming_names == set(_SHAPE_BY_TEST_FUNCTION_NAME), (
        "A function in this module requests the `gold_case` fixture but has no entry in "
        "_SHAPE_BY_TEST_FUNCTION_NAME, or vice versa. "
        f"dispatch={sorted(_SHAPE_BY_TEST_FUNCTION_NAME)} "
        f"consumers={sorted(case_consuming_names)}"
    )


# --------------------------------------------------------------------------------------------
# Coverage for the registry-derived gallery set
# --------------------------------------------------------------------------------------------
#
# gallery_expectation_types() needs no data source, so it and its guards run in the project-scoped
# lane. The filtering behavior is proven against a stubbed registry (below) so the assertion does
# not depend on what the live registry happens to hold today; the non-vacuity and exactness checks
# are then repeated against the real, live registry, because those two are exactly the properties
# that must hold of the tree as it actually stands.


@pytest.mark.project
def test_gallery_expectation_types_filters_on_defining_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a registry entry whose implementation is defined under the core package survives --
    not one from a contrib package, and not one defined inside this very test module."""
    fake_impls: Dict[str, type] = {
        "core_expectation": type(
            "CoreExpectation",
            (),
            {"__module__": "great_expectations.expectations.core.expect_thing"},
        ),
        "contrib_expectation": type(
            "ContribExpectation",
            (),
            {"__module__": "great_expectations_contrib.expectations.expect_other"},
        ),
        "test_defined_expectation": type("TestDefinedExpectation", (), {"__module__": __name__}),
    }
    # Patched on this module's own object, not by dotted path: this directory has no
    # `__init__.py`, so pytest's collected instance of this module and the one a dotted-path
    # patch would resolve via PEP 420 are two different objects.
    module = sys.modules[__name__]
    monkeypatch.setattr(
        module, "list_registered_expectation_implementations", list(fake_impls).copy
    )
    monkeypatch.setattr(module, "get_expectation_impl", lambda name: fake_impls[name])

    result = gallery_expectation_types()

    assert result == frozenset({"core_expectation"}), (
        "gallery_expectation_types() must keep only the entry defined under "
        f"{_GALLERY_MODULE_ROOT!r}; got {sorted(result)}."
    )


@pytest.mark.project
def test_gallery_expectation_types_is_non_empty() -> None:
    """A vacuously-true completeness guard is exactly the failure mode this proves against: if the
    registry were empty or the module filter matched nothing, every downstream coverage guard
    built on this set would pass having checked nothing at all."""
    result = gallery_expectation_types()
    assert len(result) > 0, (
        "gallery_expectation_types() returned an empty set. Either the live registry is empty or "
        f"the module filter matched no registered expectation under {_GALLERY_MODULE_ROOT!r}; "
        "either way, every guard built on this set would now pass vacuously."
    )


@pytest.mark.project
def test_gallery_expectation_types_matches_every_registered_expectation() -> None:
    """Filter exactness, asserted rather than assumed, over shipped registrations only: every
    registered expectation whose defining module is part of this project resolves under the core
    package. If a future change moved a shipped expectation's class out of the core package, it
    would still register from a shipped module -- just not a core one -- and this fails loudly at
    the point of registration, rather than the gallery set silently losing that member and the
    completeness guard built on it shrinking to match, undetected.

    Scoped to modules under `great_expectations.` rather than iterating the whole live registry
    unconditionally: in a shared pytest session, the registry can (and does) hold entries an
    unrelated test module registered for its own purposes, which are not shipped by this project
    at all and which this guard has no business asserting about. Those are excluded by construction
    -- checking the defining module's own prefix -- rather than by naming the specific modules that
    happen to pollute today's session, so the next unrelated test-registered expectation is
    excluded the same way without this test changing.
    """
    registered = list_registered_expectation_implementations()
    for name in registered:
        module_name = get_expectation_impl(name).__module__
        if not _is_shipped_module(module_name):
            continue
        assert _is_core_gallery_module(module_name), (
            f"{name!r} is registered from {module_name!r}, which is a shipped module but not "
            f"under {_GALLERY_MODULE_ROOT!r}. A shipped core expectation's module has changed and "
            "the gallery derivation is now silently dropping it."
        )


@pytest.mark.project
def test_gallery_expectation_types_includes_non_conventional_module_name() -> None:
    """`unexpected_rows_expectation`'s module name does not match the `expect_*.py` pattern a
    filesystem glob over the core package would use. The registry-based derivation includes it
    anyway, because it filters on the registered implementation's actual module, not its name."""
    assert "unexpected_rows_expectation" in gallery_expectation_types()


@pytest.mark.project
def test_gallery_expectation_types_excludes_unregistered_abstract_export() -> None:
    """The core package exports `ExpectMulticolumnValuesToBeUnique`, but it declares no map
    metric and so never registers. A derivation built off the core package's export list would
    overcount by this one; the registry-based derivation here cannot, because it never looks at
    the export list at all."""
    from great_expectations.expectations.core import ExpectMulticolumnValuesToBeUnique

    assert not list_registered_expectation_implementations(
        expectation_root=ExpectMulticolumnValuesToBeUnique
    ), (
        "ExpectMulticolumnValuesToBeUnique is registered under the live registry; the abstract-"
        "export trap this test documents no longer applies and its docstring needs updating."
    )


# --------------------------------------------------------------------------------------------
# The suite's guards
# --------------------------------------------------------------------------------------------
#
# All six guards below need no data source and run in the project-scoped lane, so a missing case,
# a stale exclusion, an empty tier, or a runaway engine restriction is reported by the fastest
# feedback available rather than only by a warehouse lane.
#
# The case table's own key-derivation guard (`_derive_case_keys`) is not one of those six: it is
# not a test at all but a check the case table makes as it is imported, so it runs in every lane
# rather than this one. It sits underneath the completeness guard rather than beside it -- it is
# what makes `GOLD_CASE_KEYS` a faithful index of the table, without which comparing that key set
# against the registry means nothing. Only its two failure paths are exercised here, as tests: a
# repeated key, and a key naming an expectation its own configurations do not execute.
#
# The completeness guard runs against the real registry and GOLD_CASE_KEYS -- it is currently
# vacuous in the sense that both sides already agree (58 published keys, 58 gallery members), but
# it is not vacuous in the sense that matters: `_completeness_check` is factored out so its failure
# path can be, and is, exercised directly below against a deliberately incomplete key set, proving
# the guard can actually fail rather than merely being green over an untested comparison.
#
# The exclusion-key and accessor-equality guards now exercise real declarations: several data
# sources have declared `SupportTier.GOLD` membership, so both guards check against a non-empty
# tier for the first time. Neither declares a `tier_case_exclusions` entry for the tier, since
# every admitted member passes every case it applies to -- the exclusion-key guard is exercised by
# the check itself walking every registered config's declarations (finding none for this tier),
# and the accessor-equality guard is exercised by comparing the accessor's result against the
# tier's real membership for every published key.
#
# The tier-non-empty guard is new: it fails the moment the tier's membership drops to zero, so the
# suite cannot silently degrade back into the vacuous state the guards above once tolerated.
#
# The applicability guard is not vacuous: every restricted-engine case already has real, registered
# candidates to remove, so it is checked against the live registry directly.


def _completeness_check(published_keys: FrozenSet[str], gallery_keys: FrozenSet[str]) -> None:
    """The published case-key set must equal the derived gallery set in both directions.

    Factored out from `test_gold_case_keys_match_the_gallery_set_exactly` so its failure path can
    be exercised directly, with a deliberately incomplete key set, in
    `test_completeness_guard_fails_on_missing_and_unrecognized_keys` below -- proving this check
    can fail rather than merely being green over the two sets happening to agree today.
    """
    missing = gallery_keys - published_keys
    assert not missing, (
        f"The following registered expectations have no published gold case: {sorted(missing)}. "
        "Add a GoldCase for each to GOLD_CASES in gold_expectation_case_table.py."
    )
    unrecognized = published_keys - gallery_keys
    assert not unrecognized, (
        f"The following published gold case keys do not name a currently registered expectation: "
        f"{sorted(unrecognized)}. Either the expectation was removed from the shipped package "
        "(delete the case) or the key is misspelled (fix it to match the registered "
        "expectation_type) -- check both."
    )


@pytest.mark.project
def test_gold_case_keys_match_the_gallery_set_exactly() -> None:
    """`GOLD_CASE_KEYS` and `gallery_expectation_types()` must be exactly equal: an expectation
    registered with no case, and a case naming an unregistered expectation, are both a defect."""
    _completeness_check(GOLD_CASE_KEYS, gallery_expectation_types())


@pytest.mark.project
def test_completeness_guard_fails_on_missing_and_unrecognized_keys() -> None:
    """`_completeness_check` actually fails, in each direction, with a message naming the
    offending key -- proof the completeness guard is not merely green over nothing."""
    gallery = frozenset({"expect_kept_case", "expect_missing_case"})

    with pytest.raises(AssertionError, match=r"no published gold case.*expect_missing_case"):
        _completeness_check(frozenset({"expect_kept_case"}), gallery)

    with pytest.raises(AssertionError, match=r"do not name a currently registered.*extra_key"):
        _completeness_check(
            frozenset({"expect_kept_case", "expect_missing_case", "extra_key"}), gallery
        )


@pytest.mark.project
def test_case_key_derivation_returns_every_key_of_a_well_formed_table() -> None:
    """The happy path: a table whose cases each name the expectation they execute derives to
    exactly that set of keys, so the failure-path tests below are not the whole of this guard."""
    not_null = gxe.ExpectColumnValuesToNotBeNull(column="increasing_key")
    be_null = gxe.ExpectColumnValuesToBeNull(column="nullable_value")
    cases = (
        GoldCase(key=not_null.expectation_type, passing=not_null, failing=not_null),
        GoldCase(key=be_null.expectation_type, passing=be_null, failing=be_null),
    )
    assert _derive_case_keys(cases) == frozenset(
        {not_null.expectation_type, be_null.expectation_type}
    )


@pytest.mark.project
def test_case_key_derivation_rejects_a_repeated_key() -> None:
    """Two cases for one expectation must fail loudly rather than collapse into one key.

    A plain `frozenset` comprehension absorbs the duplicate silently, and the completeness guard
    reads only the resulting set -- so without this check the table could carry two cases for one
    expectation, contradicting its stated one-case-per-expectation contract, and stay green.
    """
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="increasing_key")
    case = GoldCase(key=expectation.expectation_type, passing=expectation, failing=expectation)

    with pytest.raises(ValueError, match=r"more than one case for.*expect_column_values_to_not"):
        _derive_case_keys((case, case))


@pytest.mark.project
@pytest.mark.parametrize("mismatched_side", ["passing", "failing"])
def test_case_key_derivation_rejects_a_key_its_configurations_do_not_execute(
    mismatched_side: str,
) -> None:
    """A case whose key names one expectation while a configuration executes another must fail.

    This is the hole the completeness guard cannot see on its own: it reads `case.key`, while the
    suite runs `case.passing` and `case.failing`. A case copied and re-keyed without its
    configurations changing with it would report coverage of the newly named expectation while
    executing the old one -- the named expectation neither run nor reported missing. Both sides are
    checked, so a guard that only looked at one of them fails this test.
    """
    keyed_as = gxe.ExpectColumnValuesToNotBeNull(column="increasing_key")
    other = gxe.ExpectColumnValuesToBeNull(column="nullable_value")
    case = GoldCase(
        key=keyed_as.expectation_type,
        passing=other if mismatched_side == "passing" else keyed_as,
        failing=other if mismatched_side == "failing" else keyed_as,
    )

    with pytest.raises(ValueError, match=rf"{mismatched_side} configuration of"):
        _derive_case_keys((case,))


@pytest.mark.project
def test_gold_tier_has_at_least_one_member() -> None:
    """`SupportTier.GOLD` must never go back to having no members.

    Before any data source joined the tier, an empty suite was the correct, legal state -- the
    fixture-shape test functions collect a single skipped placeholder rather than failing. Once a
    member exists, an empty tier stops being a start-of-life condition and starts being a
    regression: it would mean every member that once claimed this tier's coverage was removed
    without anything else taking its place, and the suite would silently go back to proving
    nothing while still reporting green.
    """
    members = data_source_configs_for_tier(SupportTier.GOLD)
    assert members, (
        "No data source declares SupportTier.GOLD membership. If every prior member was removed "
        "deliberately, this suite is a no-op; if not, restore the missing tiers= declaration(s)."
    )


@pytest.mark.project
def test_every_gold_exclusion_key_is_a_published_case_key() -> None:
    """Every case key any registered backend declares a gold-tier exclusion for must be one of
    `GOLD_CASE_KEYS`.

    Several backends now declare `SupportTier.GOLD` membership, so this walks their real
    `tier_case_exclusions` declarations -- none of them holds a `GOLD` entry, because every
    admitted member passes every case it applies to, but this guard raises the moment a backend
    declares one naming a stale or misspelled key, rather than that key silently excluding
    nothing while reading, on inspection, as a real exclusion.
    """
    for config_class in iter_data_source_configs():
        gold_exclusions = config_class.DATA_SOURCE_SPEC.tier_case_exclusions.get(
            SupportTier.GOLD, {}
        )
        for excluded_key in gold_exclusions:
            assert excluded_key in GOLD_CASE_KEYS, (
                f"{config_class.__name__} declares a gold-tier case exclusion for "
                f"{excluded_key!r}, which is not one of GOLD_CASE_KEYS "
                f"({sorted(GOLD_CASE_KEYS)})."
            )


@pytest.mark.project
def test_gold_case_accessor_matches_tier_membership_minus_declared_exclusions() -> None:
    """For every published gold case key, `data_sources_for_tier_case(GOLD, key)` must equal the
    tier's live membership minus whichever members declare an exclusion for that key, computed
    from each member's own live `tier_case_exclusions` rather than assumed.

    `SupportTier.GOLD` now has real members, so both sides of this comparison are computed from
    the live registry for every key -- this is the first point this guard checks anything beyond
    two empty lists agreeing.
    """
    tier_members = [
        cast("DataSourceTestConfig", config_class())
        for config_class in data_source_configs_for_tier(SupportTier.GOLD)
    ]
    for case_key in GOLD_CASE_KEYS:
        expected = [
            config
            for config in tier_members
            if case_key
            not in config.DATA_SOURCE_SPEC.tier_case_exclusions.get(SupportTier.GOLD, {})
        ]
        actual = data_sources_for_tier_case(SupportTier.GOLD, case_key)
        assert actual == expected, (
            f"data_sources_for_tier_case(GOLD, {case_key!r}) returned "
            f"{[c.label for c in actual]!r}, expected {[c.label for c in expected]!r} (tier "
            "membership minus each member's declared exclusion for this key)"
        )


@pytest.mark.project
def test_engine_restrictions_remove_exactly_the_data_sources_outside_their_engine_set() -> None:
    """For every gold case declaring a restricted engine set, the exact set of registered data
    sources the restriction removes must equal the set the restriction claims to remove -- no
    more, no less -- so a restriction cannot quietly grow into a blanket exemption.

    The expected removed set is computed here directly from each registered config's own declared
    execution engine, independently of `_engine_applies`/`build_gold_case_params` (the production
    code this test drives through measurement mode to get the *actual* surviving set) -- so a
    defect in either side is still caught by the other.
    """
    all_configs = [
        cast("DataSourceTestConfig", config_class()) for config_class in iter_data_source_configs()
    ]
    all_labels = {config.label for config in all_configs}
    restricted_cases = [
        case for case in GOLD_CASES if case.engines != frozenset(ExecutionEngineKind)
    ]
    assert restricted_cases, (
        "expected at least one gold case to declare a restricted engine set; if every case now "
        "applies to every engine, this guard has nothing to check and should be revisited"
    )
    for case in restricted_cases:
        assert case.engine_restriction_reason, (
            f"gold case {case.key!r} restricts its engine set but declares no "
            "engine_restriction_reason"
        )
        expected_removed_labels = {
            config.label
            for config in all_configs
            if config.DATA_SOURCE_SPEC.execution_engine not in case.engines
        }
        params = build_gold_case_params([case], measurement_mode=True)
        surviving_labels = {_param_test_config(param).data_source_config.label for param in params}
        assert surviving_labels == all_labels - expected_removed_labels, (
            f"gold case {case.key!r}'s engine restriction to "
            f"{sorted(engine.value for engine in case.engines)} removed "
            f"{sorted(all_labels - surviving_labels)}, expected to remove exactly "
            f"{sorted(expected_removed_labels)}"
        )


# --------------------------------------------------------------------------------------------
# Coverage for the collection-time builder
# --------------------------------------------------------------------------------------------
#
# Several real data sources now declare SupportTier.GOLD, but the tests below still exercise the
# builder against throwaway cases and throwaway registrations wrapped in `isolated_registry()`,
# per the module docstring, so this coverage never depends on which data sources happen to be
# real members today. The "unclaimed tier" and "no case of this shape" states the builder must
# survive without raising are reproduced here with a throwaway, isolated registry rather than by
# depending on the live registry ever going empty again -- which the tier-non-empty guard above
# now forbids.


def _make_throwaway_case(
    key: str, *, engines: Optional[FrozenSet[ExecutionEngineKind]] = None
) -> GoldCase:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="increasing_key")
    kwargs: dict[str, object] = {"key": key, "passing": expectation, "failing": expectation}
    if engines is not None:
        kwargs["engines"] = engines
        kwargs["engine_restriction_reason"] = "throwaway restriction for a builder test"
    return GoldCase(**kwargs)  # type: ignore[arg-type]


def _make_gold_backend_spec(**overrides: object) -> SqlBackendSpec:
    defaults: dict[str, object] = dict(
        label="throwaway-gold-backend",
        public_name="Throwaway Gold Backend",
        marker="throwaway_gold_backend",
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway_gold_backend"),
        uses_schema=False,
        tiers=frozenset({SupportTier.GOLD, SupportTier.CANONICAL_EXPECTATIONS}),
        execution_engine=ExecutionEngineKind.SQL,
    )
    defaults.update(overrides)
    return SqlBackendSpec(**defaults)  # type: ignore[arg-type]


def _make_config_class(name: str, spec: SqlBackendSpec) -> type:
    """A minimal, instantiable `DataSourceTestConfig` subclass carrying `spec`, so a throwaway
    registration behaves like a real config for every property `build_gold_case_params` reads
    (`.label`, `.pytest_mark`, `.test_id`, `.data_source_spec`) -- not just the raw class
    attribute the registry itself reads.
    """

    def _create_batch_setup(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError("this throwaway config is never used to create a real batch")

    return type(
        name,
        (DataSourceTestConfig,),
        {"DATA_SOURCE_SPEC": spec, "create_batch_setup": _create_batch_setup},
    )


def _param_test_config(param: ParameterSet) -> _TestConfig:
    """The `_TestConfig` half of `param.values`, narrowed by an `isinstance` assertion.

    `ParameterSet.values` types each element as `object | NotSetType`, so a direct attribute
    access on `param.values[0]` is unchecked -- mypy cannot see through it. Narrowing here, once,
    keeps every call site's assertion about what a param actually carries genuinely checked rather
    than silently `Any`.
    """
    test_config, _gold_case = param.values
    assert isinstance(test_config, _TestConfig)
    return test_config


def _param_gold_case(param: ParameterSet) -> GoldCase:
    """The `GoldCase` half of `param.values`, narrowed the same way as `_param_test_config`."""
    _test_config, gold_case = param.values
    assert isinstance(gold_case, GoldCase)
    return gold_case


class TestBuildGoldCaseParams:
    """Direct coverage of `build_gold_case_params`, the pure function `pytest_generate_tests`
    delegates to. Asserted against the returned `pytest.param` objects themselves -- their ids,
    their marks, and which `_TestConfig`/`GoldCase` pair they carry -- never by reaching a data
    source, since none of these tests set up a batch.

    Marked `project` here, on the class, rather than on the module: the generated case-shape
    tests a later unit of work adds to this module carry their own marks (a data source's own
    mark plus `gold`) through `build_gold_case_params` itself, and a module-level mark would give
    each of those a second required marker alongside its own.
    """

    pytestmark = pytest.mark.project

    def test_empty_case_list_returns_no_params(self) -> None:
        assert build_gold_case_params([], measurement_mode=False) == []

    def test_unclaimed_tier_returns_no_params_without_raising(self) -> None:
        # An isolated, empty registry reproduces "tier unclaimed" without depending on the live
        # registry ever going empty again -- several real data sources declare SupportTier.GOLD
        # today, and the tier-non-empty guard above means the live registry never will.
        case = _make_throwaway_case("unclaimed_tier_case")
        with isolated_registry():
            result = build_gold_case_params([case], measurement_mode=False)
        assert result == []

    def test_normal_mode_resolves_through_the_tier_case_accessor_and_honors_exclusion(self) -> None:
        case_key = "exclusion_honoring_case"
        excluded_spec = _make_gold_backend_spec(
            label="excluded-backend",
            public_name="Excluded Backend",
            marker="sqlite",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="sqlite"),
            tier_case_exclusions={SupportTier.GOLD: {case_key: "throwaway exclusion for a test"}},
        )
        included_spec = _make_gold_backend_spec(
            label="included-backend",
            public_name="Included Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
        )
        with isolated_registry():
            register_sql_config(_make_config_class("ExcludedBackend", excluded_spec))
            register_sql_config(_make_config_class("IncludedBackend", included_spec))

            case = _make_throwaway_case(case_key)
            params = build_gold_case_params([case], measurement_mode=False)

            expected_configs = data_sources_for_tier_case(SupportTier.GOLD, case_key)

        assert [config.label for config in expected_configs] == ["included-backend"]
        assert len(params) == 1
        [param] = params
        test_config, gold_case = param.values
        assert isinstance(test_config, _TestConfig)
        assert test_config.data_source_config.label == "included-backend"
        assert gold_case is case
        assert param.id == f"included-backend-{case_key}"
        assert {mark.name for mark in param.marks} == {"postgresql", "gold"}

    def test_engine_restriction_drops_a_pair_without_treating_it_as_an_exclusion(self) -> None:
        case_key = "engine_restricted_case"
        sql_spec = _make_gold_backend_spec(
            label="sql-backend",
            public_name="SQL Backend",
            marker="mysql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="mysql"),
            execution_engine=ExecutionEngineKind.SQL,
        )
        pandas_spec = _make_gold_backend_spec(
            label="pandas-backend",
            public_name="Pandas Backend",
            marker="filesystem",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="filesystem"),
            execution_engine=ExecutionEngineKind.PANDAS,
        )
        with isolated_registry():
            register_sql_config(_make_config_class("SqlBackend", sql_spec))
            register_sql_config(_make_config_class("PandasBackend", pandas_spec))

            case = _make_throwaway_case(case_key, engines=frozenset({ExecutionEngineKind.SQL}))
            params = build_gold_case_params([case], measurement_mode=False)

        assert len(params) == 1
        [param] = params
        assert _param_test_config(param).data_source_config.label == "sql-backend"
        # Dropping the pandas backend must not be recorded on it as a `tier_case_exclusions`
        # entry -- nothing in this test declared one, so the registry's own record of that
        # backend must still show none.
        assert pandas_spec.tier_case_exclusions == {}

    def test_data_source_with_no_recorded_engine_is_dropped(self) -> None:
        # A positive control (an engine-bearing, config-bound backend) sits alongside the
        # engineless one so this test is a set difference, not an emptiness check: `params == []`
        # alone cannot distinguish "the engineless backend was dropped for its missing engine"
        # from "everything was dropped for any reason at all."
        case_key = "no_engine_case"
        engineless_spec = _make_gold_backend_spec(
            label="engineless-backend",
            public_name="Engineless Backend",
            marker="trino",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="trino"),
            execution_engine=None,
        )
        engine_bearing_spec = _make_gold_backend_spec(
            label="engine-bearing-backend",
            public_name="Engine Bearing Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
            execution_engine=ExecutionEngineKind.SQL,
        )
        with isolated_registry():
            register_sql_config(_make_config_class("EngineLessBackend", engineless_spec))
            register_sql_config(_make_config_class("EngineBearingBackend", engine_bearing_spec))

            case = _make_throwaway_case(case_key)
            params = build_gold_case_params([case], measurement_mode=False)

        labels = {_param_test_config(param).data_source_config.label for param in params}
        assert labels == {"engine-bearing-backend"}

    def test_measurement_mode_emits_one_param_per_registered_candidate_with_config_and_engine(
        self,
    ) -> None:
        # A positive control (an engine-bearing, config-bound backend) and a negative control (an
        # engineless one) are registered together, and the expected labels are stated literally
        # rather than by re-running the production predicate over `iter_data_source_configs()` --
        # doing that would move both sides of the assertion together and let a broken filter
        # survive undetected.
        case_key = "measurement_mode_case"
        engine_bearing_spec = _make_gold_backend_spec(
            label="measurement-engine-bearing-backend",
            public_name="Measurement Engine Bearing Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
            execution_engine=ExecutionEngineKind.SQL,
        )
        engineless_spec = _make_gold_backend_spec(
            label="measurement-engineless-backend",
            public_name="Measurement Engineless Backend",
            marker="trino",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="trino"),
            execution_engine=None,
        )
        with isolated_registry():
            register_sql_config(
                _make_config_class("MeasurementEngineBearingBackend", engine_bearing_spec)
            )
            register_sql_config(_make_config_class("MeasurementEnginelessBackend", engineless_spec))

            case = _make_throwaway_case(case_key)
            params = build_gold_case_params([case], measurement_mode=True)

            labels = {_param_test_config(param).data_source_config.label for param in params}
            ids = {param.id for param in params}

        assert labels == {"measurement-engine-bearing-backend"}
        assert ids == {f"measurement-engine-bearing-backend-{case_key}"}
        for param in params:
            assert pytest.mark.gold in param.marks

    def test_measurement_mode_ignores_a_declared_exclusion(self) -> None:
        # A candidate in measurement mode has declared no membership at all, so it has nothing
        # for `tier_case_exclusions` to take effect against -- unlike normal mode, an exclusion
        # declared for this case must not remove the backend from measurement mode's result.
        case_key = "measurement_ignores_exclusion_case"
        excluded_spec = _make_gold_backend_spec(
            label="measurement-excluded-backend",
            public_name="Measurement Excluded Backend",
            marker="oracle",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="oracle"),
            tier_case_exclusions={SupportTier.GOLD: {case_key: "throwaway exclusion for a test"}},
        )
        with isolated_registry():
            register_sql_config(_make_config_class("MeasurementExcludedBackend", excluded_spec))

            case = _make_throwaway_case(case_key)
            params = build_gold_case_params([case], measurement_mode=True)

            labels = {_param_test_config(param).data_source_config.label for param in params}

        assert "measurement-excluded-backend" in labels

    def test_a_case_declaring_empty_engines_shape_is_rejected_at_construction(self) -> None:
        with pytest.raises(ValueError, match="empty engine set"):
            _make_throwaway_case("empty_engines_case", engines=frozenset())

    @pytest.mark.parametrize("case_key", [_TYPE_LIST_CASE_KEY, _TYPE_CASE_KEY])
    def test_default_spec_resolves_type_name_case_byte_identical_to_the_case_table(
        self, case_key: str
    ) -> None:
        # A backend spec that declares nothing for the two type-name fields must produce the
        # exact configurations the case table already carries for these two cases -- the
        # rebuild in `_resolve_case_for_config` is required to be a no-op for a default spec,
        # not merely equivalent in effect.
        [table_case] = [case for case in GOLD_CASES if case.key == case_key]
        default_spec = _make_gold_backend_spec()
        config_class = _make_config_class(f"DefaultSpecBackend-{case_key}", default_spec)

        resolved = _resolve_case_for_config(
            table_case, cast("DataSourceTestConfig", config_class())
        )

        assert resolved.passing == table_case.passing
        assert resolved.failing == table_case.failing

    def test_declared_type_names_flow_into_the_in_type_list_case(self) -> None:
        [table_case] = [case for case in GOLD_CASES if case.key == _TYPE_LIST_CASE_KEY]
        declaring_spec = _make_gold_backend_spec(
            integer_column_type_name="NUMBER",
            non_integer_column_type_name="STRING",
        )
        config_class = _make_config_class("DeclaringSpecBackend-typelist", declaring_spec)

        resolved = _resolve_case_for_config(
            table_case, cast("DataSourceTestConfig", config_class())
        )

        assert isinstance(resolved.passing, gxe.ExpectColumnValuesToBeInTypeList)
        assert resolved.passing.type_list == ["NUMBER", "BIGINT", "SMALLINT"]
        assert isinstance(resolved.failing, gxe.ExpectColumnValuesToBeInTypeList)
        assert resolved.failing.type_list == ["STRING"]
        # The declaration must actually have moved the result -- proven by disagreeing with the
        # table's own hardcoded literals, not merely by matching some value.
        assert resolved.passing != table_case.passing
        assert resolved.failing != table_case.failing

    def test_declared_type_names_flow_into_the_of_type_case(self) -> None:
        [table_case] = [case for case in GOLD_CASES if case.key == _TYPE_CASE_KEY]
        declaring_spec = _make_gold_backend_spec(
            integer_column_type_name="NUMBER",
            non_integer_column_type_name="STRING",
        )
        config_class = _make_config_class("DeclaringSpecBackend-oftype", declaring_spec)

        resolved = _resolve_case_for_config(
            table_case, cast("DataSourceTestConfig", config_class())
        )

        assert isinstance(resolved.passing, gxe.ExpectColumnValuesToBeOfType)
        assert resolved.passing.type_ == "NUMBER"
        assert isinstance(resolved.failing, gxe.ExpectColumnValuesToBeOfType)
        assert resolved.failing.type_ == "STRING"
        assert resolved.passing != table_case.passing
        assert resolved.failing != table_case.failing

    def test_a_case_outside_the_type_name_pair_is_returned_unchanged(self) -> None:
        case = _make_throwaway_case("unrelated_case")
        declaring_spec = _make_gold_backend_spec(
            integer_column_type_name="NUMBER",
            non_integer_column_type_name="STRING",
        )
        config_class = _make_config_class("DeclaringSpecBackend-unrelated", declaring_spec)

        resolved = _resolve_case_for_config(case, cast("DataSourceTestConfig", config_class()))

        assert resolved is case

    def test_a_real_backends_declared_type_name_reaches_the_resolved_case(self) -> None:
        # Unlike the tests above, which use throwaway specs, this drives `_resolve_case_for_config`
        # against Databricks' own registered config -- proving the declaration on that module
        # actually reaches the resolved case, not just that the resolver honors *a* declaration.
        from tests.integration.test_utils.data_source_config.databricks import (
            DatabricksDatasourceTestConfig,
        )

        [table_case] = [case for case in GOLD_CASES if case.key == _TYPE_CASE_KEY]

        resolved = _resolve_case_for_config(
            table_case, cast("DataSourceTestConfig", DatabricksDatasourceTestConfig())
        )

        assert isinstance(resolved.passing, gxe.ExpectColumnValuesToBeOfType)
        assert resolved.passing.type_ == "INT"

    def test_build_gold_case_params_carries_the_resolved_case_through(self) -> None:
        # The end-to-end path: a case built through `build_gold_case_params` for a data source
        # declaring non-default type names must carry the resolved configurations, not the
        # case table's originals, in both halves of its param.
        [table_case] = [case for case in GOLD_CASES if case.key == _TYPE_CASE_KEY]
        declaring_spec = _make_gold_backend_spec(
            label="declaring-of-type-backend",
            public_name="Declaring Of Type Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
            integer_column_type_name="NUMBER",
            non_integer_column_type_name="STRING",
        )
        with isolated_registry():
            register_sql_config(_make_config_class("DeclaringOfTypeBackend", declaring_spec))

            params = build_gold_case_params([table_case], measurement_mode=True)

        assert len(params) == 1
        [param] = params
        resolved_case = _param_gold_case(param)
        resolved_test_config = _param_test_config(param)
        assert isinstance(resolved_case.passing, gxe.ExpectColumnValuesToBeOfType)
        assert resolved_case.passing.type_ == "NUMBER"
        assert resolved_test_config.data.equals(GOLD_FIXTURE_DATA)
        # The param carries the declaring backend's own config, which is what makes the type
        # name above attributable to that backend's declaration rather than to the default.
        assert resolved_test_config.data_source_config.label == "declaring-of-type-backend"


# --------------------------------------------------------------------------------------------
# Coverage for `pytest_generate_tests` itself
# --------------------------------------------------------------------------------------------
#
# `pytest_generate_tests` is a collection-time hook: pytest calls it with a real `Metafunc`, which
# nothing here constructs. A stub carrying exactly the four members the hook reads --
# `function.__name__`, `fixturenames`, `config.getoption`, and a recording `parametrize` -- is
# enough to drive it directly and pin all four behaviors the hook itself is responsible for: the
# `_SHAPE_BY_TEST_FUNCTION_NAME` name dispatch, both `fixturenames` gates, the
# `--gold-measurement` option read reaching `build_gold_case_params`, and `indirect=` being passed
# as intended. `build_gold_case_params` itself is not re-verified here -- that is
# `TestBuildGoldCaseParams`'s job -- so these cases use throwaway registrations only large enough
# to tell the hook's own wiring apart from a no-op.


class _RecordedParametrizeCall:
    def __init__(
        self,
        argnames: object,
        argvalues: object,
        *,
        indirect: object,
    ) -> None:
        self.argnames = argnames
        self.argvalues = list(cast("List[ParameterSet]", argvalues))
        self.indirect = indirect


class _StubMetafunc:
    """A minimal stand-in for `pytest.Metafunc`, carrying only what `pytest_generate_tests` reads
    and a recording `parametrize` in place of pytest's collection-time one."""

    def __init__(
        self,
        *,
        function_name: str,
        fixturenames: Sequence[str],
        measurement_mode: bool,
    ) -> None:
        self.function = type("StubFunction", (), {"__name__": function_name})()
        self.fixturenames = list(fixturenames)
        self.config = type(
            "StubConfig",
            (),
            {
                "getoption": lambda self, name: measurement_mode
                if name == GOLD_MEASUREMENT_OPTION
                else None
            },  # FIXME CoP
        )()
        self.calls: List[_RecordedParametrizeCall] = []

    def parametrize(
        self,
        argnames: object,
        argvalues: object,
        *,
        indirect: object = False,
    ) -> None:
        self.calls.append(_RecordedParametrizeCall(argnames, argvalues, indirect=indirect))


class TestPytestGenerateTests:
    """Direct coverage of the `pytest_generate_tests` hook, via `_StubMetafunc`."""

    pytestmark = pytest.mark.project

    def test_no_case_of_this_shape_is_collected_as_one_marked_skipped_placeholder(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # GOLD_CASES holds no case of this shape, so `cases` is empty before the registry is even
        # consulted -- this is the "no case of this shape" branch, and must report as such even
        # though several real data sources hold gold-tier membership in the live registry this
        # test does not isolate.
        monkeypatch.setattr(sys.modules[__name__], "GOLD_CASES", ())
        metafunc = _StubMetafunc(
            function_name="test_standard_case",
            fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
            measurement_mode=False,
        )
        pytest_generate_tests(cast("pytest.Metafunc", metafunc))

        [call] = metafunc.calls
        # A real, non-empty parametrize call -- not the empty list `build_gold_case_params` itself
        # would legally return -- is what makes marker-coverage see a required marker on this
        # otherwise-collected-but-empty test (see `_empty_product_placeholder`'s docstring).
        assert len(call.argvalues) == 1
        [param] = call.argvalues
        assert param.values == (None, None)
        assert {mark.name for mark in param.marks} == {"project", "skip"}
        [skip_mark] = [mark for mark in param.marks if mark.name == "skip"]
        assert skip_mark.kwargs["reason"] == _NO_CASE_OF_SHAPE_REASON

    def test_no_data_source_claiming_the_case_is_collected_with_the_other_reason(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A published case of this shape exists (`cases` is non-empty), but an isolated, empty
        # registry offers it no candidate at all -- this is the second way an empty product
        # arises, and it must report a *different* reason than "no case of this shape", even
        # though both leave `build_gold_case_params` returning the same empty list.
        case = _make_throwaway_case("no_claimant_case")
        monkeypatch.setattr(sys.modules[__name__], "GOLD_CASES", (case,))
        metafunc = _StubMetafunc(
            function_name="test_standard_case",
            fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
            measurement_mode=False,
        )
        with isolated_registry():
            pytest_generate_tests(cast("pytest.Metafunc", metafunc))

        [call] = metafunc.calls
        assert len(call.argvalues) == 1
        [param] = call.argvalues
        assert param.values == (None, None)
        [skip_mark] = [mark for mark in param.marks if mark.name == "skip"]
        assert skip_mark.kwargs["reason"] == _NO_DATA_SOURCE_CLAIMED_REASON
        assert skip_mark.kwargs["reason"] != _NO_CASE_OF_SHAPE_REASON

    def test_unrecognized_function_name_is_left_untouched(self) -> None:
        metafunc = _StubMetafunc(
            function_name="test_not_a_gold_shape_function",
            fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
            measurement_mode=False,
        )
        pytest_generate_tests(cast("pytest.Metafunc", metafunc))
        assert metafunc.calls == []

    def test_recognized_name_missing_batch_setup_fixture_is_left_untouched(self) -> None:
        metafunc = _StubMetafunc(
            function_name="test_standard_case",
            fixturenames=[_GOLD_CASE_FIXTURE_NAME],
            measurement_mode=False,
        )
        pytest_generate_tests(cast("pytest.Metafunc", metafunc))
        assert metafunc.calls == []

    def test_recognized_name_missing_gold_case_fixture_is_left_untouched(self) -> None:
        metafunc = _StubMetafunc(
            function_name="test_standard_case",
            fixturenames=[_BATCH_SETUP_FIXTURE_NAME],
            measurement_mode=False,
        )
        pytest_generate_tests(cast("pytest.Metafunc", metafunc))
        assert metafunc.calls == []

    def test_recognized_name_with_both_fixtures_dispatches_by_shape_and_reads_the_option(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Two throwaway cases of different shapes: only the STANDARD one may reach the builder for
        # `test_standard_case`, which pins the `_SHAPE_BY_TEST_FUNCTION_NAME` dispatch itself, not
        # just "some case reached the builder."
        standard_case = _make_throwaway_case("generate_tests_standard_case")
        extra_table_case = GoldCase(
            key="generate_tests_extra_table_case",
            passing=standard_case.passing,
            failing=standard_case.failing,
            fixture_shape=CaseFixtureShape.EXTRA_TABLE,
        )
        # Patched on the live module object (via `sys.modules[__name__]`), not by dotted string
        # path: this directory has no `__init__.py`, so pytest collects this file under its bare
        # module name while the dotted path resolves (via PEP 420 namespace packages) to a
        # *second*, independently-imported module object -- patching that one leaves the globals
        # `pytest_generate_tests` actually reads untouched.
        monkeypatch.setattr(sys.modules[__name__], "GOLD_CASES", (standard_case, extra_table_case))

        spec = _make_gold_backend_spec(
            label="generate-tests-backend",
            public_name="Generate Tests Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
        )
        with isolated_registry():
            register_sql_config(_make_config_class("GenerateTestsBackend", spec))

            metafunc = _StubMetafunc(
                function_name="test_standard_case",
                fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
                measurement_mode=False,
            )
            pytest_generate_tests(cast("pytest.Metafunc", metafunc))

            expected_params = build_gold_case_params([standard_case], measurement_mode=False)

        assert len(metafunc.calls) == 1
        [call] = metafunc.calls
        assert call.argnames == [_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME]
        assert call.indirect == [_BATCH_SETUP_FIXTURE_NAME]
        assert {param.id for param in call.argvalues} == {param.id for param in expected_params}
        # The EXTRA_TABLE case must not have contributed a param to a STANDARD-shape function.
        assert all(_param_gold_case(param).key != extra_table_case.key for param in call.argvalues)

    def test_measurement_option_reaches_the_builder(self, monkeypatch: pytest.MonkeyPatch) -> None:
        case = _make_throwaway_case("generate_tests_measurement_case")
        monkeypatch.setattr(sys.modules[__name__], "GOLD_CASES", (case,))

        excluded_spec = _make_gold_backend_spec(
            label="generate-tests-measurement-backend",
            public_name="Generate Tests Measurement Backend",
            marker="postgresql",
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="postgresql"),
            tier_case_exclusions={SupportTier.GOLD: {case.key: "throwaway exclusion for a test"}},
        )
        with isolated_registry():
            register_sql_config(
                _make_config_class("GenerateTestsMeasurementBackend", excluded_spec)
            )

            normal_metafunc = _StubMetafunc(
                function_name="test_standard_case",
                fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
                measurement_mode=False,
            )
            pytest_generate_tests(cast("pytest.Metafunc", normal_metafunc))

            measurement_metafunc = _StubMetafunc(
                function_name="test_standard_case",
                fixturenames=[_BATCH_SETUP_FIXTURE_NAME, _GOLD_CASE_FIXTURE_NAME],
                measurement_mode=True,
            )
            pytest_generate_tests(cast("pytest.Metafunc", measurement_metafunc))

        # Normal mode resolves through the tier-case accessor, so a declared exclusion for this
        # backend/case pair drops it; measurement mode ignores that exclusion entirely. The two
        # calls' argvalues differing is the evidence that `metafunc.config.getoption` actually
        # reached `build_gold_case_params` rather than the option being read and then ignored.
        [normal_call] = normal_metafunc.calls
        [measurement_call] = measurement_metafunc.calls
        # An empty product is substituted with the single marked placeholder param -- see
        # `_empty_product_placeholder` -- rather than an empty list, so this asserts that
        # substitution rather than emptiness.
        assert len(normal_call.argvalues) == 1
        [normal_param] = normal_call.argvalues
        assert normal_param.values == (None, None)
        assert {mark.name for mark in normal_param.marks} == {"project", "skip"}
        assert len(measurement_call.argvalues) == 1
        [param] = measurement_call.argvalues
        assert (
            _param_test_config(param).data_source_config.label
            == "generate-tests-measurement-backend"
        )


class TestDescribeObservedValue:
    """`_describe_observed_value` is what makes a case-outcome assertion diagnosable on its own
    terms: the value that made the case fail is named as its own field, rather than left for a
    reader to find inside a full result dump.
    """

    pytestmark = pytest.mark.project

    def test_reports_a_scalar_observed_value(self) -> None:
        result = cast(
            "_ExpectationValidationResult",
            SimpleNamespace(result={"observed_value": "VARCHAR"}),
        )
        assert _describe_observed_value(result) == "observed_value='VARCHAR'"

    def test_reports_a_list_observed_value(self) -> None:
        result = cast(
            "_ExpectationValidationResult",
            SimpleNamespace(result={"observed_value": [1, 2, 3]}),
        )
        assert _describe_observed_value(result) == "observed_value=[1, 2, 3]"

    def test_states_absence_explicitly_when_the_expectation_reports_no_observed_value(
        self,
    ) -> None:
        result = cast(
            "_ExpectationValidationResult",
            SimpleNamespace(result={"unexpected_count": 0}),
        )
        assert (
            _describe_observed_value(result) == "observed_value=<not reported by this expectation>"
        )

    def test_states_absence_explicitly_when_result_itself_is_not_a_mapping(self) -> None:
        result = cast("_ExpectationValidationResult", SimpleNamespace(result=None))
        assert (
            _describe_observed_value(result) == "observed_value=<not reported by this expectation>"
        )
