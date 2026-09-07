"""Single definition of the standard and curated data-source lists.

Before this module existed, two conftest modules each hand-maintained their own copy of these
lists, and the copies had already drifted: one defined a combined list the other did not. A
data source added to one copy and forgotten in the other would silently under-test without any
signal, since nothing checked the two against each other.

Every list here is now derived from data-source declarations rather than hand-maintained: a data
source states what it is and what it claims once, on its own declaration (see `data_source_spec.py`
and its SQL sub-record in `backend_spec.py`), and this module reads that declaration through the
registry rather than naming data sources itself. That leaves only one place membership is stated —
the declaration — instead of a second, hand-maintained copy these lists could drift from.

The lists are derived on two different keys, and the difference is not an inconsistency: it is the
difference between the two kinds of thing being asked.

- A **tier** is a claim about coverage. `SupportTier.CANONICAL_EXPECTATIONS` and
  `SupportTier.CURATED_SQL` say "a suite runs against this data source and proves this much",
  which is something a
  maintainer decides and a data source can join or leave without anything about the data source
  itself changing. `ALL_DATA_SOURCES` and the two curated-and-standard lists key on that claim,
  because they exist to answer "what does this tier's suite run against". The shared canonical
  expectation parameterization is not a SQL suite — it runs pandas, Spark and SQL data sources
  alike — so the three non-SQL configs declare its criterion too, and `ALL_DATA_SOURCES` is that
  one claim read directly rather than three engine-keyed lists added together.
- An **execution engine** is a fact about the data source. `ExecutionEngineKind.PANDAS` and
  `ExecutionEngineKind.SPARK` say what actually executes the tests; no maintainer decision moves a
  Spark data source onto pandas. The pandas and Spark lists key on that fact, because they exist to
  answer "which data sources does this engine run", a question a coverage claim cannot answer.

Keying either list on the other's question would state something untrue: a tier claim would make
an engine look optional, and an engine would make a coverage claim look like a property of the
data source. `SQL_DATA_SOURCES` is the one list that needs both keys and says so — the criterion,
intersected with the SQL engine — because "the SQL data sources this suite runs against" really is
a coverage claim and an engine fact together.

Stating membership in one place is not the same as this module seeing it, though: the lists below
are built once, when this module is first imported, from whatever the registry holds at that
moment, so a data source module imported after this one has not yet run its registration and is
silently absent from every list even though it is declared and registered. This package's
`__init__.py` imports every data source module before this one for exactly that reason; the
regression test in `tests/test_data_source_registry.py` that guards the ordering is what turns a
violation of it into a failing test instead of a silent gap.

This module reads the registry and imports nothing else from this package at runtime. It sits to
the right of every other module in this package's dependency direction (see `sql_config.py`'s
module docstring), so nothing else in this package may import it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, List, cast

from tests.integration.test_utils.data_source_config.data_source_spec import (
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import (
    data_source_configs_for_engine,
    data_source_configs_for_tier,
)

if TYPE_CHECKING:
    from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig

# Both engine-keyed lists below, and the tier-keyed lists after them, take the same `cast`:
# the registry accessors are typed against its minimal registration protocol, not against
# `DataSourceTestConfig`, so that `registry.py` need not import the config base (see this
# package's dependency direction, stated in `sql_config.py`). Every class the registry hands back
# is a `DataSourceTestConfig`, because only a config class can be enrolled with one; the cast
# states that fact at the places it matters instead of widening the registry's return type.
#
# `data_source_configs_for_engine` walks config-bound entries only, so the records registered
# without a config class — the data sources this repository declares but does not exercise — are
# not reachable from these lists at all, whatever engine they might name.

PANDAS_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    cast("DataSourceTestConfig", config_class())
    for config_class in data_source_configs_for_engine(ExecutionEngineKind.PANDAS)
]
"""Every registered config declaring `ExecutionEngineKind.PANDAS`, instantiated with no arguments,
in label order (the order `data_source_configs_for_engine` itself returns)."""

SPARK_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    cast("DataSourceTestConfig", config_class())
    for config_class in data_source_configs_for_engine(ExecutionEngineKind.SPARK)
]
"""Every registered config declaring `ExecutionEngineKind.SPARK`, instantiated with no arguments,
in label order."""

_SHARED_PARAMETERIZATION_CONFIGS: Final = data_source_configs_for_tier(
    SupportTier.CANONICAL_EXPECTATIONS
)
"""Every registered config declaring the shared-parameterization criterion, in label order.

Read once here and used twice below, so that the combined list and its SQL half are two views of
one declaration rather than two independent derivations that could part.

`data_source_configs_for_tier` walks every config-bound registration, whatever engine it names, so
a pandas or Spark config declaring this criterion is returned by it exactly as a SQL backend is.
"""

CURATED_SQL_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    cast("DataSourceTestConfig", config_class())
    for config_class in data_source_configs_for_tier(SupportTier.CURATED_SQL)
]
"""Every registered config declaring `SupportTier.CURATED_SQL` membership, instantiated with no
arguments, in label order. Four SQL backends declare that tier today."""

ALL_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    cast("DataSourceTestConfig", config_class())
    for config_class in _SHARED_PARAMETERIZATION_CONFIGS
]
"""Every registered config declaring the shared-parameterization criterion, instantiated with no
arguments, in label order.

This is one declared claim read once, not three derivations added together. It was
`PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES`, which made membership an accident
of which engine lists happened to exist: a data source joined this list by being pandas, or being
Spark, or claiming a SQL tier, and no single statement anywhere said which data sources the shared
canonical expectation parameterization runs against. Now one does — the criterion on each record —
and a data source joins or leaves this list by changing that declaration and nothing else.

The membership is the same set the concatenation produced. The order is not: a concatenation
grouped the engines, while a tier read is in label order like every other accessor here, so the
non-SQL entries sit among the SQL ones. Nothing reads this list positionally — it is a pytest
parameterization source — so the difference is visible only in the order test ids are generated
in.
"""

SQL_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    cast("DataSourceTestConfig", config_class())
    for config_class in _SHARED_PARAMETERIZATION_CONFIGS
    if config_class.DATA_SOURCE_SPEC.execution_engine is ExecutionEngineKind.SQL
]
"""The shared-parameterization criterion intersected with the SQL execution engine, instantiated
with no arguments, in label order.

A compound derivation, and deliberately so: this list has always meant "the SQL data sources the
shared parameterization runs against", and that is two facts about a record, not one. It was
expressible as a single tier read only while the criterion itself named a SQL engine and so
excluded every non-SQL data source by its name alone. Now that the criterion names the suite it
gates, the SQL restriction has to be stated where it is meant, as the engine fact it is.
"""


def data_sources_for_tier_case(tier: SupportTier, case_key: str) -> List[DataSourceTestConfig]:
    """The tier's members, minus any declaring a `tier_case_exclusions` entry for `case_key`.

    A data source joins a tier as a whole; `tier_case_exclusions` (declared on `DataSourceSpec`,
    see `data_source_spec.py`) is the one way a member can sit out a single named case within
    that tier's suite instead of the whole tier. This accessor is the only place that mapping
    takes effect — registration validates it, but nothing else filters tier membership by case —
    which keeps "does this exclusion take effect" a one-module question instead of a property
    every future consumer has to reimplement correctly. It is written to take a tier and a case
    key, not to be specialized to one tier: the exclusion mechanism belongs to tiers in general,
    and a version hard-coded to one tier would have to be undone the moment a second tier needs
    the same mechanism.

    Unlike the module-level lists above, this reads the registry fresh on every call (through
    `data_source_configs_for_tier`, itself call-time) rather than once at import — the exclusion
    mapping it filters on can only be known per call, from the tier's live membership, not baked
    into a
    list built before any caller has said which case it means.

    Returns instances, in the tier's label order, with an empty exclusion mapping on every member
    yielding exactly that tier's derived list.
    """
    return [
        cast("DataSourceTestConfig", config_class())
        for config_class in data_source_configs_for_tier(tier)
        if case_key not in config_class.DATA_SOURCE_SPEC.tier_case_exclusions
    ]
