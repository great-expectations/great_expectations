"""The shared data-source lists this package's tests parameterize over.

Three of the five lists that `test_canonical_expectations.py` used to hold are not registry facts.
Two of them — `ALL_DATA_SOURCES` and `SQL_DATA_SOURCES` — were, and they are gone from here: those
names now have exactly one definition each, derived from the registry in
`tests/integration/test_utils/data_source_config/tiers.py`, and every module in this package imports
them from there. Until they were deleted, each name meant two different memberships depending on
which module a reader happened to import from, and nothing compared the two.

The three that remain live here rather than in the harness package because a list expressing which
backends one family of tests runs against is a property of that family, not of the registry. They
live in a non-test module because a shared import surface is not a test suite; hosting one inside a
test module is what let the collision above survive for as long as it did.

Membership is pinned in `tests/test_data_source_registry.py` against values captured from
`test_canonical_expectations.py` before any of this moved, not against the code below.
"""

from __future__ import annotations

from typing import Final, List

from tests.integration.test_utils.data_source_config import (
    PANDAS_DATA_SOURCES,
    SPARK_DATA_SOURCES,
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

NON_SQL_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    *PANDAS_DATA_SOURCES,
    *SPARK_DATA_SOURCES,
]
"""Every registered config whose execution engine is not SQL, grouped by engine: pandas then Spark.

Derived, because the execution engine reproduces the hand-written membership exactly: the two
pandas configs and the one Spark config are precisely the three this list has always held. It is
kept here rather than in `tiers.py` because "the non-SQL data sources these tests run against" is a
statement about this package's tests; the registry states the engine, and this list reads it.
"""

DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS: Final[List[DataSourceTestConfig]] = [
    BigQueryDatasourceTestConfig(),
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PandasDataFrameDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SparkFilesystemCsvDatasourceTestConfig(),
]
"""The data sources whose backends compare date values the way these tests expect.

**Declared, not derived, and it cannot be derived today**: no field on a data source's record
expresses whether its backend supports date comparison. Every candidate key gets the membership
wrong — it is not an engine (it holds pandas and Spark and most of SQL), not a tier (it excludes
`sqlite` and `pandas-filesystem-csv`, both of which claim the shared-parameterization criterion),
and not the escape hatch's absence (it includes it). Adding a capability field to `DataSourceSpec`
is what would make derivation possible, and that is a change to a shared contract this list alone
does not justify making.

`GenericSQLDatasourceTestConfig` stays here. It left `ALL_DATA_SOURCES` and `SQL_DATA_SOURCES`
because those are the derived sets that gate CI and it must never appear in them; this list is
neither derived nor CI-gating, so the same reasoning does not reach it.
"""

JUST_PANDAS_DATA_SOURCES: Final[List[DataSourceTestConfig]] = [
    PandasDataFrameDatasourceTestConfig(),
]
"""The single pandas DataFrame config, for tests that assert behavior only pandas exhibits.

**Declared, not derived.** A one-entry list is a choice of one data source, not a derivation over a
set: `PANDAS_DATA_SOURCES` holds two entries, so the pandas engine does not reproduce this
membership, and no narrower key exists that would. Derivation becomes possible only if a record
ever states why this config in particular is the one an in-memory-only test wants — which is a
property of the test, not of the data source.
"""
