"""The standard CRUD contract applied to every registered fluent datasource type.

This module declares, and only declares: the named cases that make up the contract, the
per-type table of arguments that exercise those cases, and the accessors a test module reads
instead of touching the table's internals directly.

**The contract.** Every registered fluent datasource type is expected to support eight
behaviors through its create, update and create-or-update factories:

- ``create`` — the returned object is an instance of the class the registry holds for the
  type, carries the requested name, and is the object the context returns for that name
  afterwards.
- ``create_rejects_duplicate_name`` — a second create with the same name raises the data
  context's duplicate-name error naming the datasource, and the stored datasource is left
  byte-identical to the one stored before the attempt.
- ``update_replaces_configuration`` — after update with a materially different set of
  arguments, the stored datasource's configuration reflects those arguments, and the
  identifier seeded at creation is preserved.
- ``update_rejects_absent_name`` — update against a name that was never created raises a
  value error naming that name, and no datasource is created as a side effect.
- ``create_or_update_creates_when_absent`` — against a fresh name it creates, and the result
  is retrievable by that name.
- ``create_or_update_replaces_when_present`` — against an existing name it replaces rather
  than raising, the stored configuration reflects the new arguments, and the seeded
  identifier is preserved.
- ``configuration_round_trips`` — against a file-backed context, discarding the context and
  re-reading the project from disk yields a datasource whose full configuration equals the
  one created.
- ``create_or_update_persists_one_entry`` — after create then create-or-update against a
  file-backed context, the persisted project holds exactly one entry for that name.

Three of the eight — ``update_replaces_configuration``, ``create_or_update_replaces_when_present``
and ``create_or_update_persists_one_entry`` — depend on an update overlay: arguments that
produce a materially different datasource of the same type. A type whose model has no field
an update could change cannot run them, and must say so through an exclusion rather than
running a weaker assertion.

Every case in the contract runs with connection testing neutralized at the concrete
registered class, for every type, including types that override connection testing
themselves. A passing case is therefore evidence about the CRUD contract, not about whether
any service is reachable — that neutralization is what makes the same contract meaningful for
a type that needs a running Spark cluster and one that needs nothing but a name.

**The parameter table.** One entry per registered fluent type, giving the keyword arguments
that create a datasource of that type from a scratch directory, an optional overlay of
arguments producing a materially different datasource of the same type, and any declared
exclusions. Creation arguments are produced by a callable rather than stored as a literal
because several types need a directory that exists first; the scratch directory is created by
the caller, not by this table, so the table stays a pure declaration.

Every value in the table is synthetic: no credential, no real hostname, no account identifier
and no bucket that exists appears anywhere in it.

**The rule for choosing an update overlay:** it must change a field that survives
serialization to a plain scalar or a path, because the persistence cases in the suite read
the overlay's effect back off disk. A field that pydantic excludes from serialization, or
that only affects runtime behavior, cannot serve as an overlay.

**The rule for declaring an exclusion:** declare one only where a case genuinely cannot be
driven — where the type's model has no field an update could change, for instance — and state
the property of the type that makes it so, not the difficulty of writing the case. An
exclusion always carries a case key drawn from ``CONTRACT_CASE_KEYS`` and a non-empty reason;
a type must never be excluded from every case, because a type excluded from the whole
contract is not covered and must not read as covered.

**Where a type accepts more than one shape of creation arguments** — Snowflake, SQL Server
and Fabric each do — this table declares exactly one shape. The remaining shapes are that
type's own module's subject; this table does not become a second home for per-type
validation coverage.

This module imports nothing beyond the standard library, and nothing from the package under
test or from any test harness. That is what keeps it importable in the fastest lane a
maintainer's change will run through.
"""

from __future__ import annotations

import pathlib
import types
from dataclasses import dataclass, field
from typing import Callable, Final, FrozenSet, Mapping, Optional

# ---------------------------------------------------------------------------
# Contract vocabulary
# ---------------------------------------------------------------------------

CREATE = "create"
CREATE_REJECTS_DUPLICATE_NAME = "create_rejects_duplicate_name"
UPDATE_REPLACES_CONFIGURATION = "update_replaces_configuration"
UPDATE_REJECTS_ABSENT_NAME = "update_rejects_absent_name"
CREATE_OR_UPDATE_CREATES_WHEN_ABSENT = "create_or_update_creates_when_absent"
CREATE_OR_UPDATE_REPLACES_WHEN_PRESENT = "create_or_update_replaces_when_present"
CONFIGURATION_ROUND_TRIPS = "configuration_round_trips"
CREATE_OR_UPDATE_PERSISTS_ONE_ENTRY = "create_or_update_persists_one_entry"

CONTRACT_CASE_KEYS: FrozenSet[str] = frozenset(
    {
        CREATE,
        CREATE_REJECTS_DUPLICATE_NAME,
        UPDATE_REPLACES_CONFIGURATION,
        UPDATE_REJECTS_ABSENT_NAME,
        CREATE_OR_UPDATE_CREATES_WHEN_ABSENT,
        CREATE_OR_UPDATE_REPLACES_WHEN_PRESENT,
        CONFIGURATION_ROUND_TRIPS,
        CREATE_OR_UPDATE_PERSISTS_ONE_ENTRY,
    }
)
"""Every case key the contract publishes.

An exclusion in the parameter table is checked against this set, and a downstream reader
uses it to name a case rather than inventing a string.
"""

OVERLAY_DEPENDENT_CASE_KEYS: FrozenSet[str] = frozenset(
    {
        UPDATE_REPLACES_CONFIGURATION,
        CREATE_OR_UPDATE_REPLACES_WHEN_PRESENT,
        CREATE_OR_UPDATE_PERSISTS_ONE_ENTRY,
    }
)
"""The subset of case keys that require an update overlay to run.

A type whose table entry declares no overlay obliges an exclusion for every key in this set.
"""


# ---------------------------------------------------------------------------
# Per-type parameter table
# ---------------------------------------------------------------------------

CreationArguments = Callable[[pathlib.Path], Mapping[str, object]]
"""A callable that, given a scratch directory that already exists, returns the keyword
arguments (excluding the datasource name) that create a datasource of one fluent type."""


@dataclass(frozen=True)
class FluentTypeContractParameters:
    """The declared shape of CRUD coverage for one registered fluent datasource type."""

    creation_arguments: CreationArguments
    """Keyword arguments that create a datasource of this type, given a scratch directory."""

    update_overlay: Optional[CreationArguments] = None
    """Arguments merged over ``creation_arguments`` to produce a materially different
    datasource of the same type.

    ``None`` declares that the type has no field an update could change, which obliges an
    exclusion for every key in ``OVERLAY_DEPENDENT_CASE_KEYS``.
    """

    case_exclusions: Mapping[str, str] = field(default_factory=dict)
    """Case key to reason. A case declared here is reported as skipped with that reason."""

    def __post_init__(self) -> None:
        # Coerce to an immutable mapping regardless of what was passed in, so every entry's
        # case_exclusions is uniformly a live-reference-proof view rather than trusting each
        # call site in CONTRACT_PARAMETERS to have wrapped its own dict.
        object.__setattr__(
            self, "case_exclusions", types.MappingProxyType(dict(self.case_exclusions))
        )


def _no_arguments(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
    return {}


def _connection_string(url: str) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {"connection_string": url}

    return _build


def _connection_string_overlay(url: str, *, create_temp_table: bool) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {"connection_string": url, "create_temp_table": create_temp_table}

    return _build


def _sqlite_connection_string(*, overlay: bool) -> CreationArguments:
    def _build(scratch_directory: pathlib.Path) -> Mapping[str, object]:
        db_file = scratch_directory / ("overlay.db" if overlay else "contract.db")
        return {
            "connection_string": f"sqlite:///{db_file}",
            "create_temp_table": overlay,
        }

    return _build


def _sql_server_flat(*, schema: str) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {
            "host": "sql-server.contract-suite.invalid",
            "database": "contract_db",
            "schema": schema,
            "username": "contract_user",
            "password": "contract_password",
        }

    return _build


def _fabric_flat(*, schema: str) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {
            "host": "contract-suite.datawarehouse.fabric.microsoft.com",
            "database": "contract_db",
            "schema": schema,
            "tenant_id": "00000000-0000-0000-0000-000000000001",
            "client_id": "00000000-0000-0000-0000-000000000002",
            "client_secret": "contract_secret",
        }

    return _build


def _base_directory(subdirectory: str) -> CreationArguments:
    def _build(scratch_directory: pathlib.Path) -> Mapping[str, object]:
        directory = scratch_directory / subdirectory
        directory.mkdir(parents=True, exist_ok=True)
        return {"base_directory": str(directory)}

    return _build


def _bucket(*, key: str, name: str) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {key: name}

    return _build


def _azure_options(account_url: str) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {"azure_options": {"account_url": account_url}}

    return _build


def _fabric_powerbi(*, dataset: str, workspace: Optional[str] = None) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        arguments: dict[str, object] = {"dataset": dataset}
        if workspace is not None:
            arguments["workspace"] = workspace
        return arguments

    return _build


def _spark_overlay(*, persist: bool) -> CreationArguments:
    def _build(_scratch_directory: pathlib.Path) -> Mapping[str, object]:
        return {"persist": persist}

    return _build


_POSTGRES_FORM_URL = (
    "postgresql://contract_user:contract_password@db.contract-suite.invalid:5432/contract_db"
)

_PANDAS_EXCLUSION_REASON = (
    "PandasDatasource declares no field beyond name, type, identifier and assets, so no "
    "update of its configuration can be observed to have replaced anything."
)

CONTRACT_PARAMETERS: Mapping[str, FluentTypeContractParameters] = {
    # Connection string, PostgreSQL-form URL, `create_temp_table` flipped.
    "alloy": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    "aurora": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    "citus": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    "neon": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    "postgres": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    "sql": FluentTypeContractParameters(
        creation_arguments=_connection_string(_POSTGRES_FORM_URL),
        update_overlay=_connection_string_overlay(_POSTGRES_FORM_URL, create_temp_table=True),
    ),
    # Connection string, dialect-specific URL form, `create_temp_table` flipped.
    "bigquery": FluentTypeContractParameters(
        creation_arguments=_connection_string("bigquery://contract-project/contract_dataset"),
        update_overlay=_connection_string_overlay(
            "bigquery://contract-project/contract_dataset", create_temp_table=True
        ),
    ),
    "databricks_sql": FluentTypeContractParameters(
        creation_arguments=_connection_string(
            "databricks://token:contract-token@databricks.contract-suite.invalid:443"
            "?http_path=/sql/1.0/warehouses/contract&catalog=contract_catalog"
            "&schema=contract_schema"
        ),
        update_overlay=_connection_string_overlay(
            "databricks://token:contract-token@databricks.contract-suite.invalid:443"
            "?http_path=/sql/1.0/warehouses/contract&catalog=contract_catalog"
            "&schema=contract_schema",
            create_temp_table=True,
        ),
    ),
    "redshift": FluentTypeContractParameters(
        creation_arguments=_connection_string(
            "redshift+psycopg2://contract_user:contract_password"
            "@redshift.contract-suite.invalid:5439/contract_db"
        ),
        update_overlay=_connection_string_overlay(
            "redshift+psycopg2://contract_user:contract_password"
            "@redshift.contract-suite.invalid:5439/contract_db",
            create_temp_table=True,
        ),
    ),
    "snowflake": FluentTypeContractParameters(
        creation_arguments=_connection_string(
            "snowflake://contract_user:contract_password@contract_account"
            "/contract_db/contract_schema?warehouse=contract_wh&role=contract_role"
        ),
        update_overlay=_connection_string_overlay(
            "snowflake://contract_user:contract_password@contract_account"
            "/contract_db/contract_schema?warehouse=contract_wh&role=contract_role",
            create_temp_table=True,
        ),
    ),
    "sqlite": FluentTypeContractParameters(
        creation_arguments=_sqlite_connection_string(overlay=False),
        update_overlay=_sqlite_connection_string(overlay=True),
    ),
    # Structured connection details, flat keyword form. `schema` changed on overlay.
    "sql_server": FluentTypeContractParameters(
        creation_arguments=_sql_server_flat(schema="dbo"),
        update_overlay=_sql_server_flat(schema="contract_schema"),
    ),
    "fabric": FluentTypeContractParameters(
        creation_arguments=_fabric_flat(schema="dbo"),
        update_overlay=_fabric_flat(schema="contract_schema"),
    ),
    # Directory. A second scratch subdirectory on overlay.
    "pandas_filesystem": FluentTypeContractParameters(
        creation_arguments=_base_directory("pandas_filesystem"),
        update_overlay=_base_directory("pandas_filesystem_overlay"),
    ),
    "spark_filesystem": FluentTypeContractParameters(
        creation_arguments=_base_directory("spark_filesystem"),
        update_overlay=_base_directory("spark_filesystem_overlay"),
    ),
    "pandas_dbfs": FluentTypeContractParameters(
        creation_arguments=_base_directory("pandas_dbfs"),
        update_overlay=_base_directory("pandas_dbfs_overlay"),
    ),
    "spark_dbfs": FluentTypeContractParameters(
        creation_arguments=_base_directory("spark_dbfs"),
        update_overlay=_base_directory("spark_dbfs_overlay"),
    ),
    # Bucket. A second placeholder name on overlay.
    "pandas_s3": FluentTypeContractParameters(
        creation_arguments=_bucket(key="bucket", name="contract-suite-bucket"),
        update_overlay=_bucket(key="bucket", name="contract-suite-bucket-overlay"),
    ),
    "spark_s3": FluentTypeContractParameters(
        creation_arguments=_bucket(key="bucket", name="contract-suite-bucket"),
        update_overlay=_bucket(key="bucket", name="contract-suite-bucket-overlay"),
    ),
    "pandas_gcs": FluentTypeContractParameters(
        creation_arguments=_bucket(key="bucket_or_name", name="contract-suite-bucket"),
        update_overlay=_bucket(key="bucket_or_name", name="contract-suite-bucket-overlay"),
    ),
    "spark_gcs": FluentTypeContractParameters(
        creation_arguments=_bucket(key="bucket_or_name", name="contract-suite-bucket"),
        update_overlay=_bucket(key="bucket_or_name", name="contract-suite-bucket-overlay"),
    ),
    # Object-store options. A second placeholder account URL on overlay.
    "pandas_abs": FluentTypeContractParameters(
        creation_arguments=_azure_options("https://contractsuiteaccount.blob.core.windows.net"),
        update_overlay=_azure_options("https://contractsuiteaccountoverlay.blob.core.windows.net"),
    ),
    "spark_abs": FluentTypeContractParameters(
        creation_arguments=_azure_options("https://contractsuiteaccount.blob.core.windows.net"),
        update_overlay=_azure_options("https://contractsuiteaccountoverlay.blob.core.windows.net"),
    ),
    # Semantic model. `workspace` set on overlay.
    "fabric_powerbi": FluentTypeContractParameters(
        creation_arguments=_fabric_powerbi(dataset="00000000-0000-0000-0000-0000000000f1"),
        update_overlay=_fabric_powerbi(
            dataset="00000000-0000-0000-0000-0000000000f1",
            workspace="00000000-0000-0000-0000-0000000000f2",
        ),
    ),
    # None. `persist` flipped on overlay.
    "spark": FluentTypeContractParameters(
        creation_arguments=_no_arguments,
        update_overlay=_spark_overlay(persist=False),
    ),
    # None. No field an update could change.
    "pandas": FluentTypeContractParameters(
        creation_arguments=_no_arguments,
        update_overlay=None,
        case_exclusions={
            UPDATE_REPLACES_CONFIGURATION: _PANDAS_EXCLUSION_REASON,
            CREATE_OR_UPDATE_REPLACES_WHEN_PRESENT: _PANDAS_EXCLUSION_REASON,
            CREATE_OR_UPDATE_PERSISTS_ONE_ENTRY: _PANDAS_EXCLUSION_REASON,
        },
    ),
}
CONTRACT_PARAMETERS = types.MappingProxyType(CONTRACT_PARAMETERS)


# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------


def contract_parameters_for(fluent_type: str) -> FluentTypeContractParameters:
    """The table entry for a fluent datasource type.

    Raises:
        KeyError: naming the type and stating that ``crud_contract.py`` must declare, for
            that type, creation arguments and either an update overlay or an exclusion for
            every key in ``OVERLAY_DEPENDENT_CASE_KEYS``.
    """
    try:
        return CONTRACT_PARAMETERS[fluent_type]
    except KeyError:
        raise KeyError(
            f"No CRUD contract parameters are declared for fluent datasource type "
            f"{fluent_type!r}. Add an entry for {fluent_type!r} to CONTRACT_PARAMETERS in "
            f"tests/datasource/fluent/crud_contract.py. The entry must declare "
            f"creation_arguments (a callable taking a scratch directory and returning the "
            f"keyword arguments that create a datasource of this type) and either "
            f"update_overlay (a callable of the same shape whose result, merged over "
            f"creation_arguments, produces a materially different datasource of the same "
            f"type) or a case_exclusions entry with a non-empty reason for every key in "
            f"OVERLAY_DEPENDENT_CASE_KEYS."
        ) from None


def exclusion_reason(fluent_type: str, case_key: str) -> Optional[str]:
    """The declared reason this type does not run this case, or ``None`` when it does."""
    return contract_parameters_for(fluent_type).case_exclusions.get(case_key)


def covered_fluent_types() -> FrozenSet[str]:
    """Every fluent datasource type the table declares an entry for."""
    return frozenset(CONTRACT_PARAMETERS.keys())


def case_exclusions_by_type() -> Mapping[str, Mapping[str, str]]:
    """Every declared exclusion, keyed by fluent type and then by case key.

    The bulk form of ``exclusion_reason``. A consumer that needs to know which cases a type
    sits out — a generated compatibility reference qualifying a row, for example — reads this
    instead of crossing ``covered_fluent_types()`` against ``CONTRACT_CASE_KEYS`` and calling
    ``exclusion_reason`` once per pair, which would rebuild this mapping a second time.

    Returns the table's own declarations, copied into an immutable mapping, and consults
    nothing else.
    """
    return types.MappingProxyType(
        {
            fluent_type: types.MappingProxyType(dict(parameters.case_exclusions))
            for fluent_type, parameters in CONTRACT_PARAMETERS.items()
        }
    )


# ---------------------------------------------------------------------------
# Published record-coverage literals
#
# These two frozensets are read by the compatibility-reference generator to know which
# registered types its page cannot mention, and which of its records cannot claim the tier
# this suite backs. They live here, in the module with no test function, because the
# generator needs a plain import surface rather than a pytest module.
# ---------------------------------------------------------------------------

FLUENT_TYPES_NAMED_BY_NO_RECORD: Final[FrozenSet[str]] = frozenset(
    {
        "spark",  # the Spark DataFrame data source; the only Spark record names the filesystem type
        "pandas_dbfs",  # Databricks File System paths; the public reference names Databricks SQL, not DBFS  # noqa: E501
        "spark_dbfs",  # same
        "fabric_powerbi",  # Power BI semantic models; the Fabric record names the SQL-family type
    }
)

# Every fluent type each of these records is reached through is covered by the table above,
# so the contract itself is not what holds them back: each declares neither a test marker nor
# a continuous-integration lane, and a record with no lane has nothing running that could earn
# the claim. The remedy is the same for all six — declare a marker and a lane, and the record
# can claim the tier and leave this set.
RECORDS_COVERED_BUT_UNABLE_TO_CLAIM: Final[FrozenSet[str]] = frozenset(
    {
        "azure-blob-storage",
        "alloydb",
        "aurora",
        "citus",
        "neon",
        "fabric",
    }
)
