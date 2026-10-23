"""The single read surface for every declaration the compatibility reference takes from elsewhere.

The data-source records, the fluent suite's per-type case exclusions and its two published
coverage literals are owned by other modules. This one imports them, resolves them, and returns
them (``SupportTier``, the enumeration of declarations a record can make, is re-exported for the
same reason); it decides nothing about what a declaration means, so a reader auditing an upstream
contract reads this file and no other.

Two things are declared here because no upstream field carries them, and each is checked against
the declarations it describes so it cannot silently go stale:

* ``CONNECTION_PATH_DESCRIPTIONS`` -- a user-facing phrase for every registered fluent datasource
  type. A harness label is internal, a public name is shared by every variant of one data source
  by definition, and a fluent type literal is an internal identifier, so none of them can name a
  variant to a reader. Loading fails if a record declares a type the map does not describe, or if
  the pinned list of uncovered paths names one it does not describe, rather than printing an
  identifier.
* ``CASE_DESCRIPTIONS`` -- a user-facing phrase for every case a declaration excludes from a
  suite. A case key is a harness identifier, so it is never printed. Loading fails if a
  declared exclusion, from either source, names a case the map does not describe, and equally if
  the map describes a case no declaration excludes, so the map follows the exclusions in both
  directions.
* ``TESTED_VERSION_NOTES`` -- the version of a data source a continuous-integration lane attests
  to. No record field carries that. Loading fails if a key names no published row, or names a data
  source whose records do not all declare a lane: a version claim with no lane behind it is a
  hand-maintained assertion nothing verifies.

Records are read through the registry's accessor at call time. The registry package also exposes
lists built once when their module is first imported; those omit any record registered afterwards
and are deliberately not used here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AbstractSet, Final, FrozenSet, Iterable, Mapping, Tuple

from tests.datasource.fluent.crud_contract import (
    FLUENT_TYPES_NAMED_BY_NO_RECORD,
    RECORDS_COVERED_BUT_UNABLE_TO_CLAIM,
    case_exclusions_by_type,
)
from tests.integration.test_utils.data_source_config import (
    DataSourceProvisioning as DataSourceProvisioning,  # noqa: PLC0414 # the alias marks a deliberate re-export (F401)
)
from tests.integration.test_utils.data_source_config import (
    DataSourceSpec as DataSourceSpec,  # noqa: PLC0414 # the alias marks a deliberate re-export (F401)
)
from tests.integration.test_utils.data_source_config import (
    SupportTier as SupportTier,  # noqa: PLC0414 # the alias marks a deliberate re-export (F401)
)
from tests.integration.test_utils.data_source_config import (
    iter_data_source_specs,
)


class UpstreamDeclarationError(Exception):
    """An upstream declaration and a declaration made here disagree.

    Raised during loading, so generation stops before it can print anything built on the
    disagreement.
    """


@dataclass(frozen=True)
class UpstreamFacts:
    specs: Tuple[DataSourceSpec, ...]
    """Every registered record, read through the registry accessor at call time."""

    covered_but_unable_to_claim: FrozenSet[str]
    """Record labels the fluent suite covers that cannot declare its tier."""

    fluent_types_named_by_no_record: FrozenSet[str]
    """Registered fluent type literals no record names."""

    fluent_case_exclusions: Mapping[str, Mapping[str, str]]
    """Fluent type to case key to reason.

    Read from the bulk accessor the fluent suite publishes for this purpose, not rebuilt here by
    crossing the covered types with the case-key vocabulary, which would be a second copy of it.
    """


CONNECTION_PATH_DESCRIPTIONS: Final[Mapping[str, str]] = {
    # Paths no record names: printed in the footnote below the table.
    "spark": "Spark in-memory DataFrames",
    "pandas_dbfs": "Databricks File System paths read with pandas",
    "spark_dbfs": "Databricks File System paths read with Spark",
    "fabric_powerbi": "Power BI semantic models",
    # Every other registered type, so a row can name one of its variants. A type shared by
    # several single-record rows (``sql``) is described generically: the description must be
    # true of each of them.
    "pandas": "in-memory pandas DataFrames",
    "pandas_filesystem": "CSV and other files read with pandas",
    "pandas_s3": "Amazon S3 objects read with pandas",
    "pandas_gcs": "Google Cloud Storage objects read with pandas",
    "pandas_abs": "Azure Blob Storage objects read with pandas",
    "spark_filesystem": "files read with Spark",
    "spark_s3": "Amazon S3 objects read with Spark",
    "spark_gcs": "Google Cloud Storage objects read with Spark",
    "spark_abs": "Azure Blob Storage objects read with Spark",
    "sql": "SQL databases reached through a connection string",
    "sqlite": "SQLite databases",
    "postgres": "PostgreSQL databases",
    "alloy": "AlloyDB databases",
    "aurora": "Amazon Aurora PostgreSQL databases",
    "citus": "Citus databases",
    "neon": "Neon databases",
    "redshift": "Amazon Redshift databases",
    "snowflake": "Snowflake databases",
    "bigquery": "Google BigQuery datasets",
    "databricks_sql": "Databricks SQL warehouses",
    "sql_server": "Microsoft SQL Server databases",
    "fabric": "Microsoft Fabric SQL endpoints",
}


CASE_DESCRIPTIONS: Final[Mapping[str, str]] = {
    # Record-level exclusions from the curated SQL suite. This one case covers four assertions
    # that share the quoted-identifier code path: spaced, reserved-word and mixed-case column
    # names, and uniqueness over a quoted column.
    "quoted_identifiers": "column names that need quoting",
    # Exclusions the fluent suite records for a type whose model has no field an update could
    # change, so no update can be observed to replace anything.
    "update_replaces_configuration": "replacing a datasource's configuration on update",
    "create_or_update_replaces_when_present": (
        "replacing an existing datasource on create-or-update"
    ),
    "create_or_update_persists_one_entry": "keeping a single saved entry after create-or-update",
}


TESTED_VERSION_NOTES: Final[Mapping[str, str]] = {
    "Oracle": "Tested against Oracle 21c. 19c expected, not verified in CI.",
}


def check_connection_path_descriptions(
    specs: Iterable[DataSourceSpec],
    uncovered_paths: AbstractSet[str],
    descriptions: Mapping[str, str],
) -> None:
    """Fail unless every fluent type a record declares, and every uncovered path, is described.

    Raises:
        UpstreamDeclarationError: naming each type or path with no description.
    """
    declared = {fluent_type for spec in specs for fluent_type in spec.fluent_types}
    missing_for_records = sorted(declared - descriptions.keys())
    if missing_for_records:
        raise UpstreamDeclarationError(
            f"Record(s) declare fluent datasource type(s) {missing_for_records} that "
            f"CONNECTION_PATH_DESCRIPTIONS does not describe. Add a user-facing description for "
            f"each in tests/compatibility_reference/upstream_declarations.py; an internal "
            f"identifier must not be printed in its place."
        )
    missing_for_paths = sorted(set(uncovered_paths) - descriptions.keys())
    if missing_for_paths:
        raise UpstreamDeclarationError(
            f"The pinned list of connection paths no record names includes {missing_for_paths}, "
            f"which CONNECTION_PATH_DESCRIPTIONS does not describe. Add a user-facing "
            f"description for each in tests/compatibility_reference/upstream_declarations.py."
        )


def check_case_descriptions(
    specs: Iterable[DataSourceSpec],
    fluent_case_exclusions: Mapping[str, Mapping[str, str]],
    descriptions: Mapping[str, str],
) -> None:
    """Fail unless the described cases are exactly the cases some declaration excludes.

    Exclusions come from two sources: each record's per-tier exclusions and the fluent suite's
    per-type exclusions. A case excluded by either needs a description, and a description no
    exclusion needs is stale.

    Raises:
        UpstreamDeclarationError: naming each case with no description, or each description for
            a case nothing excludes.
    """
    excluded = {
        case for spec in specs for cases in spec.tier_case_exclusions.values() for case in cases
    } | {case for cases in fluent_case_exclusions.values() for case in cases}
    undescribed = sorted(excluded - descriptions.keys())
    if undescribed:
        raise UpstreamDeclarationError(
            f"A declaration excludes case(s) {undescribed} from a suite, but CASE_DESCRIPTIONS "
            f"does not describe them. Add a user-facing description for each in "
            f"tests/compatibility_reference/upstream_declarations.py; a case key must not be "
            f"printed in its place."
        )
    stale = sorted(descriptions.keys() - excluded)
    if stale:
        raise UpstreamDeclarationError(
            f"CASE_DESCRIPTIONS describes case(s) {stale} that no declaration excludes. Remove "
            f"each description, or restore the exclusion it describes."
        )


def check_version_notes(
    specs: Iterable[DataSourceSpec],
    version_notes: Mapping[str, str],
) -> None:
    """Fail unless every key is a published row and every record behind it declares a lane.

    A published row is one distinct public name, so a key is matched against ``public_name``.

    Raises:
        UpstreamDeclarationError: naming each offending key.
    """
    records = tuple(specs)
    for key in sorted(version_notes):
        behind_key = [spec for spec in records if spec.public_name == key]
        if not behind_key:
            raise UpstreamDeclarationError(
                f"TESTED_VERSION_NOTES names {key!r}, which is the public name of no registered "
                f"record and so resolves to no published row. Correct the key or remove the "
                f"entry."
            )
        laneless = sorted(spec.label for spec in behind_key if spec.ci_lane is None)
        if laneless:
            raise UpstreamDeclarationError(
                f"TESTED_VERSION_NOTES names {key!r}, but record(s) {laneless} declare no "
                f"continuous-integration lane. A version claim must be attested by a lane; "
                f"declare one or remove the entry."
            )


def load_upstream_facts() -> UpstreamFacts:
    """Return every upstream declaration, after checking the two maps declared here against them.

    Raises:
        UpstreamDeclarationError: if a record declares a fluent type, or the uncovered-path
            literal names a path, that ``CONNECTION_PATH_DESCRIPTIONS`` does not describe; or if
            ``TESTED_VERSION_NOTES`` names no published row or a data source with no lane; or if
            ``CASE_DESCRIPTIONS`` and the declared case exclusions disagree in either direction.
    """
    specs = iter_data_source_specs()
    check_connection_path_descriptions(
        specs, FLUENT_TYPES_NAMED_BY_NO_RECORD, CONNECTION_PATH_DESCRIPTIONS
    )
    check_version_notes(specs, TESTED_VERSION_NOTES)
    fluent_case_exclusions = case_exclusions_by_type()
    check_case_descriptions(specs, fluent_case_exclusions, CASE_DESCRIPTIONS)
    return UpstreamFacts(
        specs=specs,
        covered_but_unable_to_claim=RECORDS_COVERED_BUT_UNABLE_TO_CLAIM,
        fluent_types_named_by_no_record=FLUENT_TYPES_NAMED_BY_NO_RECORD,
        fluent_case_exclusions=fluent_case_exclusions,
    )
