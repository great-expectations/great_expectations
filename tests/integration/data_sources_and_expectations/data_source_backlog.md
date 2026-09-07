# Data source onboarding backlog

This is a working backlog for maintainers, not a promise or a schedule. It records, for each data
source this repository knows about but does not exercise, which of the onboarding wiring surfaces
already exist here, which are missing, and what is concretely known about onboarding it. See the
onboarding walkthrough in
[`tests/integration/data_sources_and_expectations/README.md`](./README.md) for what each surface
is and how to add it — this document does not restate that material, only cross-references it.

## How to read an entry

The entries below are two different shapes of gap, and reading one as the other is the mistake this
section exists to prevent.

**Shape one: a SQL backend the shipped package has a dialect entry for and this harness does not
test.** These are checked against the seven onboarding surfaces the walkthrough's step 3 and step 4
describe, plus one further signal:

- **pytest marker** — an entry in `pyproject.toml`'s `[tool.pytest.ini_options] markers` list.
- **`REQUIRED_MARKERS` entry** — the marker name added to that set in `tests/conftest.py`.
- **dev requirements file** — `reqs/requirements-dev-<backend>.txt`.
- **task-runner entry** — a key in `tasks.py`'s `MARKER_DEPENDENCY_MAP`.
- **CI lane** — a token in the marker matrix in `.github/workflows/ci.yml`.
- **compose directory** — `assets/docker/<backend>/`, for a backend that would run as a local
  container.
- **harness declaration** — a `SqlDatasourceTestConfig` subclass under
  `tests/integration/test_utils/data_source_config/`, decorated with `@register_sql_config`.
- **`GXSqlDialect` member** — whether `great_expectations/execution_engine/sqlalchemy_dialect.py`
  already has an enum member for this dialect. This one is not a harness onboarding surface; it
  signals how much dialect-specific behavior the core execution engine already carries, independent
  of the test harness.

None of the five backends in that section has a harness declaration, so none is registered, and the
wiring drift check (`pytest tests/test_data_source_wiring.py -m project -q`) has nothing to say
about any of them — that check only walks data sources that are already registered.

**Shape two: a shipped, publicly documented fluent datasource class with no test surface at all.**
These are a different gap, and the surface checklist above does not describe them. A shape-one
backend has a dialect entry in the execution engine and usually a requirements file; what it lacks
is a harness that runs against it. A shape-two data source has a fluent class users can call
today, and a line on the public compatibility reference's supported list — all eight below appear
there — and *nothing* in this repository beyond a registered record stating that it exists. There
is usually no marker to add a `REQUIRED_MARKERS` entry for, no requirements file to include, and
no `GXSqlDialect` member either. `AlloyDatasource`, `AuroraDatasource`, `CitusDatasource` and
`NeonDatasource` each subclass `SQLDatasource` and type their connection string as a `PostgresDsn`,
so they are reached through the PostgreSQL dialect rather than one of their own, and the object
stores are not SQL at all.

Every shape-two data source below **is** registered, and its record is what makes it visible to
`pytest tests/test_data_source_registry.py -m project -q`. Each such record claims no tier, which
is the accurate statement: a tier claim asserts that a suite in this repository passes against that
data source, and none does. The drift check therefore has plenty to say about these records — it
verifies every coordinate they declare — and nothing to say about coverage, because they declare
none.

To list the untested records from the tree rather than from this document:

```
python -c "import tests.integration.test_utils.data_source_config as _; \
from tests.integration.test_utils.data_source_config.registry import iter_data_source_specs; \
print(sorted(s.label for s in iter_data_source_specs() if not s.tiers))"
```

## SQL backends with a dialect entry and no harness declaration

### Athena

- **Existing**: pytest marker (`athena`), `REQUIRED_MARKERS` entry, dev requirements file
  (`reqs/requirements-dev-athena.txt`, pinning `pyathena[SQLAlchemy]`), a task-runner entry in
  `tasks.py`'s `MARKER_DEPENDENCY_MAP` (naming only the requirements file, no `services`), a CI
  lane (the `athena` token in the marker matrix in `.github/workflows/ci.yml`), and a
  `GXSqlDialect` member (`AWSATHENA`, already consulted by the execution engine for temp-table and
  sampling behavior).
- **Missing**: harness declaration, compose directory.
- **Known obstacle**: `pyathena` connects to the hosted AWS Athena service over its API rather than
  to a local server, the same shape as this harness's existing `EXTERNAL_CREDENTIALS` backends
  (BigQuery, Databricks, Redshift, Snowflake). Athena has no `assets/docker/athena/` directory, and
  its task-runner entry declaring no `services` is consistent with that — there is nothing local to
  start. Onboarding Athena would plausibly declare
  `provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS`, the same way those four backends do, but
  that is inference from the existing pattern, not something verified against an actual connection.


### Dremio

- **Existing**: dev requirements file (`reqs/requirements-dev-dremio.txt`, pinning `pyodbc` and
  `sqlalchemy-dremio`), a `GXSqlDialect` member (`DREMIO`).
- **Missing**: pytest marker, `REQUIRED_MARKERS` entry, task-runner entry, CI lane, compose
  directory, harness declaration.
- **Known obstacle**: core marks the dialect experimental — "Dremio Support is experimental,
  functionality is not fully under test" in the execution engine. That is a stated caveat on the
  dialect support itself, not merely absent wiring.

### Hive

- **Existing**: dev requirements file (`reqs/requirements-dev-hive.txt`, pinning `PyHive`, `thrift`,
  and `thrift-sasl`), a `GXSqlDialect` member (`HIVE`).
- **Missing**: pytest marker, `REQUIRED_MARKERS` entry, task-runner entry, CI lane, compose
  directory, harness declaration.
- **Known obstacle**: none established. The surfaces are simply absent; nothing in the tree records
  why.


### Teradata

- **Existing**: dev requirements file (`reqs/requirements-dev-teradata.txt`, pinning
  `teradatasqlalchemy`), a `GXSqlDialect` member (`TERADATASQL`, consulted for temp-table creation
  and for dialect-module import, but — unlike Athena's `AWSATHENA` — reaching no partitioner or
  sampler code path).
- **Missing**: pytest marker, `REQUIRED_MARKERS` entry, task-runner entry, CI lane, compose
  directory, harness declaration.
- **Known obstacle**: core marks the dialect experimental — "Teradata Support is experimental,
  functionality is not fully under test" in the execution engine, and a matching note beside the
  temp-table path. That is a stated caveat on the dialect support itself, not merely absent wiring.

### Vertica

- **Existing**: dev requirements file (`reqs/requirements-dev-vertica.txt`, pinning
  `sqlalchemy-vertica-python`), a `GXSqlDialect` member (`VERTICA`).
- **Missing**: pytest marker, `REQUIRED_MARKERS` entry, task-runner entry, CI lane, compose
  directory, harness declaration.
- **Known obstacle**: none established. The surfaces are simply absent; nothing in the tree records
  why.


## Backends that have left this backlog

**Oracle.** Oracle was previously listed here as the sharpest gap of the set — a dialect the
execution engine already knew about, with no onboarding surface at all. It now has every one of
them: an `oracle` pytest marker, a `REQUIRED_MARKERS` entry, `reqs/requirements-dev-oracle.txt`, an
`"oracle"` key in `tasks.py`'s `MARKER_DEPENDENCY_MAP`, `assets/docker/oracle/`, an `oracle` token
in the marker matrix in `.github/workflows/ci.yml`, and `OracleDatasourceTestConfig` in
`tests/integration/test_utils/data_source_config/oracle.py`, which registers a record claiming a
tier. It is recorded here rather than silently deleted so that a reader who remembers the entry can
see that it closed rather than wonder whether it was dropped.

## What this harness cannot currently express

Nothing about the five SQL backends above demands a change to `SqlBackendSpec` itself — the
declaration's existing extension points (`uses_schema`, `column_type_overrides`,
`transaction_mode`, `insert_parameter_limit`, `table_schema_items`) are dialect facts, not
provisioning facts, and provisioning is already a three-way choice
(`DataSourceProvisioning.LOCAL_CONTAINER`, `LOCAL_FILE`, `EXTERNAL_CREDENTIALS`). Whichever of them
is onboarded next should fit the existing shape; nothing here identifies a fourth provisioning kind
or a missing declaration field.

The same holds for the data sources in the next section, for a different reason: a record that
claims no tier needs only `DataSourceSpec`'s identity and provisioning fields, and every one of the
eight is expressible with what exists today. What none of them is expressible *with* is a config —
and that is the point, not a gap in the declaration: requiring a config in order to describe a data
source would mean the only data sources this repository can name are the ones it happens to run.

## Data sources declared but not tested

Each of the eight below has a registered record and no suite validating expectations against
it. Two of them, Amazon S3 and Google Cloud Storage, do claim the fluent API tier, so their
fluent types are covered by that tier's suite; the rest carry no tier at all. The records live in
`tests/integration/test_utils/data_source_config/declaration_only.py`; what a record declares is
checked by the drift check, and what follows here is what onboarding one would concretely require.

Object stores are described once each rather than once per fluent datasource type, because a single
storage target read by two engines is one data source. That is why each object-store entry names
two fluent types.

### Amazon S3

- **Fluent types**: `pandas_s3` (`PandasS3Datasource`), `spark_s3` (`SparkS3Datasource`).
- **Existing**: an `aws_deps` pytest marker with a `REQUIRED_MARKERS` entry, an `"aws_deps"` key in
  `tasks.py`'s `MARKER_DEPENDENCY_MAP`, and an `aws_deps` token in the marker matrix in
  `.github/workflows/ci.yml`. The record declares all three. **A lane is not a tier**: that job
  installs the AWS client libraries and runs the tests marked `aws_deps`; no suite validates
  expectations against an S3-backed batch.
- **Missing**: a harness config, and therefore any batch setup. There is no S3-specific requirements
  file either — the `aws_deps` task-runner entry names `reqs/requirements-dev-lite.txt`, which is
  the shared lite file rather than anything specific to S3, which is why the record declares no
  `dev_requirements_file`.
- **What onboarding would require**: a config that provisions a bucket and prefix from credentials,
  plus a decision about whether the existing `aws_deps` lane is the right place to run it or whether
  expectation coverage over S3 deserves a lane of its own. `aws_deps` is a *shared* marker — it
  names a dependency class, not this data source — so a tier claimed against it would be a claim
  about a lane that also runs unrelated tests.

### Google Cloud Storage

- **Fluent types**: `pandas_gcs` (`PandasGoogleCloudStorageDatasource`), `spark_gcs`
  (`SparkGoogleCloudStorageDatasource`).
- **Existing**: a `gcs_deps` pytest marker with a `REQUIRED_MARKERS` entry,
  `reqs/requirements-dev-gcs.txt`, a `"gcs_deps"` key in `MARKER_DEPENDENCY_MAP`, and a `gcs_deps`
  token in the marker matrix. The record declares all four, including the requirements file, which —
  unlike the AWS case — is genuinely specific to this data source.
- **Missing**: a harness config.
- **What onboarding would require**: the same two decisions as Amazon S3. `gcs_deps` is likewise a
  shared marker naming a dependency class.

### Azure Blob Storage

- **Fluent types**: `pandas_abs` (`PandasAzureBlobStorageDatasource`), `spark_abs`
  (`SparkAzureBlobStorageDatasource`).
- **Existing**: `reqs/requirements-dev-azure.txt`, pinning `azure-identity`,
  `azure-keyvault-secrets` and `azure-storage-blob`; and `AZURE_ACCESS_KEY`, `AZURE_CREDENTIAL`,
  `AZURE_CONTAINER` and `AZURE_STORAGE_ACCOUNT_URL` in the environment block of
  `.github/workflows/ci.yml`.
- **Missing**: everything that would let a test select it. There is no Azure Blob Storage marker in
  the declared marker list, no `REQUIRED_MARKERS` entry, no `MARKER_DEPENDENCY_MAP` key, no token
  in the marker matrix, and no harness config. The record therefore declares no marker, no lane and
  no requirements file: naming a marker that has never been declared would fail the drift check,
  and naming the requirements file would read as "this file installs Azure Blob Storage's
  dependencies", which is true of only part of it.
- **What onboarding would require**: a marker and a `REQUIRED_MARKERS` entry first, because the
  secrets and the requirements file already exist and the marker is what nothing can be selected
  without. This is the object store with the widest gap between what CI is already configured to
  authenticate against and what any test can ask for.

### AlloyDB

- **Fluent type**: `alloy` (`AlloyDatasource`).
- **Existing**: the fluent class, and a `SupportedDataSources.ALLOY` member in the shipped package
  whose value the record's public name is taken from verbatim.
- **Missing**: every harness surface — no marker, no `REQUIRED_MARKERS` entry, no requirements file,
  no task-runner key, no CI lane, no compose directory, no config.
- **What onboarding would require**: credentials for a managed Google Cloud database. Nothing in
  this repository can start one, which is why the record declares
  `DataSourceProvisioning.EXTERNAL_CREDENTIALS`. Beyond that, the six wiring surfaces plus a config;
  the connection itself is PostgreSQL-dialect, so no new `GXSqlDialect` member is implied.

### Amazon Aurora PostgreSQL

- **Fluent type**: `aurora` (`AuroraDatasource`).
- **Existing**: the fluent class, and a `SupportedDataSources.AURORA` member. The record carries
  that member's value verbatim, qualifier included — the shipped vocabulary spells this "Amazon
  Aurora PostgreSQL", and a shorter spelling here would be a second name for one data source.
- **Missing**: every harness surface, as for AlloyDB.
- **What onboarding would require**: credentials for a managed AWS database, plus the six wiring
  surfaces and a config. Also PostgreSQL-dialect.

### Citus

- **Fluent type**: `citus` (`CitusDatasource`).
- **Existing**: the fluent class, and a `SupportedDataSources.CITUS` member.
- **Missing**: every harness surface.
- **What onboarding would require — costed.** Citus is a PostgreSQL extension distributed as a
  container image, so unlike its four neighbours here a test run could obtain one locally; the
  record declares `DataSourceProvisioning.LOCAL_CONTAINER` for that reason. This repository has no
  compose file for it, so onboarding it as a tested backend costs **seven** surfaces:

  1. a new pytest marker in `pyproject.toml`'s declared marker list,
  2. a `REQUIRED_MARKERS` entry in `tests/conftest.py`,
  3. a requirements file under `reqs/`,
  4. a `MARKER_DEPENDENCY_MAP` entry in `tasks.py` naming that file and the compose service,
  5. a compose directory under `assets/docker/`,
  6. a CI lane marker token in `.github/workflows/ci.yml`,
  7. a harness config registering a record that claims a tier.

  That count is the reason this record declares `LOCAL_CONTAINER` while naming no compose service:
  local-container provisioning with no service is legal only because the record claims no tier.
  Declaring `EXTERNAL_CREDENTIALS` instead would misdescribe how Citus is reached; naming a compose
  service would name something that does not exist.

### Neon

- **Fluent type**: `neon` (`NeonDatasource`).
- **Existing**: the fluent class, and a `SupportedDataSources.NEON` member.
- **Missing**: every harness surface.
- **What onboarding would require**: credentials for a managed Neon database, plus the six wiring
  surfaces and a config. PostgreSQL-dialect.

### Microsoft Fabric

- **Fluent type**: `fabric` (`FabricDatasource`).
- **Existing**: the fluent class. Nothing else, including — unlike the four flavors above — any
  member in the shipped supported-data-source vocabulary; see
  [Gaps in the shipped package](#gaps-in-the-shipped-package-this-harness-does-not-close) below.
- **Missing**: every harness surface.
- **What onboarding would require, starting from the authentication requirement.** Reaching the real
  service requires Entra ID **service principal** credentials — tenant id, client id and client
  secret. This is not a preference to be traded off against a simpler mode: `FabricDatasource` types
  its connection string as `EntraIDServicePrincipalAuthConnectionDetails` and raises
  `UnsupportedAuthenticationError` for every other authentication mode. An effort scoping a lane for
  Fabric starts from provisioning those three secrets in CI; the six wiring surfaces and a config
  follow, and none of them can be exercised until the credentials exist.

## Public names the compatibility reference omits

The public compatibility reference at
[`docs/docusaurus/docs/help/compatibility_reference.md`](../../../docs/docusaurus/docs/help/compatibility_reference.md)
lists eighteen data sources in the "Data sources" row of its support table. Every one of them
resolves to a registered record. Four registered data sources are absent from that list, and two
more resolve only by reading past a spelling difference. Both kinds are recorded here so that the
effort which regenerates that page sees them rather than rediscovering them.

**Registered, containerized, CI-lane backends absent from the supported list:**

| Data source | Registered record | Provisioning | Marker and lane | Where the reference mentions it |
| --- | --- | --- | --- | --- |
| ClickHouse | `clickhouse` | `LOCAL_CONTAINER` | `clickhouse`, in the marker matrix | Only in the notes column, among data sources GX "has seen work in the past" with no ongoing guarantee (spelled "Clickhouse" there) |
| MySQL | `mysql` | `LOCAL_CONTAINER` | `mysql`, in the marker matrix | Only in the notes column, same caveat |
| Oracle | `oracle` | `LOCAL_CONTAINER` | `oracle`, in the marker matrix | Not at all |
| SingleStore | `singlestore` | `LOCAL_CONTAINER` | `singlestore`, in the marker matrix | Not at all |

Each of the four has a compose directory under `assets/docker/`, a `MARKER_DEPENDENCY_MAP` entry, a
token in the marker matrix, and a record claiming a tier — which is to say a suite in this
repository runs against each of them on every relevant CI run. The reference's supported list says
otherwise for all four, and for ClickHouse and MySQL it says so explicitly by placing them among the
data sources with no ongoing compatibility guarantee.

**Two names that resolve only past a spelling difference:**

| On the reference | Registered public name |
| --- | --- |
| Databricks SQL | `Databricks (SQL)` |
| Microsoft SQL Server | `SQL Server` |

These are the same data sources under two spellings, not omissions. The registered spellings are
taken verbatim from the shipped supported-data-source vocabulary, which is the reason they are not
changed here to match the reference: the vocabulary is the published surface Core Expectations
declare their support against, and one of the two pages has to move to the other.

**The reference's notes column is a separate list and is not covered by the claim above.** It names
nine data sources GX "has seen work in the past" with no ongoing compatibility guarantee: Athena,
AWS Glue, Clickhouse, Databricks (Spark), Dremio, EMR Spark, MySQL, Teradata and Vertica. Four of
them — Athena, Dremio, Teradata and Vertica — are the shape-one entries at the top of this document.
Two — Clickhouse and MySQL — are registered, tiered, CI-lane backends and appear in the table above.
The remaining three — AWS Glue, Databricks (Spark) and EMR Spark — are catalog or Spark deployment
environments rather than distinct data sources, and none of them has a registered record, a pytest
marker, or a CI lane. AWS Glue is the one with any trace in the tree at all:
`tests/datasource/conftest.py` holds `moto`-mocked Glue Data Catalog fixtures for the legacy data connectors named in
`great_expectations/data_context/types/base.py`, which is a data *connector* surface rather than a
data source this harness could onboard.

To reproduce both tables from the tree:

```
python -c "import pathlib, tests.integration.test_utils.data_source_config as _; \
from tests.integration.test_utils.data_source_config.registry import iter_data_source_specs; \
row = [l for l in pathlib.Path('docs/docusaurus/docs/help/compatibility_reference.md') \
       .read_text().splitlines() if l.startswith('| Data sources')][0]; \
supported = {s.strip() for s in row.split('|')[2].split('<br/>')}; \
names = {s.public_name for s in iter_data_source_specs()}; \
print('registered, not supported:', sorted(names - supported)); \
print('supported, no matching name:', sorted(supported - names))"
```

Two entries in that first result — `Databricks (SQL)` and `SQL Server` — are the spelling
difference rather than an omission, which is why the check is read rather than asserted.

## Gaps in the shipped package this harness does not close

Two gaps sit in `great_expectations/` itself. Neither is fixed here, because a test harness does not
get to change a published surface of the package it tests; both are recorded so the efforts that own
those surfaces start from a written fact.

**The supported-data-source vocabulary has no member for Microsoft Fabric.**
`great_expectations/expectations/metadata_types.py`'s `SupportedDataSources` is the vocabulary Core
Expectations declare their support against. It has a member for AlloyDB, Amazon Aurora PostgreSQL,
Citus and Neon, whose values the corresponding records adopt verbatim, and none for Microsoft
Fabric — which is the one public name among those five that had to be supplied here rather than
sourced. Fabric appears on the public compatibility reference's supported list, so the vocabulary
and the reference already disagree about it. Adding a member is a product decision about what Core
Expectations advertise, with user-visible consequences, and is not the harness's to make.

The alignment check in `tests/test_data_source_registry.py` is one-directional for that reason:
every vocabulary member must reach a registered record, but a record need not have a member. What
keeps that from being a silent ratchet is the literal `_PUBLIC_NAMES_WITH_NO_CORE_MEMBER` in the
same module, which pins the eight registered public names with no member — the three object stores,
Microsoft Fabric, ClickHouse, Oracle, SingleStore and Trino. A member added upstream for any of them
fails that check and prompts the record to adopt the member's exact value.

**The fluent datasource type stubs have no overloads for four flavors.**
`great_expectations/datasource/fluent/sources.pyi` declares no `add_alloy`, `add_aurora`,
`add_citus` or `add_neon` overload, though all four methods exist at runtime — `hasattr` on a
context's `data_sources` returns `True` for each. A caller using them therefore gets no type
checking and no editor completion for arguments that are checked at runtime. This one belongs to
the effort that owns the fluent datasource API surface rather than to the harness; it is recorded
here because declaring records for those four flavors is what surfaced it.

## The two filesystem configs are unhashable, and the obvious fix is a wrong-data defect

Both halves are written here together, because a maintainer who rediscovers the first half and
reaches for the obvious remedy causes the second.

**First half: two of the three non-SQL configs cannot be hashed.**
`PandasFilesystemCsvDatasourceTestConfig` and `SparkFilesystemCsvDatasourceTestConfig` each
re-declare themselves `@dataclass(frozen=True)` in order to add their `read_options` and
`write_options` fields. Re-declaring regenerates `__eq__` and `__hash__`, discarding the
mapping-safe implementations `DataSourceTestConfig` hand-writes. Constructing either and hashing it
raises `TypeError: unhashable type: 'dict'`. `PandasDataFrameDatasourceTestConfig`, which adds no
field and so is not re-declared, hashes correctly and inherits both implementations.

The defect is latent because nothing hashes a config today: the session-scoped cache in
`tests/integration/conftest.py` keys on `TestConfig`, whose hand-written `__hash__` hashes the data
frames and the class, not the data source config.

**Second half: the remedy the harness prescribes for SQL configs must not be applied to these two.**
That remedy — pass `eq=False` so the base's mapping-safe implementations are inherited — is correct
for a config that adds no comparable field. `DataSourceTestConfig.__eq__` compares only the test
label and the pytest mark. The generated `__eq__` these two currently carry compares every field,
including the very option mappings they exist to carry. Executed against the tree, two
`PandasFilesystemCsvDatasourceTestConfig` instances differing only in `read_options` compare
**unequal** today and compare **equal** under the inherited implementation.

That matters because `TestConfig.__eq__` delegates to `self.data_source_config == value.data_source_config`,
and the session cache is a dict keyed on `TestConfig`. Two entries whose configs differ only in read
or write options would collapse into one, and the second test would silently read its CSVs with the
first test's options.

**This is not hypothetical.** A live instance exists:
[`test_expectation_conditions.py`](./test_expectation_conditions.py) constructs
`PandasFilesystemCsvDatasourceTestConfig(read_options={"parse_dates": [...], "date_format":
"mixed"})`, while eight expectation modules under
[`expectations/`](./expectations/) construct the same config with its default empty options
(`grep -ro 'PandasFilesystemCsvDatasourceTestConfig()' tests/integration/data_sources_and_expectations/expectations/ | wc -l`). Under the inherited equality, the date-parsing instance and every
default-configured one compare equal and hash equal.

So: if the unhashability is worth fixing, fix it by making the two configs' own generated `__hash__`
mapping-safe, or by not re-declaring them — not by opting out of generated equality. Reproduce both
halves with:

```
python -c "from tests.integration.test_utils.data_source_config.pandas_filesystem_csv \
import PandasFilesystemCsvDatasourceTestConfig as P; \
print(P().__class__.__eq__.__qualname__); \
print(P(read_options={'parse_dates': ['a']}) == P())"
```

which prints the generated `__eq__` and `False` today, and would print
`DataSourceTestConfig.__eq__` and `True` after the remedy.

## Adjacent gaps observed but not fixed

These were noticed while surveying the data sources above but sit outside what this document's own
effort touches, so they are recorded as backlog rather than fixed here. Each is a **class** of gap
that can recur on any backend, illustrated with one **dated instance** — not a standing claim about
the named backend, because another change may close that instance at any time without this document
being updated.

**A marker can be required without being declared, so marker coverage accepts a test that pytest
refuses to collect.** Observed for `cli` as of 2026-08-27: `cli` is a member of `REQUIRED_MARKERS`
in `tests/conftest.py`, but there is no `cli:` line in `pyproject.toml`'s
`[tool.pytest.ini_options] markers` list. The two surfaces are checked independently and neither
knows about the other. `_verify_marker_coverage` in `tests/conftest.py` intersects each test's marks
with `REQUIRED_MARKERS`, so a test carrying `@pytest.mark.cli` counts as covered; pytest itself
looks the mark up in the declared list, does not find it, and raises `PytestUnknownMarkWarning`.
Because this repository configures `filterwarnings = ["error", ...]`, that warning is an error, and
the module fails at collection rather than merely warning:

```
E   pytest.PytestUnknownMarkWarning: Unknown pytest.mark.cli - is this a typo?
```

No test in the tree carries `cli` today, which is why nothing is red. The failure appears the moment
one does — and it appears as a collection error in an unrelated module, some distance from the
declared-marker list that causes it. It is recorded rather than fixed because the fix is a line in
`pyproject.toml`, a configuration surface this effort reads and does not write.

**A backend can have a compose directory that is never started, because the task-runner entry that
would start it declares no `services`.** Observed for `clickhouse` as of 2026-08-07:
`assets/docker/clickhouse/` exists, but `tasks.py`'s `"clickhouse"` entry in
`MARKER_DEPENDENCY_MAP` names only its requirements file, with no `services` tuple, so running that
marker through the task runner never brings the container up.

(`databricks`'s task-runner entry has the same shape — no `services` named — but Databricks is
credential-provisioned rather than locally containerized, so there is nothing local for its entry
to start; that is not the same gap.)

**A backend can be absent from the aggregate SQLAlchemy requirements file and from both of the
SQLAlchemy pin groups in `setup.py`, leaving its extra with no SQLAlchemy version constraint.**
Observed for `singlestore` as of 2026-08-07: SingleStore has its own
`reqs/requirements-dev-singlestore.txt`, but that file is not one of the `--requirement` includes
in `reqs/requirements-dev-sqlalchemy.txt` (which lists, among others, `athena`, `dremio`,
`teradata`, `hive`, and `vertica`), and the key `"singlestore"` appears in neither of `setup.py`'s
`sqla1x_only_keys` nor `sqla_keys` tuples.

The extra itself does exist — `setup.py` derives one per `reqs/requirements-dev-*.txt` file, so
`great_expectations[singlestore]` installs `singlestoredb` and `sqlalchemy-singlestoredb`. What the
pin groups govern is which SQLAlchemy constraint gets appended to an extra that already exists.
Belonging to neither leaves `singlestore` the only SQL backend extra shipping without one, where
`athena` carries `sqlalchemy>=1.4.0` and `teradata` carries `sqlalchemy<2.0.0`. Worth noting
plainly:
SingleStore is the backend this document's own effort registered with the harness; this gap is a
fresh instance next to that work, not one inherited from before it, and it is recorded here rather
than fixed because `reqs/` and `setup.py` are outside what this effort's boundary covers.

**The generic SQL escape hatch left the shared data-source lists, and generic-SQL coverage over the
expectation modules now needs a lane rather than a list entry.** `GenericSQLDatasourceTestConfig`
used to sit in the hand-written `ALL_DATA_SOURCES` and `SQL_DATA_SOURCES` in
`test_canonical_expectations.py`. Those two lists are now the derived pair in
`tests/integration/test_utils/data_source_config/tiers.py`, and the config's own docstring is what
keeps it out of them: it says the config "must never appear in the set that gates CI", and the
derived lists are exactly that set. It takes a caller-supplied connection string and has no fixed
identity to register under, so there is no record to derive an entry from either.

Removing it changed what executes in no lane, and that was measured rather than argued: across
`tests/integration/data_sources_and_expectations`, the selected-test count for every marker a
workflow selects is unchanged, and the `generic_sql` marker — declared in `pyproject.toml`, required
in `tests/conftest.py::REQUIRED_MARKERS`, and named by the config's own `CiLaneRef` — appears in no
file under `.github/workflows/` and in no `MARKER_DEPENDENCY_MAP` lane. The parameterizations it
produced were collected by a bare run of the directory and selected by nothing that runs.

A maintainer who wants generic-SQL coverage over the expectation modules should therefore **add a
lane that selects the `generic_sql` marker** — a workflow job, or a task-runner entry supplying the
connection string the config needs — rather than re-adding an instance to a derived list, which
would put a never-run case back into every list-driven parameterization including the metrics tree,
and would reinstate the violation of the config's own documented rule. The two lists a
generic-SQL entry may still legitimately appear in are the module-local ones and
`DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS` in
[`data_source_lists.py`](./data_source_lists.py), none of which is derived or CI-gating.
