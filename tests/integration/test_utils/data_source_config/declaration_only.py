"""Data sources this repository declares but does not exercise.

A declaration-only record states what a data source *is* - its harness label, the name its users
know it by, where a test run would obtain an instance of it, and the fluent datasource types it is
reachable through - together with whatever wiring genuinely exists for it in this repository. It
asserts nothing about coverage beyond what a claimed tier states. Most records here claim no tier,
because no suite in this repository runs against them, and a tier claim asserts that one does.

**A declared CI lane and a tier claim are different assertions, and conflating them is the one
misreading this module has to prevent.** A lane means a job installs this data source's
dependencies and runs something; a tier means that tier's suite passes here. Only the second is a
support claim. That distinction is what lets Amazon S3 and Google Cloud Storage declare the real
lanes that install their client libraries and, separately, claim the tier whose suite genuinely
covers their fluent types - a reader who conflated the two would credit the lane alone with a
support claim it does not make.

Registration goes through the config-less entry point, because none of these has a harness config
class: requiring one would mean the only data sources this repository can describe are the ones it
happens to run, which is exactly the gap that makes "what data sources exist" unanswerable from
code. Every record here is held to the same registration rules as a config-bound one.

Each declared coordinate below - marker, task-runner key, CI lane, requirements file - names an
entry that exists in this repository today, and each absence is a statement too: the drift check
rejects a coordinate that resolves to nothing, so naming a marker that has never been declared
would fail it, while naming none is the accurate description of a data source no marker selects.
"""

from __future__ import annotations

from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceSpec,
    MarkerScope,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_data_source

# --------------------------------------------------------------------------------------------
# Object stores.
#
# One record per data source, not one per fluent datasource type. Each of these is a single
# storage target that both a dataframe engine and Spark can read, so it is described once and
# names every fluent type it corresponds to. For the same reason `execution_engine` is left
# unset on all three: more than one engine reads them, and naming a single one would state
# something false rather than leaving a fact undeclared.
# --------------------------------------------------------------------------------------------

AMAZON_S3 = register_data_source(
    DataSourceSpec(
        label="amazon-s3",
        public_name="Amazon S3",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        # Shared, not dedicated: `aws_deps` names a dependency class - the tests that need the AWS
        # client libraries - rather than this data source alone, so it can legitimately be claimed
        # by more than one record and must not be checked for collision as though it named S3.
        marker="aws_deps",
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="aws_deps"),
        tiers=frozenset({SupportTier.FLUENT_API}),
        task_runner_marker="aws_deps",
        # No dev_requirements_file, deliberately. The `aws_deps` task-runner entry names
        # `reqs/requirements-dev-lite.txt`, which is the shared lite requirements file rather than
        # anything specific to S3. Declaring it here would read as "this file installs Amazon S3's
        # dependencies", which is not true of that file.
    )
)

GOOGLE_CLOUD_STORAGE = register_data_source(
    DataSourceSpec(
        label="google-cloud-storage",
        public_name="Google Cloud Storage",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_gcs", "spark_gcs"}),
        # Shared for the same reason as `aws_deps` above: `gcs_deps` names the tests that need the
        # Google Cloud Storage client libraries, not this data source alone.
        marker="gcs_deps",
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="gcs_deps"),
        tiers=frozenset({SupportTier.FLUENT_API}),
        # Unlike `aws_deps`, this task-runner entry names a data-source-specific requirements file,
        # so the record can declare it truthfully.
        dev_requirements_file="reqs/requirements-dev-gcs.txt",
        task_runner_marker="gcs_deps",
    )
)

AZURE_BLOB_STORAGE = register_data_source(
    DataSourceSpec(
        label="azure-blob-storage",
        public_name="Azure Blob Storage",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_abs", "spark_abs"}),
        # No marker, no lane, no task-runner entry, and that is the accurate statement rather than
        # an omission: the declared marker list in `pyproject.toml` has no Azure Blob Storage
        # marker at all, so there is nothing for this record to name. Naming one that does not
        # exist would fail the wiring drift check; naming nothing says what is true.
    )
)

# --------------------------------------------------------------------------------------------
# The postgres-compatible flavors and Microsoft Fabric.
#
# Each has a fluent datasource class in the shipped package and no harness config, no marker, no
# lane and no requirements file. Their public names come from the shipped supported-data-source
# vocabulary verbatim wherever it has a member, so that this record set does not start a second
# spelling of a name the package already fixes.
# --------------------------------------------------------------------------------------------

ALLOYDB = register_data_source(
    DataSourceSpec(
        label="alloydb",
        # Verbatim from `SupportedDataSources.ALLOY` in
        # `great_expectations/expectations/metadata_types.py`.
        public_name="AlloyDB",
        # Reaching the real service means reaching a managed Google Cloud database with
        # credentials; nothing in this repository can start one locally.
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"alloy"}),
    )
)

AURORA = register_data_source(
    DataSourceSpec(
        label="aurora",
        # Verbatim from `SupportedDataSources.AURORA`, including the qualifier: the shipped
        # vocabulary spells this "Amazon Aurora PostgreSQL", and shortening it here would be the
        # second spelling this field exists to prevent.
        public_name="Amazon Aurora PostgreSQL",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"aurora"}),
    )
)

CITUS = register_data_source(
    DataSourceSpec(
        label="citus",
        # Verbatim from `SupportedDataSources.CITUS`.
        public_name="Citus",
        # Local-container provisioning with no container service, which is legal only because this
        # record claims no tier. Citus is a PostgreSQL extension distributed as a container image,
        # so a local container is genuinely how a test run would obtain one - and this repository
        # has no compose file for it. Declaring EXTERNAL_CREDENTIALS instead would misdescribe how
        # it is reached; naming a compose service would name something that does not exist.
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        fluent_types=frozenset({"citus"}),
        provisioning_note=(
            "Distributed as a container image, but this repository has no compose file for it and "
            "runs nothing against it. Onboarding it as a tested backend costs seven surfaces: a "
            "new pytest marker, a REQUIRED_MARKERS entry in tests/conftest.py, a requirements "
            "file under reqs/, a MARKER_DEPENDENCY_MAP entry in tasks.py, a compose directory "
            "under assets/docker/, a CI lane marker token, and a harness config."
        ),
    )
)

NEON = register_data_source(
    DataSourceSpec(
        label="neon",
        # Verbatim from `SupportedDataSources.NEON`.
        public_name="Neon",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"neon"}),
    )
)

MICROSOFT_FABRIC = register_data_source(
    DataSourceSpec(
        label="fabric",
        # Supplied here rather than sourced: the shipped supported-data-source vocabulary has no
        # member for Microsoft Fabric. That gap is recorded rather than closed, because the
        # vocabulary lives in the shipped package and this work changes nothing there.
        public_name="Microsoft Fabric",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"fabric"}),
        provisioning_note=(
            "Reaching the real service requires Entra ID service principal credentials - tenant "
            "id, client id and client secret. This is not a preference: FabricDatasource types "
            "its connection string as EntraIDServicePrincipalAuthConnectionDetails and raises "
            "UnsupportedAuthenticationError for every other authentication mode. An effort "
            "scoping a lane for it starts from that requirement."
        ),
    )
)
