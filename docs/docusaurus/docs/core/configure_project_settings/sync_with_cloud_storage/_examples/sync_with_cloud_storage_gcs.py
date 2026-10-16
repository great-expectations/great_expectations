"""
This is an example script for how to keep a File Data Context in Google Cloud Storage
and run validations from a copy of it.

To test, run:
pytest --docs-tests --gcs -k "docs_example_sync_with_cloud_storage_gcs" tests/integration/test_script_runner.py
"""

# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - full code example">
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define the storage location">
import pathlib
import tempfile

from google.cloud import storage

import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier

bucket_name = "my-bucket"
project_prefix = "gx/"
data_docs_prefix = "data_docs/"

client = storage.Client()


# </snippet>
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define transfer functions">
def upload_files(
    local_dir: pathlib.Path, paths: list[pathlib.Path], prefix: str
) -> None:
    """Upload each file to the bucket, keeping its path relative to local_dir."""
    bucket = client.bucket(bucket_name)
    for path in paths:
        blob = bucket.blob(prefix + path.relative_to(local_dir).as_posix())
        # The client sets the content type from the file name.
        blob.upload_from_filename(str(path))


def download_project(gx_dir: pathlib.Path, include_results: bool) -> None:
    """Download the project from the bucket into gx_dir."""
    for blob in client.list_blobs(bucket_name, prefix=project_prefix):
        if blob.name.endswith("/"):
            continue  # A folder placeholder, not a file
        relative_path = blob.name.removeprefix(project_prefix)
        if ".." in pathlib.PurePath(relative_path).parts:
            continue  # A name that points outside its folder
        if relative_path.startswith("uncommitted/") and not include_results:
            continue
        local_path = gx_dir / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local_path))


# </snippet>
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define publish_configuration">
def publish_configuration(gx_dir: pathlib.Path) -> None:
    """Upload the project's configuration, everything except uncommitted/."""
    paths = [
        path
        for path in gx_dir.rglob("*")
        if path.is_file() and path.relative_to(gx_dir).parts[0] != "uncommitted"
    ]
    upload_files(gx_dir, paths, project_prefix)


# </snippet>
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define run_checkpoint">
def run_checkpoint(
    checkpoint_name: str, run_name: str, batch_parameters: dict | None = None
) -> gx.checkpoint.CheckpointResult:
    """Run a Checkpoint from a fresh copy of the configuration and upload its results."""
    with tempfile.TemporaryDirectory() as work_dir:
        gx_dir = pathlib.Path(work_dir) / "gx"
        download_project(gx_dir, include_results=False)
        context = gx.get_context(mode="file", project_root_dir=work_dir)

        results_dir = gx_dir / "uncommitted" / "validations"
        files_before_run = set(results_dir.rglob("*"))
        checkpoint = context.checkpoints.get(checkpoint_name)
        result = checkpoint.run(
            batch_parameters=batch_parameters, run_id=RunIdentifier(run_name=run_name)
        )

        new_files = [
            path
            for path in results_dir.rglob("*")
            if path.is_file() and path not in files_before_run
        ]
        upload_files(gx_dir, new_files, project_prefix)
    return result


# </snippet>
# <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define publish_data_docs">
def publish_data_docs() -> None:
    """Build Data Docs from every stored Validation Result and upload the site."""
    with tempfile.TemporaryDirectory() as work_dir:
        gx_dir = pathlib.Path(work_dir) / "gx"
        download_project(gx_dir, include_results=True)
        context = gx.get_context(mode="file", project_root_dir=work_dir)
        context.build_data_docs()

        site_dir = gx_dir / "uncommitted" / "data_docs" / "local_site"
        paths = [path for path in site_dir.rglob("*") if path.is_file()]
        upload_files(site_dir, paths, data_docs_prefix)


# </snippet>
# </snippet>


# Everything below this line is test code. It isn't shown in the docs.
import os
import re
import shutil
import uuid

suite_name = "my_expectation_suite"
batch_id = "my_data_source-my_data_asset-year_2024-month_01"  # Source-Asset-options
run_time_format = "%Y%m%dT%H%M%S.%fZ"  # How GX writes a run time into a result key

# Write only below a prefix unique to this run, so concurrent runs never meet and the
# cleanup below can't touch anything else in the bucket.
test_root = "docs-tests/artifact-sync/"
run_prefix = f"{test_root}{uuid.uuid4()}/"
other_run = re.compile(r"docs-tests/artifact-sync/[0-9a-f-]{36}/.*")
bucket_name = os.environ["GX_GCS_TEST_BUCKET"]
project_prefix = run_prefix + "gx/"
data_docs_prefix = run_prefix + "data_docs/"


def build_example_project() -> None:
    """Create ./gx with one Checkpoint, as a project kept under version control would."""
    data_dir = pathlib.Path("data").resolve()
    data_dir.mkdir(exist_ok=True)
    (data_dir / "trips_2024-01.csv").write_text("passenger_count\n1\n2\n3\n")
    (data_dir / "trips_2024-02.csv").write_text("passenger_count\n4\n5\n")
    context = gx.get_context(mode="file", project_root_dir=".")
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="my_data_source", base_directory=data_dir
        )
        .add_csv_asset(name="my_data_asset")
        .add_batch_definition_monthly(
            name="my_batch_definition",
            regex=r"trips_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        )
    )
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="passenger_count")
    )
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="my_validation_definition", data=batch_definition, suite=suite
        )
    )
    context.checkpoints.add(
        gx.Checkpoint(
            name="my_checkpoint", validation_definitions=[validation_definition]
        )
    )


def list_bucket() -> dict[str, tuple]:
    """Map every object below docs-tests/artifact-sync/ to values that change on a write."""
    return {
        blob.name: (blob.generation, blob.updated)
        for blob in client.list_blobs(bucket_name, prefix=test_root)
    }


def delete_run_prefix() -> None:
    bucket = client.bucket(bucket_name)
    for blob in client.list_blobs(bucket_name, prefix=run_prefix):
        bucket.delete_blob(blob.name)


def result_path(result: gx.checkpoint.CheckpointResult, suffix: str) -> str:
    """The path GX gives this run's result: suite/run name/run time/batch."""
    run_time = result.run_id.run_time.strftime(run_time_format)
    return f"{suite_name}/my_run_name/{run_time}/{batch_id}{suffix}"


build_example_project()
bucket_before = list_bucket()
try:
    # <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call publish_configuration">
    publish_configuration(pathlib.Path("gx"))
    # </snippet>

    # The publishing job is done. Remove its copy so a worker can only succeed from
    # the configuration it downloads.
    shutil.rmtree("gx")

    # A folder placeholder, like the one the Google Cloud console creates for a new
    # folder.
    client.bucket(bucket_name).blob(project_prefix).upload_from_string(b"")

    # A worker that downloads uncommitted/ fails to load this file.
    client.bucket(bucket_name).blob(
        project_prefix + "uncommitted/config_variables.yml"
    ).upload_from_string(b"a: [")

    # <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call run_checkpoint">
    result = run_checkpoint(
        "my_checkpoint",
        run_name="my_run_name",
        batch_parameters={"year": 2024, "month": 1},
    )
    # </snippet>
    second_result = run_checkpoint(
        "my_checkpoint",
        run_name="my_run_name",
        batch_parameters={"year": 2024, "month": 1},
    )
    client.bucket(bucket_name).delete_blob(
        project_prefix + "uncommitted/config_variables.yml"
    )

    # <snippet name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call publish_data_docs">
    publish_data_docs()
    # </snippet>

    # Every Data Docs publish after the first replaces the pages already there.
    publish_data_docs()

    bucket_after = list_bucket()

    # Both runs pushed their result under the key GX itself uses, and the second run,
    # with the same run name, did not overwrite the first.
    result_keys = {
        project_prefix + "uncommitted/validations/" + result_path(r, ".json")
        for r in (result, second_result)
    }
    assert len(result_keys) == 2, result_keys
    uploaded_uncommitted = {
        key for key in bucket_after if key.startswith(project_prefix + "uncommitted/")
    }
    assert uploaded_uncommitted == result_keys, uploaded_uncommitted

    # The configuration was published.
    assert project_prefix + "great_expectations.yml" in bucket_after, "config missing"
    assert project_prefix + "checkpoints/my_checkpoint.json" in bucket_after, (
        "Checkpoint missing"
    )

    # Data Docs were built from both stored results and uploaded with a content type a
    # browser can display.
    assert data_docs_prefix + "index.html" in bucket_after, "Data Docs index missing"
    for r in (result, second_result):
        page = data_docs_prefix + "validations/" + result_path(r, ".html")
        assert page in bucket_after, page
    index = client.bucket(bucket_name).get_blob(data_docs_prefix + "index.html")
    assert index.content_type == "text/html", index.content_type

    # Nothing else under docs-tests/artifact-sync/ changed, apart from other runs of
    # this test. CI's identity can't write outside that prefix, so a stray write
    # fails there.
    changed = {
        key
        for key in bucket_before.keys() | bucket_after.keys()
        if bucket_before.get(key) != bucket_after.get(key)
    }
    outside = {
        key
        for key in changed
        if not key.startswith(run_prefix) and not other_run.fullmatch(key)
    }
    assert not outside, outside
finally:
    delete_run_prefix()
