from __future__ import annotations

import difflib
import logging
import pathlib
import random
from pprint import pformat as pf
from typing import TYPE_CHECKING

import pytest

import great_expectations.expectations as gxe
from great_expectations import get_context
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.partitioners import ColumnPartitionerMonthly
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import CloudDataContext, FileDataContext
from great_expectations.datasource.fluent import SqliteDatasource
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import Datasource

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


YAML = YAMLHandler()

logger = logging.getLogger(__file__)


@pytest.mark.filesystem
def test_load_an_existing_config(
    cloud_storage_get_client_doubles,
    fluent_yaml_config_file: pathlib.Path,
    fluent_only_config: GxConfig,
):
    context = get_context(context_root_dir=fluent_yaml_config_file.parent, cloud_mode=False)

    assert context.fluent_config == fluent_only_config


@pytest.mark.filesystem
def test_serialize_fluent_config(
    cloud_storage_get_client_doubles,
    seeded_file_context: FileDataContext,
):
    dumped_yaml: str = seeded_file_context.fluent_config.yaml()
    print(f"  Dumped Config\n\n{dumped_yaml}\n")

    assert seeded_file_context.fluent_config.datasources

    for (
        ds_name,
        datasource,
    ) in seeded_file_context.fluent_config.get_datasources_as_dict().items():
        assert ds_name in dumped_yaml

        for asset_name in datasource.get_asset_names():
            assert asset_name in dumped_yaml


@pytest.mark.filesystem
def test_fluent_simple_validate_workflow(seeded_file_context: FileDataContext):
    datasource = seeded_file_context.data_sources.get("sqlite_taxi")
    assert isinstance(datasource, Datasource)
    partitioner = ColumnPartitionerMonthly(column_name="pickup_datetime")
    batch_request = datasource.get_asset("my_asset").build_batch_request(
        options={"year": 2019, "month": 1}, partitioner=partitioner
    )

    validator = seeded_file_context.get_validator(batch_request=batch_request)
    result = validator.expect_column_max_to_be_between(
        column="passenger_count", min_value=1, max_value=12
    )
    print(f"  results ->\n{pf(result)}")
    assert result["success"] is True


@pytest.mark.filesystem
def test_save_project_does_not_break(seeded_file_context: FileDataContext):
    print(seeded_file_context.fluent_config)
    seeded_file_context._save_project_config()


@pytest.mark.filesystem
def test_variables_save_does_not_break(seeded_file_context: FileDataContext):
    print(f"\tcontext.fluent_config ->\n{seeded_file_context.fluent_config}\n")
    print(f"\tcontext.variables ->\n{seeded_file_context.variables}")
    seeded_file_context.variables.save()


@pytest.mark.filesystem
def test_save_datacontext_persists_fluent_config(
    file_dc_config_dir_init: pathlib.Path, fluent_only_config: GxConfig
):
    config_file = file_dc_config_dir_init / FileDataContext.GX_YML

    initial_yaml = config_file.read_text()
    for ds_name in fluent_only_config.get_datasource_names():
        assert ds_name not in initial_yaml

    context: FileDataContext = get_context(context_root_dir=config_file.parent, cloud_mode=False)

    context.fluent_config = fluent_only_config
    context._save_project_config()

    final_yaml = config_file.read_text()
    diff = difflib.ndiff(initial_yaml.splitlines(), final_yaml.splitlines())

    print("\n".join(diff))

    for ds_name in fluent_only_config.get_datasource_names():
        assert ds_name in final_yaml


@pytest.mark.filesystem
def test_file_context_add_and_save_fluent_datasource(
    file_dc_config_dir_init: pathlib.Path,
    fluent_only_config: GxConfig,
    sqlite_database_path: pathlib.Path,
):
    datasource_name = "save_ds_test"
    config_file = file_dc_config_dir_init / FileDataContext.GX_YML

    initial_yaml = config_file.read_text()
    assert datasource_name not in initial_yaml

    context: FileDataContext = get_context(context_root_dir=config_file.parent, cloud_mode=False)

    ds = context.data_sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{sqlite_database_path}"
    )

    final_yaml = config_file.read_text()
    diff = difflib.ndiff(initial_yaml.splitlines(), final_yaml.splitlines())

    print("\n".join(diff))

    assert datasource_name == ds.name
    assert datasource_name in final_yaml
    # ensure comments preserved
    assert "# Welcome to Great Expectations!" in final_yaml


# Test markers come from empty_contexts fixture
def test_context_add_and_save_fluent_datasource(
    empty_contexts: CloudDataContext | FileDataContext,
    sqlite_database_path: pathlib.Path,
):
    context = empty_contexts

    datasource_name = "save_ds_test"

    context.data_sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{sqlite_database_path}"
    )

    assert datasource_name in context.data_sources.all()


# Test markers come from empty_contexts fixture
def test_context_add_or_update_datasource(
    empty_contexts: CloudDataContext | FileDataContext,
    sqlite_database_path: pathlib.Path,
):
    context = empty_contexts

    datasource: SqliteDatasource = context.data_sources.add_sqlite(
        name="save_ds_test", connection_string=f"sqlite:///{sqlite_database_path}"
    )

    assert datasource.connection_string == f"sqlite:///{sqlite_database_path}"

    # modify the datasource
    datasource.connection_string = "sqlite:///"  # type: ignore[assignment]
    context.data_sources.add_or_update_sqlite(datasource)

    updated_datasource = context.data_sources.all()[datasource.name]
    assert isinstance(updated_datasource, SqliteDatasource)
    assert updated_datasource.connection_string == "sqlite:///"


@pytest.fixture
def random_datasource(seeded_file_context: FileDataContext) -> Datasource:
    datasource = random.choice(list(seeded_file_context.fluent_datasources.values()))
    logger.info(f"Random DS - {pf(datasource.dict(), depth=1)}")
    return datasource


@pytest.mark.filesystem
def test_sources_delete_removes_datasource_from_yaml(
    random_datasource: Datasource,
    seeded_file_context: FileDataContext,
):
    print(f"Delete -> '{random_datasource.name}'\n")

    seeded_file_context.data_sources.delete(random_datasource.name)

    yaml_path = pathlib.Path(
        seeded_file_context.root_directory, seeded_file_context.GX_YML
    ).resolve(strict=True)
    yaml_contents = YAML.load(yaml_path.read_text())
    print(f"{pf(yaml_contents, depth=2)}")

    assert random_datasource.name not in yaml_contents["fluent_datasources"]  # type: ignore[operator] # always dict


@pytest.mark.filesystem
def test_ctx_delete_removes_datasource_from_yaml(
    random_datasource: Datasource, seeded_file_context: FileDataContext
):
    print(f"Delete -> '{random_datasource.name}'\n")

    seeded_file_context.delete_datasource(random_datasource.name)

    yaml_path = pathlib.Path(
        seeded_file_context.root_directory, seeded_file_context.GX_YML
    ).resolve(strict=True)
    yaml_contents = YAML.load(yaml_path.read_text())
    print(f"{pf(yaml_contents, depth=2)}")

    assert random_datasource.name not in yaml_contents["fluent_datasources"]  # type: ignore[operator] # always dict


# Test markers come from empty_contexts fixture
def test_quickstart_workflow(
    empty_contexts: CloudDataContext | FileDataContext,
    csv_path: pathlib.Path,
    mocker: MockerFixture,
):
    """
    What does this test do and why?

    Tests the Quickstart workflow noted in our docs: https://docs.greatexpectations.io/docs/tutorials/quickstart/

    In particular, this test covers the file-backend and cloud-backed usecases with this script.
    The ephemeral usecase is covered in: tests/integration/docusaurus/tutorials/quickstart/quickstart.py
    """  # noqa: E501
    # Slight deviation from the Quickstart here:
    #   1. Using existing contexts instead of `get_context`
    #   2. Using `read_csv` on a local file instead of making a network request
    #
    # These changes should be functionally equivalent to the real workflow but be better for testing
    context = empty_contexts
    if isinstance(context, CloudDataContext):
        pytest.xfail("Checkpoint run fails in some cases on GE Cloud")

    filepath = csv_path / "yellow_tripdata_sample_2019-01.csv"
    assert filepath.exists()

    batch = context.data_sources.pandas_default.read_csv(filepath)

    # Create Expectations
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6)
    )

    # Validate data
    result = batch.validate(suite)

    assert result.success

    # TODO: Add mechanism to view results.
    #       See: https://greatexpectations.atlassian.net/browse/V1-119
    # Previously we did:
    #     mock_open = mocker.patch("webbrowser.open")
    #     context.view_validation_result(result)
    #     mock_open.assert_called_once()
    # Our goal is to have:
    #     1. Setup mocks if necessary
    #     2. result.open_docs()
    #     3. assert docs opened.


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
