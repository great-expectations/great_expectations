from __future__ import annotations

import difflib
import functools
import logging
import pathlib
import uuid
from collections import defaultdict
from pprint import pformat as pf
from typing import TYPE_CHECKING

import pytest
import responses

from great_expectations import get_context
from great_expectations.data_context import FileDataContext
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import (
    Datasource,
)
from tests.datasource.fluent.conftest import (
    FAKE_DATA_CONTEXT_ID,
    FAKE_ORG_ID,
    GX_CLOUD_MOCK_BASE_URL,
)

if TYPE_CHECKING:

    from great_expectations.data_context import CloudDataContext
    from great_expectations.datasource.fluent import SqliteDatasource

# apply markers to entire test module
pytestmark = [pytest.mark.integration]


logger = logging.getLogger(__file__)


@pytest.fixture
def db_file() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    db_file = pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)
    assert db_file.exists()
    return db_file


@pytest.fixture
def fluent_only_config(fluent_gx_config_yml_str: str) -> GxConfig:
    """Creates a fluent `GxConfig` object and ensures it contains at least one `Datasource`"""
    fluent_config = GxConfig.parse_yaml(fluent_gx_config_yml_str)
    assert fluent_config.datasources
    return fluent_config


@pytest.fixture
def fluent_yaml_config_file(
    file_dc_config_dir_init: pathlib.Path,
    fluent_gx_config_yml_str: str,
) -> pathlib.Path:
    """
    Dump the provided GxConfig to a temporary path. File is removed during test teardown.

    Append fluent config to default config file
    """
    config_file_path = file_dc_config_dir_init / FileDataContext.GX_YML

    assert config_file_path.exists() is True

    with open(config_file_path, mode="a") as f_append:
        yaml_string = "\n# Fluent\n" + fluent_gx_config_yml_str
        f_append.write(yaml_string)

    logger.debug(f"  Config File Text\n-----------\n{config_file_path.read_text()}")
    return config_file_path


@pytest.fixture
@functools.lru_cache(maxsize=1)
def seeded_fds_file_context(
    cloud_storage_get_client_doubles,
    fluent_yaml_config_file: pathlib.Path,
) -> FileDataContext:
    context = get_context(
        context_root_dir=fluent_yaml_config_file.parent, cloud_mode=False
    )
    assert isinstance(context, FileDataContext)
    return context


@pytest.fixture
def seed_cloud(
    cloud_storage_get_client_doubles,
    cloud_api_fake: responses.RequestsMock,
    fluent_only_config: GxConfig,
):
    """
    In order to load the seeded cloud config, this fixture must be called before any
    `get_context()` calls.
    """
    org_url_base = f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}"
    dc_config_url = f"{org_url_base}/data-context-configuration"

    by_id = {}
    fds_config = {}
    for datasource in fluent_only_config._json_dict()["fluent_datasources"]:
        datasource["id"] = str(uuid.uuid4())
        fds_config[datasource["name"]] = datasource
        by_id[datasource["id"]] = datasource

    logger.info(f"Seeded Datasources ->\n{pf(fds_config, depth=2)}")
    assert fds_config

    cloud_api_fake.upsert(
        responses.GET,
        dc_config_url,
        json={
            "anonymous_usage_statistics": {
                "data_context_id": FAKE_DATA_CONTEXT_ID,
                "enabled": False,
            },
            "datasources": fds_config,
        },
    )
    yield cloud_api_fake

    assert len(cloud_api_fake.calls) >= 1, f"{org_url_base} was never called"


@pytest.fixture
def seeded_cloud_context(
    seed_cloud,  # NOTE: this fixture must be called before the CloudDataContext is created
    empty_cloud_context_fluent,
):
    return empty_cloud_context_fluent


def test_load_an_existing_config(
    cloud_storage_get_client_doubles,
    fluent_yaml_config_file: pathlib.Path,
    fluent_only_config: GxConfig,
):
    context = get_context(
        context_root_dir=fluent_yaml_config_file.parent, cloud_mode=False
    )

    assert context.fluent_config == fluent_only_config


def test_serialize_fluent_config(
    cloud_storage_get_client_doubles,
    seeded_fds_file_context: FileDataContext,
):
    dumped_yaml: str = seeded_fds_file_context.fluent_config.yaml()
    print(f"  Dumped Config\n\n{dumped_yaml}\n")

    assert seeded_fds_file_context.fluent_config.datasources

    for (
        ds_name,
        datasource,
    ) in seeded_fds_file_context.fluent_config.get_datasources_as_dict().items():
        assert ds_name in dumped_yaml

        for asset_name in datasource.get_asset_names():
            assert asset_name in dumped_yaml


def test_data_connectors_are_built_on_config_load(
    seeded_fds_file_context: FileDataContext,
):
    """
    Ensure that all Datasources that require data_connectors have their data_connectors
    created when loaded from config.
    """
    dc_datasources: dict[str, list[str]] = defaultdict(list)

    for datasource in seeded_fds_file_context.fluent_datasources.values():
        if datasource.data_connector_type:
            print(f"class: {datasource.__class__.__name__}")
            print(f"type: {datasource.type}")
            print(f"data_connector: {datasource.data_connector_type.__name__}")
            print(f"name: {datasource.name}", end="\n\n")

            dc_datasources[datasource.type].append(datasource.name)

            for asset in datasource.assets:
                assert isinstance(asset._data_connector, datasource.data_connector_type)
            print()

    print(f"Datasources with DataConnectors\n{pf(dict(dc_datasources))}")
    assert dc_datasources


def test_data_connectors_are_built_on_cloud_config_load(
    seeded_cloud_context: CloudDataContext,
):
    """
    Ensure that all Datasources that require data_connectors have their data_connectors
    created when loaded from config.
    """
    dc_datasources: dict[str, list[str]] = defaultdict(list)

    assert seeded_cloud_context.fluent_datasources
    for datasource in seeded_cloud_context.fluent_datasources.values():
        if datasource.data_connector_type:
            print(f"class: {datasource.__class__.__name__}")
            print(f"type: {datasource.type}")
            print(f"data_connector: {datasource.data_connector_type.__name__}")
            print(f"name: {datasource.name}", end="\n\n")

            dc_datasources[datasource.type].append(datasource.name)

            for asset in datasource.assets:
                assert isinstance(asset._data_connector, datasource.data_connector_type)
            print()

    print(f"Datasources with DataConnectors\n{pf(dict(dc_datasources))}")
    assert dc_datasources


def test_fluent_simple_validate_workflow(seeded_fds_file_context: FileDataContext):
    datasource = seeded_fds_file_context.get_datasource("sqlite_taxi")
    assert isinstance(datasource, Datasource)
    batch_request = datasource.get_asset("my_asset").build_batch_request(
        {"year": 2019, "month": 1}
    )

    validator = seeded_fds_file_context.get_validator(batch_request=batch_request)
    result = validator.expect_column_max_to_be_between(
        column="passenger_count", min_value=1, max_value=12
    )
    print(f"  results ->\n{pf(result)}")
    assert result["success"] is True


def test_save_project_does_not_break(seeded_fds_file_context: FileDataContext):
    print(seeded_fds_file_context.fluent_config)
    seeded_fds_file_context._save_project_config()


def test_variables_save_config_does_not_break(seeded_fds_file_context: FileDataContext):
    print(f"\tcontext.fluent_config ->\n{seeded_fds_file_context.fluent_config}\n")
    print(f"\tcontext.variables ->\n{seeded_fds_file_context.variables}")
    seeded_fds_file_context.variables.save_config()


def test_save_datacontext_persists_fluent_config(
    file_dc_config_dir_init: pathlib.Path, fluent_only_config: GxConfig
):
    config_file = file_dc_config_dir_init / FileDataContext.GX_YML

    initial_yaml = config_file.read_text()
    for ds_name in fluent_only_config.get_datasource_names():
        assert ds_name not in initial_yaml

    context: FileDataContext = get_context(
        context_root_dir=config_file.parent, cloud_mode=False
    )

    context.fluent_config = fluent_only_config
    context._save_project_config()

    final_yaml = config_file.read_text()
    diff = difflib.ndiff(initial_yaml.splitlines(), final_yaml.splitlines())

    print("\n".join(diff))

    for ds_name in fluent_only_config.get_datasource_names():
        assert ds_name in final_yaml


def test_file_context_add_and_save_fluent_datasource(
    file_dc_config_dir_init: pathlib.Path,
    fluent_only_config: GxConfig,
    db_file: pathlib.Path,
):
    datasource_name = "save_ds_test"
    config_file = file_dc_config_dir_init / FileDataContext.GX_YML

    initial_yaml = config_file.read_text()
    assert datasource_name not in initial_yaml

    context: FileDataContext = get_context(
        context_root_dir=config_file.parent, cloud_mode=False
    )

    ds = context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    )

    final_yaml = config_file.read_text()
    diff = difflib.ndiff(initial_yaml.splitlines(), final_yaml.splitlines())

    print("\n".join(diff))

    assert datasource_name == ds.name
    assert datasource_name in final_yaml
    # ensure comments preserved
    assert "# Welcome to Great Expectations!" in final_yaml


def test_context_add_and_save_fluent_datasource(
    empty_contexts: CloudDataContext | FileDataContext,
    db_file: pathlib.Path,
):
    context = empty_contexts

    datasource_name = "save_ds_test"

    context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    )

    assert datasource_name in context.datasources


def test_context_add_or_update_datasource(
    empty_contexts: CloudDataContext | FileDataContext,
    db_file: pathlib.Path,
):
    context = empty_contexts

    datasource: SqliteDatasource = context.sources.add_sqlite(
        name="save_ds_test", connection_string=f"sqlite:///{db_file}"
    )

    assert datasource.connection_string == f"sqlite:///{db_file}"

    # modify the datasource
    datasource.connection_string = "sqlite:///"  # type: ignore[assignment]
    context.sources.add_or_update_sqlite(datasource)

    updated_datasource: SqliteDatasource = context.datasources[datasource.name]  # type: ignore[assignment]
    assert updated_datasource.connection_string == "sqlite:///"


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
