from __future__ import annotations

import logging
import pathlib
import re
from collections import defaultdict
from pprint import pformat as pf
from typing import TYPE_CHECKING

import pandas as pd
import pytest
import requests

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import CloudDataContext, FileDataContext
from great_expectations.datasource.fluent.constants import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
)
from tests.datasource.fluent._fake_cloud_api import (
    DEFAULT_HEADERS,
    FAKE_ORG_ID,
    GX_CLOUD_MOCK_BASE_URL,
    UUID_REGEX,
    CallbackResult,
    CloudResponseSchema,
)
from tests.datasource.fluent.conftest import (
    CloudDetails,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from requests import PreparedRequest
    from responses import RequestsMock

    from tests.datasource.fluent._fake_cloud_api import FakeDBTypedDict


# apply markers to entire test module
pytestmark = [pytest.mark.integration]


yaml = YAMLHandler()

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def taxi_data_samples_dir() -> pathlib.Path:
    return pathlib.Path(
        __file__, "..", "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    ).resolve(strict=True)


@pytest.mark.cloud
def test_add_fluent_datasource_are_persisted(
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
    db_file: pathlib.Path,
    mocker: MockerFixture,
):
    context = empty_cloud_context_fluent
    set_spy = mocker.spy(context._datasource_store, "set")

    datasource_name = "save_ds_test"

    datasource = context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    )

    assert datasource.id
    assert set_spy.call_count == 1
    cloud_api_fake.assert_call_count(
        f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources",
        1,
    )


def test_add_fluent_datasource_are_persisted_without_duplicates(
    empty_file_context: FileDataContext,
    db_file: pathlib.Path,
):
    context = empty_file_context
    datasource_name = "save_ds_test"

    context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    )

    yaml_path = pathlib.Path(context.root_directory, context.GX_YML)
    assert yaml_path.exists()

    yaml_dict: dict = yaml.load(yaml_path.read_text())
    print(pf(yaml_dict, depth=2))
    assert datasource_name in yaml_dict["fluent_datasources"]
    assert datasource_name not in yaml_dict["datasources"]


@pytest.mark.cloud
def test_splitters_are_persisted_on_creation(
    empty_cloud_context_fluent: CloudDataContext,
    cloud_api_fake_db: FakeDBTypedDict,
    db_file: pathlib.Path,
):
    context = empty_cloud_context_fluent

    datasource_name = "save_ds_splitters_test"
    datasource = context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    )
    my_asset = datasource.add_table_asset("table_partitioned_by_date_column__A")
    my_asset.test_connection()
    my_asset.add_splitter_year("date")

    datasource_config = cloud_api_fake_db["datasources"][str(datasource.id)]["data"][
        "attributes"
    ]["datasource_config"]
    print(f"'{datasource_name}' config -> \n\n{pf(datasource_config)}")

    # splitters should be present
    assert datasource_config["assets"][0]["splitter"]


def test_assets_are_persisted_on_creation_and_removed_on_deletion(
    empty_file_context: FileDataContext,
    db_file: pathlib.Path,
):
    context = empty_file_context

    # ensure empty initial state
    yaml_path = pathlib.Path(context.root_directory, context.GX_YML)
    assert yaml_path.exists()
    assert not yaml.load(yaml_path.read_text()).get("fluent_datasources")

    datasource_name = "my_datasource"
    asset_name = "my_asset"

    context.sources.add_sqlite(
        name=datasource_name, connection_string=f"sqlite:///{db_file}"
    ).add_query_asset(
        asset_name, query='SELECT name FROM sqlite_master WHERE type = "table"'
    )

    fds_after_add: dict = yaml.load(yaml_path.read_text())["fluent_datasources"]  # type: ignore[assignment] # json union
    print(f"'{asset_name}' added\n-----------------\n{pf(fds_after_add)}")
    assert asset_name in fds_after_add[datasource_name]["assets"]

    context.fluent_datasources[datasource_name].delete_asset(asset_name)

    fds_after_delete: dict = yaml.load(yaml_path.read_text())["fluent_datasources"]  # type: ignore[assignment] # json union
    print(f"\n\n'{asset_name}' deleted\n-----------------\n{pf(fds_after_delete)}")
    assert asset_name not in fds_after_delete[datasource_name].get("assets", {})


@pytest.mark.cloud
def test_context_add_or_update_datasource(
    cloud_api_fake: RequestsMock,
    empty_contexts: CloudDataContext | FileDataContext,
    # db_file: pathlib.Path, TODO: sqlite deser broken
    taxi_data_samples_dir: pathlib.Path,
):
    context = empty_contexts

    datasource = context.sources.add_pandas_filesystem(
        name="save_ds_test", base_directory=taxi_data_samples_dir
    )
    datasource.add_csv_asset(
        name="my_asset",
    )

    # TODO: spy the store.delete calls instead of ctx specific tests
    if isinstance(empty_contexts, CloudDataContext):
        # TODO: adjust call counts as needed
        cloud_api_fake.assert_call_count(
            f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources",
            1,
        )
        cloud_api_fake.assert_call_count(
            f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources/{datasource.id}?name={datasource.name}",
            2,
        )

        response = requests.get(
            f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources/{datasource.id}"
        )
        response.raise_for_status()
        print(pf(response.json(), depth=4))
        assert response.json()["data"]["attributes"]["datasource_config"].get("assets")

    # add_or_update should be idempotent
    datasource = context.sources.add_or_update_pandas_filesystem(
        name="save_ds_test", base_directory=taxi_data_samples_dir
    )


@pytest.mark.cloud
def test_cloud_add_or_update_datasource_kw_vs_positional(
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
    taxi_data_samples_dir: pathlib.Path,
):
    name: str = "kw_vs_positional_test"

    datasource1 = empty_cloud_context_fluent.sources.add_pandas_filesystem(
        name=name, base_directory=taxi_data_samples_dir
    )

    # pass name as keyword arg
    datasource2 = empty_cloud_context_fluent.sources.add_or_update_pandas_filesystem(
        name=name, base_directory=taxi_data_samples_dir
    )

    # pass name as positional arg
    datasource3 = empty_cloud_context_fluent.sources.add_or_update_pandas_filesystem(
        name, base_directory=taxi_data_samples_dir
    )

    assert datasource1 == datasource2 == datasource3


@pytest.mark.cloud
def test_context_add_and_then_update_datasource(
    cloud_api_fake: RequestsMock,
    empty_contexts: CloudDataContext | FileDataContext,
    taxi_data_samples_dir: pathlib.Path,
):
    context = empty_contexts

    datasource1 = context.sources.add_pandas_filesystem(
        name="update_ds_test", base_directory=taxi_data_samples_dir
    )

    # add_or_update should be idempotent
    datasource2 = context.sources.update_pandas_filesystem(
        name="update_ds_test", base_directory=taxi_data_samples_dir
    )

    assert datasource1 == datasource2

    # modify a field
    datasource2.base_directory = pathlib.Path(__file__)
    datasource3 = context.sources.update_pandas_filesystem(datasource2)

    assert datasource1 != datasource3
    assert datasource2 == datasource3


@pytest.mark.cloud
def test_update_non_existant_datasource(
    cloud_api_fake: RequestsMock,
    empty_contexts: CloudDataContext | FileDataContext,
    taxi_data_samples_dir: pathlib.Path,
):
    context = empty_contexts

    with pytest.raises(ValueError, match="I_DONT_EXIST"):
        context.sources.update_pandas_filesystem(
            name="I_DONT_EXIST", base_directory=taxi_data_samples_dir
        )


@pytest.mark.cloud
def test_cloud_context_delete_datasource(
    cloud_api_fake: RequestsMock,
    empty_cloud_context_fluent: CloudDataContext,
    taxi_data_samples_dir: pathlib.Path,
):
    context = empty_cloud_context_fluent

    datasource = context.sources.add_pandas_filesystem(
        name="delete_ds_test", base_directory=taxi_data_samples_dir
    )

    # check cloud_api_fake items
    response1 = requests.get(
        f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources/{datasource.id}",
    )
    print(f"Before Delete -> {response1}\n{pf(response1.json())}\n")
    assert response1.status_code == 200

    context.sources.delete(datasource.name)
    assert datasource.name not in context.fluent_datasources

    cloud_api_fake.assert_call_count(
        f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources",
        1,
    )
    cloud_api_fake.assert_call_count(
        f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources/{datasource.id}",
        2,
    )

    response2 = requests.get(
        f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}/datasources/{datasource.id}",
    )
    print(f"After Delete -> {response2}\n{pf(response2.json())}")
    assert response2.status_code == 404


@pytest.fixture
def verify_asset_names_mock(cloud_api_fake: RequestsMock, cloud_details: CloudDetails):
    def verify_asset_name_cb(request: PreparedRequest) -> CallbackResult:
        if request.body:
            payload = CloudResponseSchema.from_datasource_json(request.body)
            LOGGER.info(f"PUT payload: ->\n{pf(payload.dict())}")
            assets = payload.data.attributes["datasource_config"]["assets"]  # type: ignore[index]
            assert assets, "No assets found"
            for asset in assets:
                if asset["name"] == DEFAULT_PANDAS_DATA_ASSET_NAME:  # type: ignore[index]
                    raise ValueError(
                        f"Asset name should not be default - '{DEFAULT_PANDAS_DATA_ASSET_NAME}'"
                    )
            return CallbackResult(
                200,
                headers=DEFAULT_HEADERS,
                body=payload.json(),
            )
        return CallbackResult(500, DEFAULT_HEADERS, "No body found")

    cloud_url = re.compile(
        f"{cloud_details.base_url}/organizations/{cloud_details.org_id}/datasources/{UUID_REGEX}"
    )

    cloud_api_fake.remove("PUT", url=cloud_url)
    cloud_api_fake.add_callback("PUT", url=cloud_url, callback=verify_asset_name_cb)

    return cloud_api_fake


@pytest.mark.cloud
class TestPandasDefaultWithCloud:
    def test_payload_sent_to_cloud(
        self,
        cloud_details: CloudDetails,
        empty_cloud_context_fluent: CloudDataContext,
        verify_asset_names_mock: RequestsMock,
    ):
        context = empty_cloud_context_fluent
        df = pd.DataFrame.from_dict(
            {"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]}
        )

        context.sources.pandas_default.read_dataframe(df)

        pandas_default_id = context.sources.pandas_default.id
        assert pandas_default_id

        assert verify_asset_names_mock.assert_call_count(
            f"{cloud_details.base_url}/organizations/{cloud_details.org_id}/datasources/{pandas_default_id}",
            1,
        )


def test_data_connectors_are_built_on_config_load(
    seeded_contexts: CloudDataContext | FileDataContext,
):
    """
    Ensure that all Datasources that require data_connectors have their data_connectors
    created when loaded from config.
    """
    context = seeded_contexts
    dc_datasources: dict[str, list[str]] = defaultdict(list)

    assert context.fluent_datasources
    for datasource in context.fluent_datasources.values():
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


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
