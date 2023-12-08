"""This file is meant for integration tests related to datasource CRUD."""
from __future__ import annotations

import random
import string
from typing import cast

import pytest

import great_expectations as gx
from great_expectations.data_context import CloudDataContext
from great_expectations.data_context.types.base import GXCloudConfig
from great_expectations.datasource import Datasource
from great_expectations.datasource.fluent import PandasDatasource

# module level markers
pytestmark = pytest.mark.cloud


def test_cloud_context_add_datasource_with_individual_fds_args_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
):
    context = empty_base_data_context_in_cloud_mode

    with pytest.raises(TypeError):
        context.add_datasource(name="my_pandas_fds", type="pandas", assets=[])


def test_cloud_context_add_datasource_with_legacy_datasource_raises_error(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
):
    context = empty_base_data_context_in_cloud_mode
    datasource = Datasource(
        name="my_legacy_datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    )

    with pytest.raises(TypeError):
        context.add_datasource(datasource=datasource)


def test_cloud_context_add_datasource_with_fds(
    cloud_api_fake,
    empty_cloud_data_context: CloudDataContext,
    ge_cloud_config: GXCloudConfig,
):
    context = empty_cloud_data_context
    name = "my_pandas_ds"

    post_url = f"{ge_cloud_config.base_url}/organizations/{ge_cloud_config.organization_id}/datasources"

    fds = PandasDatasource(name=name)
    _ = context.add_datasource(datasource=fds)

    assert cloud_api_fake.assert_call_count(url=post_url, count=2)


@pytest.mark.e2e
def test_cloud_context_datasource_crud_e2e() -> None:
    context = cast(CloudDataContext, gx.get_context(cloud_mode=True))
    datasource_name = f"OSSTestDatasource_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"

    context.sources.add_pandas(name=datasource_name)

    saved_datasource = context.get_datasource(datasource_name)
    assert saved_datasource is not None and saved_datasource.name == datasource_name

    context.delete_datasource(datasource_name)

    # Make another call to the backend to confirm deletion
    with pytest.raises(ValueError):
        context.get_datasource(datasource_name)
