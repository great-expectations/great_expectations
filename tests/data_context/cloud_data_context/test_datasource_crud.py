"""This file is meant for integration tests related to datasource CRUD."""
from __future__ import annotations

import random
import string
from typing import cast

import pytest
import responses

import great_expectations as gx
from great_expectations.data_context import CloudDataContext
from great_expectations.data_context.types.base import GXCloudConfig
from great_expectations.datasource import Datasource
from great_expectations.datasource.fluent import PandasDatasource

# module level markers
pytestmark = pytest.mark.cloud


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


@responses.activate
def test_cloud_context_add_datasource_with_fds(
    empty_base_data_context_in_cloud_mode: CloudDataContext,
    ge_cloud_config: GXCloudConfig,
):
    context = empty_base_data_context_in_cloud_mode
    name = "my_pandas_ds"
    type_ = "pandas"
    id_ = "a135f497-31b0-4da3-9704-911bd9c190c3"

    responses.add(
        responses.POST,
        f"{ge_cloud_config.base_url}/organizations/{ge_cloud_config.organization_id}/datasources",
        json={
            "data": {
                "attributes": {
                    "datasource_config": {"id": id_, "name": name, "type": type_}
                },
                "id": id_,
                "type": "datasource",
            }
        },
        status=200,
    )

    responses.add(
        responses.GET,
        f"{ge_cloud_config.base_url}/organizations/{ge_cloud_config.organization_id}/datasources/{id_}?name={name}",
        json={
            "data": {
                "attributes": {
                    "datasource_config": {"id": id_, "name": name, "type": type_}
                },
                "id": id_,
                "type": "datasource",
            }
        },
        status=200,
    )

    fds = PandasDatasource(name=name)
    _ = context.add_datasource(datasource=fds)


@pytest.mark.e2e
def test_cloud_context_datasource_crud_e2e() -> None:
    context = cast(CloudDataContext, gx.get_context(cloud_mode=True))
    datasource_name = f"OSSTestDatasource_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"

    context.add_datasource(name=datasource_name, type="pandas")

    saved_datasource = context.get_datasource(datasource_name)
    assert saved_datasource is not None and saved_datasource.name == datasource_name

    context.delete_datasource(datasource_name)

    # Make another call to the backend to confirm deletion
    with pytest.raises(ValueError):
        context.get_datasource(datasource_name)
