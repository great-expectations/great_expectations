"""This file is meant for integration tests related to datasource CRUD."""
from __future__ import annotations

import random
import string
from typing import cast

import pytest

import great_expectations as gx
from great_expectations.data_context import CloudDataContext


@pytest.mark.cloud
def test_cloud_context_add_datasource_with_legacy_datasource_raises_error():
    pass


@pytest.mark.cloud
def test_cloud_context_add_datasource_with_fds():
    pass


@pytest.mark.e2e
@pytest.mark.cloud
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
