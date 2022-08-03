import pathlib

import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudStoreBackend,
)
from great_expectations.data_context.store.inline_store_backend import (
    InlineStoreBackend,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig


@pytest.mark.integration
def test_data_context_instantiates_ge_cloud_store_backend_with_cloud_config(
    tmp_path: pathlib,
    data_context_config_with_datasources: DataContextConfig,
    ge_cloud_config: GeCloudConfig,
) -> None:
    project_path = tmp_path / "my_data_context"
    project_path.mkdir()

    # Clear datasources to improve test performance in DataContext._init_datasources
    data_context_config_with_datasources.datasources = {}

    context = BaseDataContext(
        project_config=data_context_config_with_datasources,
        context_root_dir=str(project_path),
        ge_cloud_mode=True,
        ge_cloud_config=ge_cloud_config,
    )

    assert isinstance(context._datasource_store.store_backend, GeCloudStoreBackend)


@pytest.mark.integration
def test_data_context_instantiates_inline_store_backend_with_filesystem_config(
    tmp_path: pathlib,
    data_context_config_with_datasources: DataContextConfig,
) -> None:
    project_path = tmp_path / "my_data_context"
    project_path.mkdir()

    # Clear datasources to improve test performance in DataContext._init_datasources
    data_context_config_with_datasources.datasources = {}

    context = BaseDataContext(
        project_config=data_context_config_with_datasources,
        context_root_dir=str(project_path),
        ge_cloud_mode=False,
    )

    assert isinstance(context._datasource_store.store_backend, InlineStoreBackend)
