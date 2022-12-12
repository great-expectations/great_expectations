from __future__ import annotations

import os
from typing import Optional

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.types.base import GXCloudConfig


def DataContext(
    context_root_dir: Optional[str] = None,
    runtime_environment: Optional[dict] = None,
    cloud_mode: bool = False,
    cloud_base_url: Optional[str] = None,
    cloud_access_token: Optional[str] = None,
    cloud_organization_id: Optional[str] = None,
    # Deprecated as of 0.15.37
    ge_cloud_mode: bool = False,
    ge_cloud_base_url: Optional[str] = None,
    ge_cloud_access_token: Optional[str] = None,
    ge_cloud_organization_id: Optional[str] = None,
) -> AbstractDataContext:
    # Chetan - 20221208 - not formally deprecating these values until a future date
    cloud_base_url = cloud_base_url if cloud_base_url is not None else ge_cloud_base_url
    cloud_access_token = (
        cloud_access_token if cloud_access_token is not None else ge_cloud_access_token
    )
    cloud_organization_id = (
        cloud_organization_id
        if cloud_organization_id is not None
        else ge_cloud_organization_id
    )
    cloud_mode = True if cloud_mode or ge_cloud_mode else False

    cloud_config = _init_cloud_config(
        cloud_mode=cloud_mode,
        cloud_base_url=cloud_base_url,
        cloud_access_token=cloud_access_token,
        cloud_organization_id=cloud_organization_id,
    )

    context_root_directory = _init_context_root_directory(
        cloud_mode=cloud_mode,
        context_root_dir=context_root_dir,
    )

    project_config = SerializableDataContext._load_project_config(
        context_root_directory=context_root_directory,
        cloud_mode=cloud_mode,
        cloud_config=cloud_config,
    )

    context = BaseDataContext(
        project_config=project_config,
        context_root_dir=context_root_directory,
        runtime_environment=runtime_environment,
        cloud_mode=cloud_mode,
        cloud_config=cloud_config,
    )

    return context

    # # Save project config if data_context_id auto-generated
    # if context._check_for_usage_stats_sync(project_config):
    #     context._save_project_config()


def _init_cloud_config(
    cloud_mode: bool,
    cloud_base_url: Optional[str],
    cloud_access_token: Optional[str],
    cloud_organization_id: Optional[str],
) -> Optional[GXCloudConfig]:
    if not cloud_mode:
        return None

    cloud_config = CloudDataContext.get_cloud_config(
        cloud_base_url=cloud_base_url,
        cloud_access_token=cloud_access_token,
        cloud_organization_id=cloud_organization_id,
    )
    return cloud_config


def _init_context_root_directory(
    cloud_mode: bool, context_root_dir: Optional[str]
) -> str:
    if cloud_mode and context_root_dir is None:
        context_root_dir = CloudDataContext.determine_context_root_directory(
            context_root_dir
        )
    else:
        context_root_dir = (
            SerializableDataContext.find_context_root_dir()
            if context_root_dir is None
            else context_root_dir
        )

    return os.path.abspath(os.path.expanduser(context_root_dir))
