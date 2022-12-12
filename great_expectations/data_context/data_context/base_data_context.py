from __future__ import annotations

import os
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GXCloudConfig


def BaseDataContext(
    project_config: Union[DataContextConfig, Mapping],
    context_root_dir: Optional[str] = None,
    runtime_environment: Optional[dict] = None,
    cloud_mode: bool = False,
    cloud_config: Optional[GXCloudConfig] = None,
    # Deprecated as of 0.15.37
    ge_cloud_mode: bool = False,
    ge_cloud_config: Optional[GXCloudConfig] = None,
) -> AbstractDataContext:
    # Chetan - 20221208 - not formally deprecating these values until a future date
    cloud_config = cloud_config if cloud_config is not None else ge_cloud_config
    cloud_mode = True if cloud_mode or ge_cloud_mode else False

    project_data_context_config: DataContextConfig = (
        AbstractDataContext.get_or_create_data_context_config(project_config)
    )

    if context_root_dir is not None:
        context_root_dir = os.path.abspath(context_root_dir)
    # initialize runtime_environment as empty dict if None
    runtime_environment = runtime_environment or {}

    if ge_cloud_mode:
        cloud_base_url: Optional[str] = None
        cloud_access_token: Optional[str] = None
        cloud_organization_id: Optional[str] = None
        if cloud_config:
            cloud_base_url = cloud_config.base_url
            cloud_access_token = cloud_config.access_token
            cloud_organization_id = cloud_config.organization_id
        return CloudDataContext(
            project_config=project_data_context_config,
            runtime_environment=runtime_environment,
            context_root_dir=context_root_dir,
            cloud_base_url=cloud_base_url,
            cloud_access_token=cloud_access_token,
            cloud_organization_id=cloud_organization_id,
        )
    elif context_root_dir:
        return FileDataContext(  # type: ignore[assignment]
            project_config=project_data_context_config,
            context_root_dir=context_root_dir,  # type: ignore[arg-type]
            runtime_environment=runtime_environment,
        )
    else:
        return EphemeralDataContext(  # type: ignore[assignment]
            project_config=project_data_context_config,
            runtime_environment=runtime_environment,
        )
