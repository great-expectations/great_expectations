from __future__ import annotations

import os
from typing import TYPE_CHECKING, Literal, Optional, Tuple, overload

from great_expectations.core._docs_decorators import deprecated_argument
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,  # noqa: TCH001
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.types.base import GXCloudConfig  # noqa: TCH001

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr


@overload
def DataContext(  # noqa: PLR0913
    context_root_dir: PathStr = ...,
    runtime_environment: Optional[dict] = ...,
    cloud_mode: Literal[False] = ...,
    cloud_base_url: None = ...,
    cloud_access_token: None = ...,
    cloud_organization_id: None = ...,
    # Deprecated as of 0.15.37
    ge_cloud_mode: Literal[False] = ...,
    ge_cloud_base_url: None = ...,
    ge_cloud_access_token: None = ...,
    ge_cloud_organization_id: None = ...,
) -> FileDataContext:
    # If `context_root_dir` is provided and `cloud_mode`/`ge_cloud_mode` are `False` a `FileDataContext` will always be returned.
    ...


@overload
def DataContext(  # noqa: PLR0913
    context_root_dir: Optional[PathStr] = ...,
    runtime_environment: Optional[dict] = ...,
    cloud_mode: bool = ...,
    cloud_base_url: Optional[str] = ...,
    cloud_access_token: Optional[str] = ...,
    cloud_organization_id: Optional[str] = ...,
    # <GX_RENAME> Deprecated as of 0.15.37
    ge_cloud_mode: bool = ...,
    ge_cloud_base_url: Optional[str] = ...,
    ge_cloud_access_token: Optional[str] = ...,
    ge_cloud_organization_id: Optional[str] = ...,
) -> AbstractDataContext:
    ...


# TODO: add additional overloads


@deprecated_argument(argument_name="ge_cloud_mode", version="0.15.37")
@deprecated_argument(argument_name="ge_cloud_base_url", version="0.15.37")
@deprecated_argument(argument_name="ge_cloud_access_token", version="0.15.37")
@deprecated_argument(argument_name="ge_cloud_organization_id", version="0.15.37")
def DataContext(  # noqa: PLR0913
    context_root_dir: Optional[PathStr] = None,
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
    """A DataContext represents a Great Expectations project.

    It is the primary entry point for a Great Expectations deployment, with configurations and methods for all
    supporting components. The DataContext is configured via a yml file stored in a directory called great_expectations;
    this configuration file as well as managed Expectation Suites should be stored in version control. There are other
    ways to create a Data Context that may be better suited for your particular deployment e.g. ephemerally or backed by
    GX Cloud (coming soon). Please refer to our documentation for more details.

    You can Validate data or generate Expectations using Execution Engines including:
     * SQL (multiple dialects supported)
     * Spark
     * Pandas

    Your data can be stored in common locations including:
     * databases / data warehouses
     * files in s3, GCS, Azure, local storage
     * dataframes (spark and pandas) loaded into memory

    Please see our documentation for examples on how to set up Great Expectations, connect to your data,
    create Expectations, and Validate data.

    Other configuration options you can apply to a DataContext besides how to access data include things like where to
    store Expectations, Profilers, Checkpoints, Metrics, Validation Results and Data Docs and how those Stores are
    configured. Take a look at our documentation for more configuration options.

    --Public API--

    --Documentation--
        - https://docs.greatexpectations.io/docs/terms/data_context

    Args:
        context_root_dir: Path to directory that contains your data context related files
        runtime_environment: A dictionary containing relevant runtime information (like class_name and module_name)
        cloud_mode: Whether to run GX in Cloud mode (default is None).
            If None, cloud mode is assumed if Cloud credentials are set up. Set to False to override.
        cloud_base_url: Your cloud base url. Optional, you may provide this alternatively via
                environment variable GX_CLOUD_BASE_URL or within a config file.
        cloud_access_token: Your cloud access token. Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ACCESS_TOKEN or within a config file.
        cloud_organization_id: Your cloud organization ID. Optional, you may provide this alternatively
                via environment variable GX_CLOUD_ORGANIZATION_ID or within a config file.
        ge_cloud_mode: The deprecated cloud mode
        ge_cloud_base_url: Your deprecated cloud base url
        ge_cloud_access_token: Your deprecated cloud access token
        ge_cloud_organization_id: Your deprecated cloud organization ID

    Returns:
        context
    """
    # Chetan - 20221208 - not formally deprecating these values until a future date
    (
        cloud_base_url,
        cloud_access_token,
        cloud_organization_id,
        cloud_mode,
    ) = _resolve_cloud_args(
        cloud_mode=cloud_mode,
        cloud_base_url=cloud_base_url,
        cloud_access_token=cloud_access_token,
        cloud_organization_id=cloud_organization_id,
        ge_cloud_mode=ge_cloud_mode,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_access_token=ge_cloud_access_token,
        ge_cloud_organization_id=ge_cloud_organization_id,
    )

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

    if cloud_mode:
        project_config = CloudDataContext._load_cloud_backed_project_config(
            cloud_config=cloud_config,
        )
    else:
        project_config = FileDataContext._load_file_backed_project_config(
            context_root_directory=context_root_directory,
        )

    context = BaseDataContext(
        project_config=project_config,
        context_root_dir=context_root_directory,
        runtime_environment=runtime_environment,
        cloud_mode=cloud_mode,
        cloud_config=cloud_config,
    )

    # # Save project config if data_context_id auto-generated
    if isinstance(
        context, SerializableDataContext
    ) and context._check_for_usage_stats_sync(project_config):
        context._save_project_config()

    return context


def _resolve_cloud_args(  # noqa: PLR0913
    cloud_mode: bool = False,
    cloud_base_url: Optional[str] = None,
    cloud_access_token: Optional[str] = None,
    cloud_organization_id: Optional[str] = None,
    # <GX_RENAME> Deprecated as of 0.15.37
    ge_cloud_mode: bool = False,
    ge_cloud_base_url: Optional[str] = None,
    ge_cloud_access_token: Optional[str] = None,
    ge_cloud_organization_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
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
    return cloud_base_url, cloud_access_token, cloud_organization_id, cloud_mode


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
    cloud_mode: bool, context_root_dir: Optional[PathStr]
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

    return os.path.abspath(os.path.expanduser(context_root_dir))  # noqa: PTH111, PTH100
