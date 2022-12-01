import os
from typing import Optional

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GXCloudConfig

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


def DataContext(
    context_root_dir: Optional[str] = None,
    runtime_environment: Optional[dict] = None,
    ge_cloud_mode: bool = False,
    ge_cloud_base_url: Optional[str] = None,
    ge_cloud_access_token: Optional[str] = None,
    ge_cloud_organization_id: Optional[str] = None,
):
    ge_cloud_config = _init_ge_cloud_config(
        ge_cloud_mode=ge_cloud_mode,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_access_token=ge_cloud_access_token,
        ge_cloud_organization_id=ge_cloud_organization_id,
    )

    context_root_directory = _init_context_root_directory(
        context_root_dir=context_root_dir, ge_cloud_mode=ge_cloud_mode
    )

    project_config = _load_project_config(
        context_root_directory=context_root_directory,
        ge_cloud_mode=ge_cloud_mode,
        ge_cloud_config=ge_cloud_config,
    )

    return BaseDataContext(
        project_config=project_config,
        context_root_dir=context_root_directory,
        runtime_environment=runtime_environment,
        ge_cloud_mode=ge_cloud_mode,
        ge_cloud_config=ge_cloud_config,
    )


def _init_ge_cloud_config(
    ge_cloud_mode: bool,
    ge_cloud_base_url: Optional[str],
    ge_cloud_access_token: Optional[str],
    ge_cloud_organization_id: Optional[str],
) -> Optional[GXCloudConfig]:
    if not ge_cloud_mode:
        return None

    ge_cloud_config = CloudDataContext.get_ge_cloud_config(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_access_token=ge_cloud_access_token,
        ge_cloud_organization_id=ge_cloud_organization_id,
    )
    return ge_cloud_config


def _init_context_root_directory(
    context_root_dir: Optional[str], ge_cloud_mode: bool
) -> str:
    if ge_cloud_mode and context_root_dir is None:
        context_root_dir = CloudDataContext.determine_context_root_directory(
            context_root_dir
        )
    else:
        context_root_dir = (
            FileDataContext.find_context_root_dir()
            if context_root_dir is None
            else context_root_dir
        )

    return os.path.abspath(os.path.expanduser(context_root_dir))


def _load_project_config(
    context_root_directory: str,
    ge_cloud_mode: bool,
    ge_cloud_config: Optional[GXCloudConfig],
):
    if ge_cloud_mode:
        ge_cloud_config = ge_cloud_config
        assert ge_cloud_config is not None
        config = CloudDataContext.retrieve_data_context_config_from_ge_cloud(
            ge_cloud_config=ge_cloud_config
        )
        return config

    path_to_yml = os.path.join(context_root_directory, FileDataContext.GE_YML)
    try:
        with open(path_to_yml) as data:
            config_commented_map_from_yaml = yaml.load(data)

    except DuplicateKeyError:
        raise ge_exceptions.InvalidConfigurationYamlError(
            "Error: duplicate key found in project YAML file."
        )
    except YAMLError as err:
        raise ge_exceptions.InvalidConfigurationYamlError(
            "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                err
            )
        )
    except OSError:
        raise ge_exceptions.ConfigNotFoundError()

    try:
        return DataContextConfig.from_commented_map(
            commented_map=config_commented_map_from_yaml
        )
    except ge_exceptions.InvalidDataContextConfigError:
        # Just to be explicit about what we intended to catch
        raise
