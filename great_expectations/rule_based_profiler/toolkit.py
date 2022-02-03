import os
from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store import ProfilerStore
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.types import RuleBasedProfilerConfig
from great_expectations.util import filter_properties_dict


def get_profiler(
    data_context: "DataContext",  # noqa: F821
    profiler_store: ProfilerStore,
    name: Optional[str] = None,
    ge_cloud_id: Optional[str] = None,
) -> RuleBasedProfiler:
    assert bool(name) ^ bool(
        ge_cloud_id
    ), "Must provide either name or ge_cloud_id (but not both)"

    key: Union[GeCloudIdentifier, ConfigurationIdentifier]
    if ge_cloud_id:
        key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
    else:
        key = ConfigurationIdentifier(
            configuration_key=name,
        )
    try:
        profiler_config: RuleBasedProfilerConfig = profiler_store.get(key=key)
    except ge_exceptions.InvalidKeyError as exc_ik:
        id_ = key.configuration_key if isinstance(key, ConfigurationIdentifier) else key
        raise ge_exceptions.ProfilerNotFoundError(
            message=f'Non-existent Profiler configuration named "{id_}".\n\nDetails: {exc_ik}'
        )

    config = profiler_config.to_json_dict()
    if name:
        config.update({"name": name})
    config = filter_properties_dict(properties=config, clean_falsy=True)

    profiler = instantiate_class_from_config(
        config=config,
        runtime_environment={
            "data_context": data_context,
        },
        config_defaults={
            "module_name": "great_expectations.rule_based_profiler",
        },
    )

    return profiler


def delete_profiler(
    profiler_store: ProfilerStore,
    name: Optional[str] = None,
    ge_cloud_id: Optional[str] = None,
) -> None:
    assert bool(name) ^ bool(
        ge_cloud_id
    ), "Must provide either name or ge_cloud_id (but not both)"

    key: Union[GeCloudIdentifier, ConfigurationIdentifier]
    if ge_cloud_id:
        key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
    else:
        key = ConfigurationIdentifier(configuration_key=name)

    try:
        profiler_store.remove_key(key=key)
    except (ge_exceptions.InvalidKeyError, KeyError) as exc_ik:
        id_ = key.configuration_key if isinstance(key, ConfigurationIdentifier) else key
        raise ge_exceptions.ProfilerNotFoundError(
            message=f'Non-existent Profiler configuration named "{id_}".\n\nDetails: {exc_ik}'
        )


def list_profilers(
    profiler_store: ProfilerStore,
    ge_cloud_mode: bool,
) -> List[str]:
    if ge_cloud_mode:
        return profiler_store.list_keys()
    return [x.configuration_key for x in profiler_store.list_keys()]


def default_profilers_exist(directory_path: Optional[str]) -> bool:
    if not directory_path:
        return False

    profiler_directory_path: str = os.path.join(
        directory_path,
        DataContextConfigDefaults.DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
    )
    return os.path.isdir(profiler_directory_path)
