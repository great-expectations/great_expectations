import os
from typing import Any, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.util import batch_request_contains_batch_data
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.store import ProfilerStore
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
)


def add_profiler(
    data_context: "DataContext",  # noqa: F821
    config: RuleBasedProfilerConfig,
    ge_cloud_id: Optional[str] = None,
) -> RuleBasedProfiler:

    _check_validity_of_batch_requests_in_config(
        config, data_context.profiler_store_name
    )

    cleaned_config = deep_filter_properties_iterable(
        properties=config.to_json_dict(),
        clean_falsy=True,
        keep_falsy_numerics=True,
    )

    new_profiler = instantiate_class_from_config(
        config=cleaned_config,
        runtime_environment={"data_context": data_context},
        config_defaults={"module_name": "great_expectations.rule_based_profiler"},
    )

    key: Union[GeCloudIdentifier, ConfigurationIdentifier]
    if data_context.ge_cloud_mode:
        key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
    else:
        key = ConfigurationIdentifier(
            configuration_key=config.name,
        )

    return new_profiler


def _check_validity_of_batch_requests_in_config(
    config: RuleBasedProfilerConfig, profiler_store_name: str
) -> None:
    json_dict = config.to_raw_dict()

    # Recursive helper to walk nested config
    def _traverse(node: Any) -> None:
        if not isinstance(node, dict):
            return
        for k, v in node.items():
            # DataFrames shouldn't be saved to ProfilerStore
            if k == "batch_request" and batch_request_contains_batch_data(
                batch_request=v
            ):
                raise ge_exceptions.InvalidConfigError(
                    f'batch_data found in batch_request cannot be saved to ProfilerStore "{profiler_store_name}"'
                )
            _traverse(v)

    # Trigger checks using top level config as root node
    _traverse(json_dict)


def get_profiler(
    data_context: "DataContext",
    profiler_store: ProfilerStore,
    name: Optional[str] = None,
    ge_cloud_id: Optional[str] = None,
) -> RuleBasedProfiler:
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
