import os
import uuid
from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.util import batch_request_contains_batch_data
from great_expectations.data_context.store import ProfilerStore
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.types.refs import GeCloudIdAwareRef
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig
from great_expectations.util import filter_properties_dict


def add_profiler(
    config: RuleBasedProfilerConfig,
    data_context: "DataContext",  # noqa: F821
    ge_cloud_id: Optional[str] = None,
) -> RuleBasedProfiler:
    _check_validity_of_batch_requests_in_config(
        config, data_context.profiler_store_name
    )

    profiler_config: dict = config.to_json_dict()

    new_profiler = instantiate_class_from_config(
        config=profiler_config,
        runtime_environment={"data_context": data_context},
        config_defaults={"module_name": "great_expectations.rule_based_profiler"},
    )

    key: Union[GeCloudIdentifier, ConfigurationIdentifier]
    if data_context.ge_cloud_mode and ge_cloud_id:
        key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
    else:
        key = ConfigurationIdentifier(
            configuration_key=config.name,
        )

    profiler_store = data_context.profiler_store
    profiler_ref = profiler_store.set(key=key, value=config)
    if isinstance(profiler_ref, GeCloudIdAwareRef):
        pass
        # Chetan - 20220201 - Open to determine how to treat ge_cloud_id
        # ge_cloud_id = profiler_ref.ge_cloud_id
        # new_profiler.ge_cloud_id = uuid.UUID(ge_cloud_id)

    return new_profiler


def _check_validity_of_batch_requests_in_config(
    config: RuleBasedProfilerConfig, profiler_store_name: str
) -> None:
    # Evaluate nested types in RuleConfig to parse out BatchRequests
    batch_requests = []
    for rule in config.rules.values():

        domain_builder = rule["domain_builder"]
        if "batch_request" in domain_builder:
            batch_requests.append(domain_builder["batch_request"])

        parameter_builders = rule.get("parameter_builders", [])
        for parameter_builder in parameter_builders:
            if "batch_request" in parameter_builder:
                batch_requests.append(parameter_builder["batch_request"])

    # DataFrames shouldn't be saved to ProfilerStore
    for batch_request in batch_requests:
        if batch_request_contains_batch_data(batch_request=batch_request):
            raise ge_exceptions.InvalidConfigError(
                f'batch_data found in batch_request cannot be saved to ProfilerStore "{profiler_store_name}"'
            )


def get_profiler(
    data_context: "DataContext",
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
