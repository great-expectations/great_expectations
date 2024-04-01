from __future__ import annotations

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig


class ProfilerStore(ConfigurationStore):
    """
    A ProfilerStore manages Profilers for the DataContext.
    """

    _configuration_class = RuleBasedProfilerConfig

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: dict) -> dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        ge_cloud_profiler_id = response_json["data"]["id"]
        profiler_config_dict = response_json["data"]["attributes"]["profiler"]
        profiler_config_dict["id"] = ge_cloud_profiler_id

        return profiler_config_dict

    def _add(self, key, value, **kwargs):
        try:
            return super()._add(key=key, value=value, **kwargs)
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.ProfilerError(f"A Profiler named {value.name} already exists.")  # noqa: TRY003

    def _update(self, key, value, **kwargs):
        try:
            return super()._update(key=key, value=value, **kwargs)
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.ProfilerNotFoundError(  # noqa: TRY003
                f"Could not find an existing Profiler named {value.name}."
            )
