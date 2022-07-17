import random
import uuid
from typing import Optional, Union

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig


class ProfilerStore(ConfigurationStore):
    """
    A ProfilerStore manages Profilers for the DataContext.
    """

    _configuration_class = RuleBasedProfilerConfig

    def serialization_self_check(self, pretty_print: bool) -> None:
        """
        Fufills the abstract method defined by the parent class.
        See `ConfigurationStore` for more details.
        """
        test_profiler_name = f"profiler_{''.join([random.choice(list('0123456789ABCDEF')) for _ in range(20)])}"
        test_profiler_configuration = RuleBasedProfilerConfig(
            name=test_profiler_name,
            config_version=1.0,
            rules={},
        )

        test_key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if self.ge_cloud_mode:
            test_key = self.key_class(
                resource_type=GeCloudRESTResource.PROFILER,
                ge_cloud_id=str(uuid.uuid4()),
            )
        else:
            test_key = self.key_class(configuration_key=test_profiler_name)

        if pretty_print:
            print(f"Attempting to add a new test key {test_key} to Profiler store...")

        self.set(key=test_key, value=test_profiler_configuration)
        if pretty_print:
            print(f"\tTest key {test_key} successfully added to Profiler store.\n")
            print(
                f"Attempting to retrieve the test value associated with key {test_key} from Profiler store..."
            )

        test_value = self.get(key=test_key)
        if pretty_print:
            print(
                f"\tTest value successfully retrieved from Profiler store: {test_value}\n"
            )
            print(f"Cleaning up test key {test_key} and value from Profiler store...")

        test_value = self.remove_key(key=test_key)
        if pretty_print:
            print(
                f"\tTest key and value successfully removed from Profiler store: {test_value}\n"
            )

    def ge_cloud_response_json_to_object_dict(self, response_json: dict) -> dict:
        """
        This method takes full json response from GE cloud and outputs a dict appropriate for
        deserialization into a GE object
        """
        ge_cloud_profiler_id = response_json["data"]["id"]
        profiler_config_dict = response_json["data"]["attributes"]["profiler"]
        profiler_config_dict["ge_cloud_id"] = ge_cloud_profiler_id

        return profiler_config_dict
