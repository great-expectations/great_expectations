from __future__ import annotations

import datetime
import hashlib
import platform
import sys
import uuid
from typing import TYPE_CHECKING

from great_expectations.core.usage_statistics.execution_environment import (
    GXExecutionEnvironment,
    PackageInfo,
    PackageInfoSchema,
)

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context import AbstractDataContext


class UsageStatisticsPayloadBuilder:
    """
    Responsible for encapsulating payload construction logic.
    """

    def __init__(
        self,
        data_context: AbstractDataContext,
        data_context_id: str,
        oss_id: uuid.UUID | None,
        gx_version: str,
    ) -> None:
        self._data_context = data_context
        self._data_context_id = data_context_id
        self._data_context_instance_id = data_context.instance_id
        self._oss_id = oss_id
        self._gx_version = gx_version

    def build_init_payload(self) -> dict:
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        expectation_suites: list[ExpectationSuite] = [
            self._data_context.get_expectation_suite(expectation_suite_name)
            for expectation_suite_name in self._data_context.list_expectation_suite_names()
        ]

        # <WILL> 20220701 - ValidationOperators have been deprecated, so some init_payloads will not have them included
        validation_operators = None
        if hasattr(self._data_context, "validation_operators"):
            validation_operators = self._data_context.validation_operators

        init_payload = {
            "platform.system": platform.system(),
            "platform.release": platform.release(),
            "version_info": str(sys.version_info),
            "datasources": self._data_context.project_config_with_variables_substituted.datasources,
            "stores": self._data_context.stores,
            "validation_operators": validation_operators,
            "data_docs_sites": self._data_context.project_config_with_variables_substituted.data_docs_sites,
            "expectation_suites": expectation_suites,
            "dependencies": self._get_serialized_dependencies(),
        }

        return init_payload

    @staticmethod
    def _get_serialized_dependencies() -> list[dict]:
        """Get the serialized dependencies from the GXExecutionEnvironment."""
        ge_execution_environment = GXExecutionEnvironment()
        dependencies: list[PackageInfo] = ge_execution_environment.dependencies

        schema = PackageInfoSchema()

        serialized_dependencies: list[dict] = [
            schema.dump(package_info) for package_info in dependencies
        ]

        return serialized_dependencies

    def build_envelope(self, message: dict) -> dict:
        message["version"] = "2"  # Not actually being utilized by analytics
        message["ge_version"] = self._gx_version

        message["data_context_id"] = self._data_context_id
        message["data_context_instance_id"] = self._data_context_instance_id

        message["mac_address"] = self._determine_hashed_mac_address()
        message["oss_id"] = str(self._oss_id) if self._oss_id else None

        message["event_time"] = (
            datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z"
        )

        event_duration_property_name: str = f'{message["event"]}.duration'.replace(
            ".", "_"
        )
        if hasattr(self, event_duration_property_name):
            delta_t: int = getattr(self, event_duration_property_name)
            message["event_duration"] = delta_t

        return message

    @staticmethod
    def _determine_hashed_mac_address() -> str:
        address = uuid.UUID(int=uuid.getnode())
        hashed_address = hashlib.sha256(address.bytes)
        return hashed_address.hexdigest()
