from typing import Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class DataConnectorAnonymizer(BaseAnonymizer):
    def anonymize(self, obj: Optional[object] = None, **kwargs) -> Any:
        name: str = kwargs["name"]
        config: dict = kwargs["config"]

        anonymized_info_dict = {
            "anonymized_name": self._anonymize_string(name),
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        from great_expectations.data_context.types.base import (
            DataConnectorConfig,
            dataConnectorConfigSchema,
        )

        data_connector_config: DataConnectorConfig = dataConnectorConfigSchema.load(
            config
        )
        data_connector_config_dict: dict = dataConnectorConfigSchema.dump(
            data_connector_config
        )

        self._anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=data_connector_config_dict,
        )

        return anonymized_info_dict

    @staticmethod
    def can_handle(obj: Optional[object] = None, **kwargs) -> bool:
        from great_expectations.datasource.data_connector.data_connector import (
            DataConnector,
        )

        return obj is not None and isinstance(obj, DataConnector)
