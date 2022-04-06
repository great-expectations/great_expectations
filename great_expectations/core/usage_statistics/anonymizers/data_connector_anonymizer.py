from typing import Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class DataConnectorAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(
        self, name: str, config: dict, obj: Optional[object] = None, **kwargs
    ) -> Any:
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

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        from great_expectations.datasource.data_connector.data_connector import (
            DataConnector,
        )

        return (obj is not None and isinstance(obj, DataConnector)) or (
            "name" and kwargs and "config" in kwargs
        )
