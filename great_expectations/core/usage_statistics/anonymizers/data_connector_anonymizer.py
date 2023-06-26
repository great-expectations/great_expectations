from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

if TYPE_CHECKING:
    from great_expectations.core.usage_statistics.anonymizers.anonymizer import (
        Anonymizer,
    )


class DataConnectorAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: Anonymizer,
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(  # type: ignore[override] # differs from parent class
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

        is_data_connector = obj is not None and isinstance(obj, DataConnector)
        if kwargs:
            contains_data_connector = "name" in kwargs and "config" in kwargs
        else:
            contains_data_connector = False
        return is_data_connector or contains_data_connector
