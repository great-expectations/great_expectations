# isort:skip_file

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.data_context.types.base import (
    DataConnectorConfig,
    dataConnectorConfigSchema,
)


class DataConnectorAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

    def anonymize_data_connector_info(self, name, config):
        anonymized_info_dict = {
            "anonymized_name": self.anonymize(name),
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        data_connector_config: DataConnectorConfig = dataConnectorConfigSchema.load(
            config
        )
        data_connector_config_dict: dict = dataConnectorConfigSchema.dump(
            data_connector_config
        )

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=data_connector_config_dict,
        )

        return anonymized_info_dict
