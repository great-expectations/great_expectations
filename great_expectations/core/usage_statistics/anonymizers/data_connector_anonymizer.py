from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
    ConfiguredAssetFilesystemDataConnector,
    ConfiguredAssetS3DataConnector,
    ConfiguredAssetSqlDataConnector,
    DataConnector,
    FilePathDataConnector,
    InferredAssetFilePathDataConnector,
    InferredAssetFilesystemDataConnector,
    InferredAssetS3DataConnector,
    InferredAssetSqlDataConnector,
    RuntimeDataConnector,
)


class DataConnectorAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            InferredAssetS3DataConnector,
            InferredAssetFilesystemDataConnector,
            InferredAssetFilePathDataConnector,
            InferredAssetSqlDataConnector,
            ConfiguredAssetS3DataConnector,
            ConfiguredAssetFilesystemDataConnector,
            ConfiguredAssetFilePathDataConnector,
            ConfiguredAssetSqlDataConnector,
            RuntimeDataConnector,
            FilePathDataConnector,
            DataConnector,
        ]

    def anonymize_data_connector_info(self, name, config):
        anonymized_info_dict = dict()
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            object_config=config,
        )

        return anonymized_info_dict
