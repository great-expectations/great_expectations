from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource.data_connector import (
    ConfiguredAssetAzureDataConnector,
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

        # This list should contain all DataConnector types. When new DataConnector types
        # are created, please make sure to add ordered bottom up in terms of inheritance order
        self._ge_classes = [
            InferredAssetS3DataConnector,
            InferredAssetFilesystemDataConnector,
            InferredAssetFilePathDataConnector,
            InferredAssetSqlDataConnector,
            ConfiguredAssetS3DataConnector,
            ConfiguredAssetAzureDataConnector,
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

    def is_parent_class_recognized(self, config):
        return self._is_parent_class_recognized(
            classes_to_check=self._ge_classes,
            object_config=config,
        )
