from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource.data_connector import (
    DataConnector,  # isort:skip
    RuntimeDataConnector,  # isort:skip
    FilePathDataConnector,  # isort:skip
    ConfiguredAssetFilePathDataConnector,  # isort:skip
    InferredAssetFilePathDataConnector,  # isort:skip
    ConfiguredAssetFilesystemDataConnector,  # isort:skip
    InferredAssetFilesystemDataConnector,  # isort:skip
    ConfiguredAssetS3DataConnector,  # isort:skip
    InferredAssetS3DataConnector,  # isort:skip
    ConfiguredAssetAzureDataConnector,  # isort:skip
    InferredAssetAzureDataConnector,  # isort:skip
    ConfiguredAssetGCSDataConnector,  # isort:skip
    InferredAssetGCSDataConnector,  # isort:skip
    ConfiguredAssetSqlDataConnector,  # isort:skip
    InferredAssetSqlDataConnector,  # isort:skip
)


class DataConnectorAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # This list should contain all DataConnector types. When new DataConnector types
        # are created, please make sure to add ordered bottom up in terms of inheritance order
        self._ge_classes = [
            ConfiguredAssetSqlDataConnector,
            InferredAssetSqlDataConnector,
            ConfiguredAssetGCSDataConnector,
            InferredAssetGCSDataConnector,
            ConfiguredAssetAzureDataConnector,
            InferredAssetAzureDataConnector,
            ConfiguredAssetS3DataConnector,
            InferredAssetS3DataConnector,
            ConfiguredAssetFilesystemDataConnector,
            InferredAssetFilesystemDataConnector,
            ConfiguredAssetFilePathDataConnector,
            InferredAssetFilePathDataConnector,
            FilePathDataConnector,
            RuntimeDataConnector,
            DataConnector,
        ]

    def anonymize_data_connector_info(self, name, config):
        anonymized_info_dict = {}
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
