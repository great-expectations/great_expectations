from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource.data_connector import (
    ConfiguredAssetAzureDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    ConfiguredAssetGCSDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    ConfiguredAssetS3DataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    ConfiguredAssetSqlDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    FilePathDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetAzureDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetFilePathDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetFilesystemDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetGCSDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetS3DataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    InferredAssetSqlDataConnector,  # isort:skip
)
from great_expectations.datasource.data_connector import (
    RuntimeDataConnector,  # isort:skip
)

from great_expectations.datasource.data_connector import DataConnector  # isort:skip


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
