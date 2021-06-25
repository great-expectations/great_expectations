from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.data_connector_anonymizer import (
    DataConnectorAnonymizer,
)
from great_expectations.core.usage_statistics.anonymizers.execution_engine_anonymizer import (
    ExecutionEngineAnonymizer,
)
from great_expectations.datasource import (
    Datasource,
    LegacyDatasource,
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)


class DatasourceAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._legacy_ge_classes = [
            PandasDatasource,
            SqlAlchemyDatasource,
            SparkDFDatasource,
            LegacyDatasource,
        ]

        self._ge_classes = [Datasource]
        self._execution_engine_anonymizer = ExecutionEngineAnonymizer(salt=salt)
        self._data_connector_anonymizer = DataConnectorAnonymizer(salt=salt)

    def anonymize_datasource_info(self, name, config):
        anonymized_info_dict = dict()
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        # Legacy Datasources (<= v0.12)
        if config.get("class_name") in [lc.__name__ for lc in self._legacy_ge_classes]:
            self.anonymize_object_info(
                anonymized_info_dict=anonymized_info_dict,
                ge_classes=self._legacy_ge_classes,
                object_config=config,
            )
        # Datasources (>= v0.13)
        elif config.get("class_name") in [c.__name__ for c in self._ge_classes]:
            self.anonymize_object_info(
                anonymized_info_dict=anonymized_info_dict,
                ge_classes=self._ge_classes,
                object_config=config,
            )
            execution_engine_config = config.get("execution_engine")
            anonymized_info_dict[
                "anonymized_execution_engine"
            ] = self._execution_engine_anonymizer.anonymize_execution_engine_info(
                name=execution_engine_config.get("name", ""),
                config=execution_engine_config,
            )
            data_connector_configs = config.get("data_connectors")
            anonymized_info_dict["anonymized_data_connectors"] = [
                self._data_connector_anonymizer.anonymize_data_connector_info(
                    name=data_connector_name, config=data_connector_config
                )
                for data_connector_name, data_connector_config in data_connector_configs.items()
            ]

        return anonymized_info_dict
