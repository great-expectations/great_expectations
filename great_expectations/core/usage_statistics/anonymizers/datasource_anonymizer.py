from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.data_connector_anonymizer import (
    DataConnectorAnonymizer,
)
from great_expectations.datasource import (
    BaseDatasource,
    Datasource,
    LegacyDatasource,
    PandasDatasource,
    SimpleSqlalchemyDatasource,
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

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            SimpleSqlalchemyDatasource,
            Datasource,
            BaseDatasource,
        ]

        self._data_connector_anonymizer = DataConnectorAnonymizer(salt=salt)

    def anonymize_datasource_info(self, name, config):
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        # Legacy Datasources (<= v0.12 v2 BatchKwargs API)
        if self.get_parent_class_v2_api(config=config) is not None:
            self.anonymize_object_info(
                anonymized_info_dict=anonymized_info_dict,
                object_config=config,
            )
        # Datasources (>= v0.13 v3 BatchRequest API), and custom v2 BatchKwargs API
        elif self.get_parent_class_v3_api(config=config) is not None:
            self.anonymize_object_info(
                anonymized_info_dict=anonymized_info_dict,
                object_config=config,
            )
            execution_engine_config = config.get("execution_engine")
            anonymized_info_dict[
                "anonymized_execution_engine"
            ] = self.anonymize_execution_engine_info(
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

    def anonymize_simple_sqlalchemy_datasource(self, name, config):
        """
        SimpleSqlalchemyDatasource requires a separate anonymization scheme.
        """
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)
        if config.get("module_name") is None:
            config["module_name"] = "great_expectations.datasource"
        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=config,
        )

        # Only and directly provide parent_class of execution engine
        anonymized_info_dict["anonymized_execution_engine"] = {
            "parent_class": "SqlAlchemyExecutionEngine"
        }

        # Use the `introspection` and `tables` keys to find data_connectors in SimpleSqlalchemyDatasources
        introspection_data_connector_configs = config.get("introspection")
        tables_data_connector_configs = config.get("tables")

        introspection_data_connector_anonymized_configs = []
        if introspection_data_connector_configs is not None:
            for (
                data_connector_name,
                data_connector_config,
            ) in introspection_data_connector_configs.items():
                if data_connector_config.get("class_name") is None:
                    data_connector_config[
                        "class_name"
                    ] = "InferredAssetSqlDataConnector"
                if data_connector_config.get("module_name") is None:
                    data_connector_config[
                        "module_name"
                    ] = "great_expectations.datasource.data_connector"
                introspection_data_connector_anonymized_configs.append(
                    self._data_connector_anonymizer.anonymize_data_connector_info(
                        name=data_connector_name, config=data_connector_config
                    )
                )

        tables_data_connector_anonymized_configs = []
        if tables_data_connector_configs is not None:
            for (
                data_connector_name,
                data_connector_config,
            ) in tables_data_connector_configs.items():
                if data_connector_config.get("class_name") is None:
                    data_connector_config[
                        "class_name"
                    ] = "ConfiguredAssetSqlDataConnector"
                if data_connector_config.get("module_name") is None:
                    data_connector_config[
                        "module_name"
                    ] = "great_expectations.datasource.data_connector"
                tables_data_connector_anonymized_configs.append(
                    self._data_connector_anonymizer.anonymize_data_connector_info(
                        name=data_connector_name, config=data_connector_config
                    )
                )

        anonymized_info_dict["anonymized_data_connectors"] = (
            introspection_data_connector_anonymized_configs
            + tables_data_connector_anonymized_configs
        )

        return anonymized_info_dict

    def get_parent_class(self, config) -> Optional[str]:
        return super().get_parent_class(
            classes_to_check=self._ge_classes + self._legacy_ge_classes,
            object_config=config,
        )

    def get_parent_class_v2_api(self, config) -> Optional[str]:
        return super().get_parent_class(
            classes_to_check=self._legacy_ge_classes,
            object_config=config,
        )

    def get_parent_class_v3_api(self, config) -> Optional[str]:
        return super().get_parent_class(
            classes_to_check=self._ge_classes,
            object_config=config,
        )
