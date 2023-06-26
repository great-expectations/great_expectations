from typing import Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.datasource import (
    BaseDatasource,
    Datasource,
    SimpleSqlalchemyDatasource,
)


class DatasourceAnonymizer(BaseAnonymizer):
    # ordered bottom up in terms of inheritance order
    _ge_classes = [
        SimpleSqlalchemyDatasource,
        Datasource,
        BaseDatasource,
    ]

    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(self, obj: Optional[object] = None, *args, **kwargs) -> Any:
        if obj is not None and isinstance(obj, SimpleSqlalchemyDatasource):
            return self._anonymize_simple_sqlalchemy_datasource(*args, **kwargs)
        return self._anonymize_datasource_info(*args, **kwargs)

    def _anonymize_datasource_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(name)

        # Datasources (>= v0.13 v3 BatchRequest API)
        if self.get_parent_class_v3_api(config=config) is not None:
            self._anonymize_object_info(
                anonymized_info_dict=anonymized_info_dict,
                object_config=config,
            )
            execution_engine_config = config.get("execution_engine")
            anonymized_info_dict[
                "anonymized_execution_engine"
            ] = self._anonymize_execution_engine_info(
                name=execution_engine_config.get("name", ""),
                config=execution_engine_config,
            )
            data_connector_configs = config.get("data_connectors")
            if data_connector_configs:
                anonymized_info_dict["anonymized_data_connectors"] = [
                    self._aggregate_anonymizer.anonymize(
                        name=data_connector_name, config=data_connector_config
                    )
                    for data_connector_name, data_connector_config in data_connector_configs.items()
                ]

        return anonymized_info_dict

    def _anonymize_simple_sqlalchemy_datasource(self, name: str, config: dict) -> dict:
        """
        SimpleSqlalchemyDatasource requires a separate anonymization scheme.
        """
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(name)
        if config.get("module_name") is None:
            config["module_name"] = "great_expectations.datasource"
        self._anonymize_object_info(
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
                    self._aggregate_anonymizer.anonymize(
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
                    self._aggregate_anonymizer.anonymize(
                        name=data_connector_name, config=data_connector_config
                    )
                )

        anonymized_info_dict["anonymized_data_connectors"] = (
            introspection_data_connector_anonymized_configs
            + tables_data_connector_anonymized_configs
        )

        return anonymized_info_dict

    def _anonymize_execution_engine_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(name)

        from great_expectations.data_context.types.base import (
            ExecutionEngineConfig,
            executionEngineConfigSchema,
        )

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        execution_engine_config: ExecutionEngineConfig = (
            executionEngineConfigSchema.load(config)
        )
        execution_engine_config_dict: dict = executionEngineConfigSchema.dump(
            execution_engine_config
        )

        self._anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=execution_engine_config_dict,
        )

        return anonymized_info_dict

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        return obj is not None and isinstance(obj, BaseDatasource)

    @staticmethod
    def get_parent_class(config: dict) -> Optional[str]:
        return BaseAnonymizer.get_parent_class(
            classes_to_check=DatasourceAnonymizer._ge_classes,
            object_config=config,
        )

    @staticmethod
    def get_parent_class_v3_api(config: dict) -> Optional[str]:
        return BaseAnonymizer.get_parent_class(
            classes_to_check=DatasourceAnonymizer._ge_classes,
            object_config=config,
        )
