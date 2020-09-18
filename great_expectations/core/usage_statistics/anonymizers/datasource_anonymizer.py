from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource import (
    Datasource,
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)


class DatasourceAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            PandasDatasource,
            SqlAlchemyDatasource,
            SparkDFDatasource,
            Datasource,
        ]

    def anonymize_datasource_info(self, name, config):
        anonymized_info_dict = dict()
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            object_config=config,
        )

        return anonymized_info_dict


class ExecutionEngineAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            PandasExecutionEngine,
            SqlAlchemyExecutionEngine,
            # SparkExecutionEngine,
            ExecutionEngine,
        ]

    def anonymize_execution_engine_info(self, name, config):
        anonymized_info_dict = dict()
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            object_config=config,
        )

        return anonymized_info_dict
