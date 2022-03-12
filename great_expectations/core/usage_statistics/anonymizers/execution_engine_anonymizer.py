from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.data_context.types.base import (
    ExecutionEngineConfig,
    executionEngineConfigSchema,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)


class ExecutionEngineAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            PandasExecutionEngine,
            SparkDFExecutionEngine,
            SqlAlchemyExecutionEngine,
            ExecutionEngine,
        ]

    def anonymize_execution_engine_info(self, name, config):
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        execution_engine_config: ExecutionEngineConfig = (
            executionEngineConfigSchema.load(config)
        )
        execution_engine_config_dict: dict = executionEngineConfigSchema.dump(
            execution_engine_config
        )

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=execution_engine_config_dict,
        )

        return anonymized_info_dict
