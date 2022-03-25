import logging
from typing import Any, Callable, Dict, List, Optional, Type

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

logger = logging.getLogger(__name__)


class Anonymizer(BaseAnonymizer):
    def __init__(self, salt: Optional[str] = None) -> None:
        super().__init__(salt=salt)

        from great_expectations.core.usage_statistics.anonymizers.action_anonymizer import (
            ActionAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.batch_anonymizer import (
            BatchAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.batch_request_anonymizer import (
            BatchRequestAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.checkpoint_run_anonymizer import (
            CheckpointRunAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.data_connector_anonymizer import (
            DataConnectorAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.data_docs_anonymizer import (
            DataDocsAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.expectation_anonymizer import (
            ExpectationAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.profiler_run_anonymizer import (
            ProfilerRunAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.store_anonymizer import (
            StoreAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.store_backend_anonymizer import (
            StoreBackendAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.validation_operator_anonymizer import (
            ValidationOperatorAnonymizer,
        )

        self.STRATEGIES: List[Type[BaseAnonymizer]] = [
            CheckpointRunAnonymizer,
            ProfilerRunAnonymizer,
            DatasourceAnonymizer,
            DataConnectorAnonymizer,
            StoreAnonymizer,
            BatchRequestAnonymizer,
            BatchAnonymizer,
            ActionAnonymizer,
            DataDocsAnonymizer,
            ExpectationAnonymizer,
            StoreBackendAnonymizer,
            ValidationOperatorAnonymizer,
        ]

    def anonymize(self, obj: object = None, **kwargs) -> Any:
        anonymizer: Optional[BaseAnonymizer] = None
        for anonymizer_cls in self.STRATEGIES:
            if anonymizer_cls.can_handle(obj=obj, **kwargs):
                anonymizer = anonymizer_cls(salt=self._salt)
                return anonymizer.anonymize(obj=obj, **kwargs)

        if isinstance(obj, str):
            return self._anonymize_string(string_=obj)

        raise TypeError(
            f"The type {type(obj)} cannot be handled by the Anonymizer; no suitable strategy found."
        )

    @staticmethod
    def can_handle(obj: object, **kwargs) -> bool:
        return isinstance(obj, object)

    def anonymize_init_payload(self, init_payload: dict) -> dict:
        anonymized_init_payload = {}
        anonymizer_funcs = {
            "datasources": self._anonymize_datasources_init_payload,
            "stores": self._anonymize_stores_init_payload,
            "validation_operators": self._anonymize_validation_operator_init_payload,
            "data_docs_sites": self._anonymize_data_docs_sites_init_payload,
            "expectation_suites": self._anonymize_expectation_suite_init_payload,
        }

        for key, val in init_payload.items():
            anonymizer_func: Optional[Callable] = anonymizer_funcs.get(key)
            if anonymizer_func:
                anonymized_key: str = f"anonymized_{key}"
                anonymized_init_payload[anonymized_key] = anonymizer_func(val)
            else:
                anonymizer_funcs[key] = val

        return anonymized_init_payload

    def _anonymize_datasources_init_payload(self, payload: dict) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )

        anonymizer = DatasourceAnonymizer(salt=self._salt)
        anonymized_values: List[dict] = []
        for name, config in payload.items():
            anonymize_value: dict = anonymizer._anonymize_datasource_info(
                name=name, config=config
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_stores_init_payload(
        self, payload: Dict[str, "Store"]  # noqa: F821
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.store_anonymizer import (
            StoreAnonymizer,
        )

        anonymizer = StoreAnonymizer(salt=self._salt)
        anonymized_values: List[dict] = []
        for store_name, store_obj in payload.items():
            anonymize_value: dict = anonymizer.anonymize(
                store_name=store_name,
                store_obj=store_obj,
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_validation_operator_init_payload(
        self, payload: Dict[str, "ValidationOperator"]  # noqa: F821
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.validation_operator_anonymizer import (
            ValidationOperatorAnonymizer,
        )

        anonymizer = ValidationOperatorAnonymizer(salt=self._salt)
        anonymized_values: List[dict] = []
        for validation_operator_name, validation_operator_obj in payload.items():
            anonymize_value: dict = anonymizer.anonymize(
                validation_operator_name=validation_operator_name,
                validation_operator_obj=validation_operator_obj,
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_data_docs_sites_init_payload(
        self, payload: Dict[str, dict]
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.data_docs_anonymizer import (
            DataDocsAnonymizer,
        )

        anonymizer = DataDocsAnonymizer(salt=self._salt)
        anonymized_values: List[dict] = []
        for site_name, site_config in payload.items():
            anonymize_value: dict = anonymizer.anonymize(
                site_name=site_name, site_config=site_config
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_expectation_suite_init_payload(
        self, payload: List["ExpectationSuite"]  # noqa: F821
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.expectation_anonymizer import (
            ExpectationAnonymizer,
        )

        anonymizer = ExpectationAnonymizer(salt=self._salt)

        anonymized_values: List[dict] = []
        for suite in payload:
            anonymize_value: dict = anonymizer.anonymize(expectation_suite=suite)
            anonymized_values.append(anonymize_value)

        return anonymized_values
