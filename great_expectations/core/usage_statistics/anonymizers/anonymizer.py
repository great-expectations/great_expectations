from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.data_context.store import Store
    from great_expectations.validation_operators import ValidationOperator

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
        from great_expectations.core.usage_statistics.anonymizers.checkpoint_anonymizer import (
            CheckpointAnonymizer,
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
            ExpectationSuiteAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.profiler_anonymizer import (
            ProfilerAnonymizer,
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

        self._strategies: List[Type[BaseAnonymizer]] = [
            CheckpointAnonymizer,
            ProfilerAnonymizer,
            DatasourceAnonymizer,
            DataConnectorAnonymizer,
            ActionAnonymizer,
            DataDocsAnonymizer,
            ExpectationSuiteAnonymizer,
            ValidationOperatorAnonymizer,
            BatchRequestAnonymizer,
            BatchAnonymizer,
            StoreAnonymizer,
            StoreBackendAnonymizer,
        ]

        # noinspection PyArgumentList
        self._anonymizers: Dict[Type[BaseAnonymizer], BaseAnonymizer] = {
            strategy: strategy(salt=self._salt, aggregate_anonymizer=self)  # type: ignore[call-arg] # BaseAnonymizer has no kwarg `aggregate_anonymizer`
            for strategy in self._strategies
        }

    def anonymize(self, obj: Optional[object] = None, **kwargs: Optional[dict]) -> Any:
        anonymizer: Optional[BaseAnonymizer] = self._get_anonymizer(obj=obj, **kwargs)

        if anonymizer is not None:
            return anonymizer.anonymize(obj=obj, **kwargs)
        elif isinstance(obj, str):
            return self._anonymize_string(string_=obj)
        elif not obj:
            return obj

        raise TypeError(
            f"The type {type(obj)} cannot be handled by the Anonymizer; no suitable strategy found."
        )

    def can_handle(self, obj: object, **kwargs: Optional[dict]) -> bool:
        return self._get_anonymizer(obj=obj, **kwargs) is not None

    def _get_anonymizer(self, obj: object, **kwargs) -> Optional[BaseAnonymizer]:
        for anonymizer in self._anonymizers.values():
            if anonymizer.can_handle(obj=obj, **kwargs):
                return anonymizer

        return None

    def anonymize_init_payload(self, init_payload: Dict[str, Any]) -> Dict[str, Any]:
        anonymized_init_payload: Dict[str, Any] = {}
        anonymizer_funcs: Dict[str, Any] = {
            "datasources": self._anonymize_datasources_init_payload,
            "stores": self._anonymize_stores_init_payload,
            "validation_operators": self._anonymize_validation_operator_init_payload,
            "data_docs_sites": self._anonymize_data_docs_sites_init_payload,
            "expectation_suites": self._anonymize_expectation_suite_init_payload,
            "dependencies": None,  # dependencies do not need anonymization
        }

        for key, val in init_payload.items():
            anonymizer_func: Optional[Callable] = anonymizer_funcs.get(key)
            if anonymizer_func:
                anonymized_key: str = f"anonymized_{key}"
                anonymized_init_payload[anonymized_key] = anonymizer_func(val)
            else:
                anonymized_init_payload[key] = val
        return anonymized_init_payload

    def _anonymize_datasources_init_payload(self, payload: dict) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )

        anonymizer: DatasourceAnonymizer = self._anonymizers[DatasourceAnonymizer]  # type: ignore[assignment] # Type is more concrete subclass

        anonymized_values: List[dict] = []
        for name, config in payload.items():
            anonymize_value: dict = anonymizer._anonymize_datasource_info(
                name=name, config=config
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_stores_init_payload(self, payload: Dict[str, Store]) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.store_anonymizer import (
            StoreAnonymizer,
        )

        anonymizer: StoreAnonymizer = self._anonymizers[StoreAnonymizer]  # type: ignore[assignment] # Type is more concrete subclass

        anonymized_values: List[dict] = []
        for store_name, store_obj in payload.items():
            anonymize_value: dict = anonymizer.anonymize(
                store_name=store_name,
                store_obj=store_obj,
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_validation_operator_init_payload(
        self, payload: Optional[Dict[str, ValidationOperator]] = None
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.validation_operator_anonymizer import (
            ValidationOperatorAnonymizer,
        )

        if payload is None:
            return []

        anonymizer: ValidationOperatorAnonymizer = self._anonymizers[  # type: ignore[assignment] # Type is more concrete subclass
            ValidationOperatorAnonymizer
        ]

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

        anonymizer: DataDocsAnonymizer = self._anonymizers[DataDocsAnonymizer]  # type: ignore[assignment] # Type is more concrete subclass

        anonymized_values: List[dict] = []
        for site_name, site_config in payload.items():
            anonymize_value: dict = anonymizer.anonymize(
                site_name=site_name, site_config=site_config
            )
            anonymized_values.append(anonymize_value)

        return anonymized_values

    def _anonymize_expectation_suite_init_payload(
        self, payload: List[ExpectationSuite]
    ) -> List[dict]:
        from great_expectations.core.usage_statistics.anonymizers.expectation_anonymizer import (
            ExpectationSuiteAnonymizer,
        )

        anonymizer = self._anonymizers[ExpectationSuiteAnonymizer]

        anonymized_values: List[dict] = []
        for suite in payload:
            anonymize_value: dict = anonymizer.anonymize(obj=suite)
            anonymized_values.append(anonymize_value)

        return anonymized_values
