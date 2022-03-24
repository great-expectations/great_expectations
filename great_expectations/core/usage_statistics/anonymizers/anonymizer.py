import logging
from collections import defaultdict
from typing import Any, List, Optional, Type

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

logger = logging.getLogger(__name__)


class Anonymizer(BaseAnonymizer):
    def __init__(self, salt: Optional[str] = None) -> None:
        super().__init__(salt=salt)

        from great_expectations.core.usage_statistics.anonymizers.checkpoint_run_anonymizer import (
            CheckpointRunAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )
        from great_expectations.core.usage_statistics.anonymizers.profiler_run_anonymizer import (
            ProfilerRunAnonymizer,
        )

        self.strategies: List[Type[BaseAnonymizer]] = [
            CheckpointRunAnonymizer,
            ProfilerRunAnonymizer,
            DatasourceAnonymizer,
        ]

    def anonymize(self, obj: object = None, **kwargs) -> Any:
        anonymizer: Optional[BaseAnonymizer] = None
        for anonymizer_cls in self.strategies:
            if anonymizer_cls.can_handle(obj=obj, **kwargs):
                anonymizer = anonymizer_cls(salt=self._salt)
                return anonymizer.anonymize(obj=obj, **kwargs)

        # If our specialized handlers cannot handle the object, default to base anonymizer strategies.
        return self._anonymize(obj=obj, **kwargs)

    def _anonymize(self, obj: object, **kwargs) -> Any:
        if self._is_batch_request_info(info=kwargs):
            return self._anonymize_batch_request(**kwargs)
        if self._is_data_connector_info(obj=obj):
            return self._anonymize_data_connector_info(**kwargs)
        if self._is_batch_info(obj=obj):
            return self._anonymize_batch_info(batch=obj)
        if self._is_store_info(info=kwargs):
            return self._anonymize_store_info(**kwargs)
        if isinstance(obj, str):
            return self._anonymize_string(string_=obj)
        raise TypeError(
            f"The type {type(obj)} cannot be handled by the Anonymizer; no suitable strategy found."
        )

    @staticmethod
    def _is_batch_request_info(info: dict) -> bool:
        return all(
            attr in info
            for attr in ("datasource_name", "data_connector_name", "data_asset_name")
        )

    @staticmethod
    def _is_data_connector_info(obj: object) -> bool:
        from great_expectations.datasource.data_connector.data_connector import (
            DataConnector,
        )

        return obj is not None and isinstance(obj, DataConnector)

    @staticmethod
    def _is_batch_info(obj: object) -> bool:
        from great_expectations.data_asset.data_asset import DataAsset
        from great_expectations.validator.validator import Validator

        if object is None:
            return False

        return isinstance(obj, (Validator, DataAsset)) or (
            isinstance(obj, tuple) and len(obj) == 2
        )

    @staticmethod
    def _is_store_info(info: dict):
        return "store_name" in info or "store_obj" in info

    @staticmethod
    def can_handle(obj: object, **kwargs) -> bool:
        return isinstance(obj, object)

    def anonymize_init_payload(self, init_payload: dict) -> dict:
        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )

        datasource_anonymizer = DatasourceAnonymizer(salt=self._salt)

        anonymizer_funcs = {
            "datasources": datasource_anonymizer._anonymize_datasource_info,
            "stores": self._anonymize_store_info,
            "validation_operators": self._anonymize_validation_operator_info,
            "data_docs_sites": self._anonymize_data_docs_site_info,
            "expectation_suites": self._anonymize_expectation_suite_info,
        }

        anonymized_init_payload = defaultdict(list)
        for key, val in init_payload.items():
            anonymizer_func = anonymizer_funcs.get(key)
            if anonymizer_func:
                anonymized_key: str = f"anonymized_{key}"
                if isinstance(val, list):
                    for v in val:
                        anonymized_init_payload[anonymized_key] = anonymizer_func(v)
                elif isinstance(val, dict):
                    for k, v in val.items():
                        anonymized_init_payload[anonymized_key] = anonymizer_func(k, v)
            else:
                anonymizer_funcs[key] = val

        return anonymized_init_payload
