import logging
import platform
import sys
from typing import List, Optional, Type, Union, cast

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

    def anonymize(self, obj: object = None, **kwargs) -> Union[str, dict]:
        anonymizer: Optional[BaseAnonymizer] = None
        for anonymizer_cls in self.strategies:
            if anonymizer_cls.can_handle(obj, **kwargs):
                anonymizer = anonymizer_cls(salt=self._salt)
                return anonymizer.anonymize(obj, **kwargs)

        return self._anonymize(obj, **kwargs)

    def _anonymize(self, obj: object, **kwargs) -> Union[str, dict]:
        if self._is_data_connector_info(obj=obj):
            return self._anonymize_data_connector_info(**kwargs)
        if self._is_batch_info(obj=obj):
            return self._anonymize_batch_info(batch=obj)
        if self._is_store_info(kwargs):
            return self._anonymize_store_info(**kwargs)
        if isinstance(obj, str):
            payload: str = cast(str, self._anonymize_string(string_=obj))
            return payload
        return {}

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

    def build_init_payload(self, data_context: "DataContext") -> dict:  # noqa: F821
        """Adds information that may be available only after full data context construction, but is useful to
        calculate only one time (for example, anonymization)."""
        expectation_suites = [
            data_context.get_expectation_suite(expectation_suite_name)
            for expectation_suite_name in data_context.list_expectation_suite_names()
        ]

        from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
            DatasourceAnonymizer,
        )

        datasource_anonymizer = DatasourceAnonymizer(salt=self._salt)

        return {
            "platform.system": platform.system(),
            "platform.release": platform.release(),
            "version_info": str(sys.version_info),
            "anonymized_datasources": [
                datasource_anonymizer._anonymize_datasource_info(
                    datasource_name, datasource_config
                )
                for datasource_name, datasource_config in data_context.project_config_with_variables_substituted.datasources.items()
            ],
            "anonymized_stores": [
                self._anonymize_store_info(store_name, store_obj)
                for store_name, store_obj in data_context.stores.items()
            ],
            "anonymized_validation_operators": [
                self._anonymize_validation_operator_info(
                    validation_operator_name=validation_operator_name,
                    validation_operator_obj=validation_operator_obj,
                )
                for validation_operator_name, validation_operator_obj in data_context.validation_operators.items()
            ],
            "anonymized_data_docs_sites": [
                self._anonymize_data_docs_site_info(
                    site_name=site_name, site_config=site_config
                )
                for site_name, site_config in data_context.project_config_with_variables_substituted.data_docs_sites.items()
            ],
            "anonymized_expectation_suites": [
                self._anonymize_expectation_suite_info(expectation_suite)
                for expectation_suite in expectation_suites
            ],
        }
