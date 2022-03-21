import logging
from typing import List, Optional, Type, Union, cast

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

logger = logging.getLogger(__name__)


class Anonymizer(BaseAnonymizer):
    """Anonymize string names in an optionally-consistent way."""

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

    def anonymize(self, obj: object, *args, **kwargs) -> Union[str, dict]:
        anonymizer: Optional[BaseAnonymizer] = None
        for anonymizer_cls in self.strategies:
            if anonymizer_cls.can_handle(obj, *args, **kwargs):
                anonymizer = anonymizer_cls(salt=self._salt)
                return anonymizer.anonymize(obj, *args, **kwargs)

        return self._anonymize(obj, *args, **kwargs)

    def _anonymize(self, obj: object, *args, **kwargs) -> Union[str, dict]:
        if isinstance(obj, str):
            payload: str = cast(str, self._anonymize_string(string_=obj))
            return payload
        if self._is_batch_info(obj=obj):
            return self._anonymize_batch_info(batch=obj)
        return {}

    @staticmethod
    def _is_batch_info(obj: object) -> bool:
        from great_expectations.data_asset.data_asset import DataAsset
        from great_expectations.validator.validator import Validator

        return isinstance(obj, (Validator, DataAsset)) or (
            isinstance(obj, tuple) and len(obj) == 2
        )

    @staticmethod
    def can_handle(obj: object) -> bool:
        return isinstance(obj, object)
