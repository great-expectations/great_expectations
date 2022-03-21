import logging
from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.core.usage_statistics.util import (
    aggregate_all_core_expectation_types,
)

logger = logging.getLogger(__name__)


class Anonymizer(BaseAnonymizer):
    """Anonymize string names in an optionally-consistent way."""

    # Any class that starts with this __module__ is considered a "core" object
    CORE_GE_OBJECT_MODULE_PREFIX = "great_expectations"
    CORE_GE_EXPECTATION_TYPES = aggregate_all_core_expectation_types()

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

        self.strategies = [
            CheckpointRunAnonymizer,
            ProfilerRunAnonymizer,
            DatasourceAnonymizer,
        ]

    def anonymize(self, obj: object) -> dict:
        raise NotImplementedError

    @staticmethod
    def can_handle(obj: object) -> bool:
        raise NotImplementedError
