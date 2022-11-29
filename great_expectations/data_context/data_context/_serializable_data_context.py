from typing import Optional

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)


class _SerializableDataContext(AbstractDataContext):
    def __init__(self, runtime_environment: Optional[dict] = None) -> None:
        super().__init__(runtime_environment=runtime_environment)
