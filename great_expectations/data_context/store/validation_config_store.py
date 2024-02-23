from __future__ import annotations

from great_expectations.data_context.store.store import Store


class ValidationConfigStore(Store):
    # _key_class = DataContextVariableKey

    def __init__(
        self,
        store_name: str,
        store_backend: dict | None = None,
        runtime_environment: dict | None = None,
    ) -> None:
        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,  # type: ignore[arg-type]
        )
