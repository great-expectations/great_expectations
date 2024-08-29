from __future__ import annotations

import json
from typing import ClassVar, Type

from great_expectations.data_context.store.database_store_backend import (
    DatabaseStoreBackend,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.resource_identifiers import (
    ValidationMetricIdentifier,
)
from great_expectations.util import (
    load_class,
    verify_dynamic_loading_support,
)


class MetricStore(Store):
    """
    A MetricStore stores ValidationMetric information to be used between runs.
    """

    _key_class: ClassVar[Type] = ValidationMetricIdentifier

    def __init__(self, store_backend=None, store_name=None) -> None:
        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get("class_name", "InMemoryStoreBackend")
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            store_backend_class = load_class(store_backend_class_name, store_backend_module_name)

            if issubclass(store_backend_class, DatabaseStoreBackend):
                # Provide defaults for this common case
                if "table_name" not in store_backend:
                    store_backend["table_name"] = store_backend.get("table_name", "ge_metrics")
                if "key_columns" not in store_backend:
                    store_backend["key_columns"] = store_backend.get(
                        "key_columns",
                        [
                            "run_name",
                            "run_time",
                            "data_asset_name",
                            "expectation_suite_identifier",
                            "metric_name",
                            "metric_kwargs_id",
                        ],
                    )

        super().__init__(store_backend=store_backend, store_name=store_name)

    def serialize(self, value):  # type: ignore[explicit-override] # FIXME
        return json.dumps({"value": value})

    def deserialize(self, value):  # type: ignore[explicit-override] # FIXME
        if value:
            return json.loads(value)["value"]
