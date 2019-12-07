import json

from great_expectations.core import ensure_json_serializable, DataAssetIdentifier
from great_expectations.data_context.store.database_store_backend import DatabaseStoreBackend
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.metrics import ExpectationDefinedMetricIdentifier
from great_expectations.util import load_class


class EvaluationParameterStore(Store):
    key_class = ExpectationDefinedMetricIdentifier

    def __init__(self, store_backend=None):
        if store_backend is not None:
            store_backend_module_name = store_backend.get("module_name", "great_expectations.data_context.store")
            store_backend_class_name = store_backend.get("class_name", "InMemoryStoreBackend")
            store_backend_class = load_class(store_backend_class_name, store_backend_module_name)

            if issubclass(store_backend_class, DatabaseStoreBackend):
                # Provide defaults for this common case
                store_backend["table_name"] = store_backend.get("table_name", "ge_expectation_defined_metrics")
                store_backend["key_columns"] = store_backend.get(
                    "key_columns", [
                        "run_id",
                        "datasource",
                        "generator",
                        "generator_asset",
                        "expectation_suite_name",
                        "expectation_type",
                        "metric_name",
                        "metric_kwargs_str"
                    ]
                )

        super(EvaluationParameterStore, self).__init__(store_backend=store_backend)

    # noinspection PyMethodMayBeStatic
    def _validate_value(self, value):
        # Values must be json serializable since they must be inputs to expectation configurations
        ensure_json_serializable(value)

    def get_bind_params(self, run_id):
        params = {}
        for k in self._store_backend.list_keys((run_id,)):
            backend_value = json.loads(self._store_backend.get(k))
            metric_kwargs = backend_value["metric_kwargs"]
            metric_id = ExpectationDefinedMetricIdentifier(
                run_id=k[0],
                data_asset_name=DataAssetIdentifier.from_tuple(k[1:4]),
                expectation_suite_name=k[4],
                expectation_type=k[5],
                metric_name=k[6],
                metric_kwargs=metric_kwargs
            )
            params[metric_id.to_urn()] = backend_value["value"]
        return params

    def key_to_tuple(self, key):
        return key.to_string_tuple()

    def serialize(self, key, value):
        return json.dumps({
            "value": value,
            "metric_kwargs": key.metric_kwargs
        })

    def deserialize(self, key, value):
        return json.loads(value)["value"]
