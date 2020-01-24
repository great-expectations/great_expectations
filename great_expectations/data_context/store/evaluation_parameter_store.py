import json

from great_expectations.core import ensure_json_serializable, BatchKwargs, MetricKwargs
from great_expectations.data_context.store.database_store_backend import DatabaseStoreBackend
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.metrics import EvaluationParameterIdentifier, BatchMetricIdentifier
from great_expectations.util import load_class


class EvaluationParameterStore(Store):
    key_class = EvaluationParameterIdentifier

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
                        "batch_identifier",
                        "metric_identifier",
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
            batch_kwargs = backend_value["batch_kwargs"]
            metric_name = backend_value["metric_name"]
            metric_kwargs = backend_value["metric_kwargs"]
            evaluation_parameter_identifier = EvaluationParameterIdentifier(
                run_id=run_id,
                batch_metric_identifier=BatchMetricIdentifier(
                    batch_identifier=BatchKwargs(batch_kwargs).to_id(),
                    metric_name=metric_name,
                    metric_kwargs_identifier=MetricKwargs(metric_kwargs).to_id()
                )
            )
            params[evaluation_parameter_identifier.to_urn()] = backend_value["value"]
        return params
    #
    # def serialize(self, key, value):
    #     return json.dumps({
    #         "value": value,
    #         "batch_kwargs": key.metric_kwargs,
    #         "metric_kwargs": key.metric_kwargs
    #     })
    #
    # def deserialize(self, key, value):
    #     return json.loads(value)["value"]
