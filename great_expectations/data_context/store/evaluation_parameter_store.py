from abc import abstractmethod

from great_expectations.core import ensure_json_serializable
from great_expectations.data_context.store import NamespacedReadWriteStore
from great_expectations.data_context.types.metrics import ExpectationDefinedMetricIdentifier


class EvaluationParameterStore(NamespacedReadWriteStore):
    key_class = ExpectationDefinedMetricIdentifier

    def _validate_value(self, value):
        ensure_json_serializable(value)

    @abstractmethod
    def _init_store_backend(self, store_backend_config, runtime_config):
        pass

    @abstractmethod
    def get_bind_params(self, run_id):
        pass


class InMemoryEvaluationParameterStore(EvaluationParameterStore):
    # noinspection PyUnusedLocal
    def __init__(self, root_directory=None):
        super(InMemoryEvaluationParameterStore, self).__init__(
            store_backend={},
            root_directory=root_directory,
            serialization_type=None
        )

    def _get(self, key):
        return self._store[key]

    def get_bind_params(self, run_id):
        params = {}
        for k in self._store.keys():
            if k.run_id == run_id:
                params[k.to_urn()] = self._store[k]
        return params

    def _set(self, key, value):
        self._store[key] = value

    def _validate_value(self, value):
        pass

    def _init_store_backend(self, store_backend_config, runtime_config):
        self._store = {}
