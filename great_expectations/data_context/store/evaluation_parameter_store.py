from six import string_types

from great_expectations.core import DataContextKey
from great_expectations.data_context.store import NamespacedReadWriteStore


class InMemoryEvaluationParameterStore(object):
    """You want to be a dict. You get to be a dict. But we call you a Store."""
    
    def __init__(self, root_directory=None):
        self.store = {}

    def get(self, key):
        return self.store[key]

    def set(self, key, value):
        self.store[key] = value

    def has_key(self, key):
        return key in self.store

    def list_keys(self):
        return list(self.store.keys())


class BaseEvaluationParameterIdentifier(DataContextKey):
    def __init__(self, run_id, parameter_name):
        self._run_id = run_id
        if not isinstance(parameter_name, string_types):
            raise ValueError("parameter_name must be a string")
        self._parameter_name = parameter_name

    @property
    def run_id(self):
        return self._run_id

    @property
    def parameter_name(self):
        return self._parameter_name

    def to_tuple(self):
        return self.run_id, self.parameter_name


class EvaluationParameterStore(NamespacedReadWriteStore):

    def _validate_value(self, value):
        pass

    def _init_store_backend(self, store_backend_config, runtime_config):
        self.key_class = BaseEvaluationParameterIdentifier
        self.namespaceAwareExpectationSuiteSchema = NamespaceAwareExpectationSuiteSchema(strict=True)

        if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
            config_defaults = {
                "key_length": 4,
                "module_name": "great_expectations.data_context.store",
                "filepath_template": "{0}/{1}/{2}/{3}.json",
            }
        else:
            config_defaults = {
                "module_name": "great_expectations.data_context.store",
            }

        return instantiate_class_from_config(
            config=store_backend_config,
            runtime_config=runtime_config,
            config_defaults=config_defaults,
        )
