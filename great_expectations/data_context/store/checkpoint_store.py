from ruamel.yaml import YAML

from great_expectations.core.data_context_key import StringKey
from great_expectations.data_context.store.store import Store


class CheckpointStore(Store):
    """
A CheckpointStore manages Checkpoints for the DataContext.
    """

    _key_class = StringKey

    def serialize(self, key, value):
        # return yaml.dump(value.get_config())
        return value.get_config(format="yaml")

    def deserialize(self, key, value):
        yaml = YAML()
        return yaml.load(value)
