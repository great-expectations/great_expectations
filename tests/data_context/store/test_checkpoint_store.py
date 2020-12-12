import os
import pytest
from copy import deepcopy
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import logging

from great_expectations.checkpoint.checkpoint import (
    LegacyCheckpoint,
    Checkpoint,
)
from great_expectations.data_context import DataContext
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    validates_schema,
)
from great_expectations.data_context.types.base import (
    BaseConfig,
    save_checkpoint_config_to_filesystem,
    load_checkpoint_config_from_filesystem,
    delete_checkpoint_config_from_filesystem,
)
from great_expectations.data_context.store import CheckpointStore
from great_expectations.checkpoint.checkpoint import LegacyCheckpoint
from great_expectations.core.data_context_key import StringKey
from great_expectations.util import gen_directory_tree_str
import great_expectations.exceptions as ge_exceptions
import great_expectations as ge

yaml = YAML()

logger = logging.getLogger(__name__)


def test_checkpoint_store(empty_data_context):
    checkpoint_store = CheckpointStore()

    assert len(checkpoint_store.list_keys()) == 0

    with pytest.raises(TypeError):
        checkpoint_store.set(
            "my_first_checkpoint",
            "this is not a checkpoint"
        )

    assert len(checkpoint_store.list_keys()) == 0

    my_checkpoint = LegacyCheckpoint(
        empty_data_context,
        "my_checkpoint",
        [],
        "my_validation_operator",
    )

    checkpoint_store.set(
        StringKey("my_checkpoint"),
        my_checkpoint
    )

    assert len(checkpoint_store.list_keys()) == 1

    with pytest.raises(ge_exceptions.InvalidKeyError):
        assert checkpoint_store.get(
            StringKey("nonexistent_checkpoint")
        )

    assert checkpoint_store.get(
        StringKey("my_checkpoint")
    ) == {'batches': [], 'validation_operator_name': 'my_validation_operator'}


def test_checkpoint_store_with_filesystem_backend(empty_data_context):
    base_directory = os.path.join(empty_data_context.root_directory, "checkpoints")
    print(base_directory)

    store_backend_config = {
        "module_name": "great_expectations.data_context.store",
        "class_name": "TupleFilesystemStoreBackend",
        "filepath_suffix": ".yml",
        "base_directory": base_directory,
    }

    checkpoint_store = CheckpointStore(
        store_backend=store_backend_config
    )

    my_checkpoint = LegacyCheckpoint(
        empty_data_context,
        "my_checkpoint",
        [],
        "my_validation_operator",
    )

    checkpoint_store.set(
        StringKey("my_checkpoint"),
        my_checkpoint
    )

    file_tree = gen_directory_tree_str(base_directory)
    assert (
            file_tree
            == """checkpoints/
    .ge_store_backend_id
    my_checkpoint.yml
""")

    assert checkpoint_store.get(
        StringKey("my_checkpoint")
    ) == {'batches': [], 'validation_operator_name': 'my_validation_operator'}


def test_checkpoint_v3_configuration_store(tmp_path_factory):
    class CheckpointConfig(BaseConfig):
        def __init__(
            self,
            some_param_0: str = None,
            some_param_1: int = None,
            commented_map: CommentedMap = None,
        ):
            if some_param_0 is None:
                some_param_0 = "param_value_0"
            self.some_param_0 = some_param_0
            if some_param_1 is None:
                some_param_1 = 169
            self.some_param_1 = some_param_1

            super().__init__(commented_map=commented_map)

        @classmethod
        def from_commented_map(cls, commented_map: CommentedMap) -> BaseConfig:
            try:
                config: dict = CheckpointConfigSchema().load(commented_map)
                return cls(commented_map=commented_map, **config)
            except ValidationError:
                logger.error(
                    "Encountered errors during loading checkpoint config. See ValidationError for more details."
                )
                raise

        def get_schema_validated_updated_commented_map(self) -> CommentedMap:
            commented_map: CommentedMap = deepcopy(self.commented_map)
            commented_map.update(CheckpointConfigSchema().dump(self))
            return commented_map

    class CheckpointConfigSchema(Schema):
        class Meta:
            unknown = INCLUDE

        some_param_0 = fields.String()
        some_param_1 = fields.Integer()

        @validates_schema
        def validate_schema(self, data, **kwargs):
            pass

    ge.data_context.types.base.CheckpointConfig = CheckpointConfig
    ge.data_context.types.base.CheckpointConfigSchema = CheckpointConfigSchema

    base_directory: str = str(
        tmp_path_factory.mktemp(
            "test_checkpoint_v3_configuration_store"
        )
    )

    checkpoint_config_0: CheckpointConfig = CheckpointConfig(
        some_param_0="test_str_0",
        some_param_1=65
    )
    store_name_0: str = "test_checkpoint_config_0"
    save_checkpoint_config_to_filesystem(
        store_name=store_name_0,
        base_directory=base_directory,
        checkpoint_config=checkpoint_config_0,
    )

    assert len([path for path in Path(base_directory).iterdir() if str(path).find(".ge_store_backend_id") == (-1)]) == 1

    stored_checkpoint_file_name_0: str = os.path.join(base_directory, f"{store_name_0}.yml")
    with open(stored_checkpoint_file_name_0, "r") as f:
        config: CommentedMap = yaml.load(f)
        expected_config: CommentedMap = CommentedMap(
            {
                "some_param_0": "test_str_0",
                "some_param_1": 65
            }
        )
        assert config == expected_config

    loaded_config: CheckpointConfig = load_checkpoint_config_from_filesystem(
        store_name=store_name_0,
        base_directory=base_directory,
    )
    assert loaded_config.to_json_dict() == checkpoint_config_0.to_json_dict()

    checkpoint_config_1: CheckpointConfig = CheckpointConfig(
        some_param_0="test_str_1",
        some_param_1=26
    )
    store_name_1: str = "test_checkpoint_config_1"
    save_checkpoint_config_to_filesystem(
        store_name=store_name_1,
        base_directory=base_directory,
        checkpoint_config=checkpoint_config_1,
    )

    assert len([path for path in Path(base_directory).iterdir() if str(path).find(".ge_store_backend_id") == (-1)]) == 2

    stored_checkpoint_file_name_1: str = os.path.join(base_directory, f"{store_name_1}.yml")
    with open(stored_checkpoint_file_name_1, "r") as f:
        config: CommentedMap = yaml.load(f)
        expected_config: CommentedMap = CommentedMap(
            {
                "some_param_0": "test_str_1",
                "some_param_1": 26
            }
        )
        assert config == expected_config

    loaded_config: CheckpointConfig = load_checkpoint_config_from_filesystem(
        store_name=store_name_1,
        base_directory=base_directory,
    )
    assert loaded_config.to_json_dict() == checkpoint_config_1.to_json_dict()

    delete_checkpoint_config_from_filesystem(
        store_name=store_name_0,
        base_directory=base_directory,
    )
    assert len([path for path in Path(base_directory).iterdir() if str(path).find(".ge_store_backend_id") == (-1)]) == 1

    delete_checkpoint_config_from_filesystem(
        store_name=store_name_1,
        base_directory=base_directory,
    )
    assert len([path for path in Path(base_directory).iterdir() if str(path).find(".ge_store_backend_id") == (-1)]) == 0
