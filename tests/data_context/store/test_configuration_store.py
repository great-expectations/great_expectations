from pathlib import Path

import pytest
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

try:
    from unittest import mock
except ImportError:
    from unittest import mock

import logging

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    fields,
    validates_schema,
)
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import (
    delete_config_from_filesystem,
    load_config_from_filesystem,
    save_config_to_filesystem,
)

yaml = YAML()

logger = logging.getLogger(__name__)


class SampleConfig(BaseYamlConfig):
    @classmethod
    def get_config_class(cls):
        return cls  # SampleConfig

    @classmethod
    def get_schema_class(cls):
        return SampleConfigSchema

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


class SampleConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    some_param_0 = fields.String()
    some_param_1 = fields.Integer()

    @validates_schema
    def validate_schema(self, data, **kwargs):
        pass


class SampleConfigurationStore(ConfigurationStore):
    _configuration_class = SampleConfig


def test_v3_configuration_store(tmp_path_factory):
    root_directory_path: str = "test_v3_configuration_store"
    root_directory: str = str(tmp_path_factory.mktemp(root_directory_path))
    base_directory: str = str(Path(root_directory) / "some_store_config_dir")

    config_0: SampleConfig = SampleConfig(some_param_0="test_str_0", some_param_1=65)
    store_name_0: str = "test_config_store_0"
    configuration_name_0: str = "test_config_name_0"

    with pytest.raises(FileNotFoundError):
        save_config_to_filesystem(
            configuration_store_class_name="unknown_class",
            configuration_store_module_name="unknown_module",
            store_name=store_name_0,
            base_directory=base_directory,
            configuration_key=configuration_name_0,
            configuration=config_0,
        )

    save_config_to_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_0,
        base_directory=base_directory,
        configuration_key=configuration_name_0,
        configuration=config_0,
    )

    dir_tree: str = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """some_store_config_dir/
    .ge_store_backend_id
    test_config_name_0.yml
"""
    )
    assert (
        len(
            [
                path
                for path in Path(base_directory).iterdir()
                if str(path).find(".ge_store_backend_id") == (-1)
            ]
        )
        == 1
    )

    stored_file_name_0: str = Path(base_directory) / f"{configuration_name_0}.yml"
    with open(stored_file_name_0) as f:
        config: CommentedMap = yaml.load(f)
        expected_config: CommentedMap = CommentedMap(
            {"some_param_0": "test_str_0", "some_param_1": 65}
        )
        assert config == expected_config

    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        loaded_config: BaseYamlConfig = load_config_from_filesystem(
            configuration_store_class_name="SampleConfigurationStore",
            configuration_store_module_name=SampleConfigurationStore.__module__,
            store_name=store_name_0,
            base_directory="unknown_base_directory",
            configuration_key=configuration_name_0,
        )
    with pytest.raises(ge_exceptions.InvalidKeyError):
        # noinspection PyUnusedLocal
        loaded_config: BaseYamlConfig = load_config_from_filesystem(
            configuration_store_class_name="SampleConfigurationStore",
            configuration_store_module_name=SampleConfigurationStore.__module__,
            store_name=store_name_0,
            base_directory=base_directory,
            configuration_key="unknown_configuration",
        )

    loaded_config: BaseYamlConfig = load_config_from_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_0,
        base_directory=base_directory,
        configuration_key=configuration_name_0,
    )
    assert loaded_config.to_json_dict() == config_0.to_json_dict()

    config_1: SampleConfig = SampleConfig(some_param_0="test_str_1", some_param_1=26)
    store_name_1: str = "test_config_store_1"
    configuration_name_1: str = "test_config_name_1"
    save_config_to_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_1,
        base_directory=base_directory,
        configuration_key=configuration_name_1,
        configuration=config_1,
    )

    dir_tree: str = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """some_store_config_dir/
    .ge_store_backend_id
    test_config_name_0.yml
    test_config_name_1.yml
"""
    )
    assert (
        len(
            [
                path
                for path in Path(base_directory).iterdir()
                if str(path).find(".ge_store_backend_id") == (-1)
            ]
        )
        == 2
    )

    stored_file_name_1: str = Path(base_directory) / f"{configuration_name_1}.yml"
    with open(stored_file_name_1) as f:
        config: CommentedMap = yaml.load(f)
        expected_config: CommentedMap = CommentedMap(
            {"some_param_0": "test_str_1", "some_param_1": 26}
        )
        assert config == expected_config

    loaded_config: BaseYamlConfig = load_config_from_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_1,
        base_directory=base_directory,
        configuration_key=configuration_name_1,
    )
    assert loaded_config.to_json_dict() == config_1.to_json_dict()

    delete_config_from_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_0,
        base_directory=base_directory,
        configuration_key=configuration_name_0,
    )
    assert (
        len(
            [
                path
                for path in Path(base_directory).iterdir()
                if str(path).find(".ge_store_backend_id") == (-1)
            ]
        )
        == 1
    )

    delete_config_from_filesystem(
        configuration_store_class_name="SampleConfigurationStore",
        configuration_store_module_name=SampleConfigurationStore.__module__,
        store_name=store_name_1,
        base_directory=base_directory,
        configuration_key=configuration_name_1,
    )
    assert (
        len(
            [
                path
                for path in Path(base_directory).iterdir()
                if str(path).find(".ge_store_backend_id") == (-1)
            ]
        )
        == 0
    )
