import logging
import string
from pathlib import Path
from typing import List, Optional

import pytest
from marshmallow import INCLUDE, Schema, fields, validates_schema
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.data_context_key import DataContextKey
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.exceptions.exceptions import DataContextError
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import (
    delete_config_from_filesystem,
    load_config_from_filesystem,
    save_config_to_filesystem,
)

yaml = YAMLHandler()

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

    def serialization_self_check(self, pretty_print: bool) -> None:
        # Required to fulfill contract set by parent
        pass

    def list_keys(self) -> List[DataContextKey]:
        # Mock values to work with self.self_check
        return [
            ConfigurationIdentifier(f"key{char}") for char in string.ascii_uppercase
        ]


@pytest.mark.integration
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
    with pytest.raises(gx_exceptions.InvalidKeyError):
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


@pytest.mark.unit
def test_overwrite_existing_property_and_setter() -> None:
    store = SampleConfigurationStore(store_name="my_configuration_store")

    assert store.overwrite_existing is False
    store.overwrite_existing = True
    assert store.overwrite_existing is True


@pytest.mark.unit
def test_config_property_and_defaults() -> None:
    store = SampleConfigurationStore(store_name="my_configuration_store")

    assert store.config == {
        "class_name": "SampleConfigurationStore",
        "module_name": "tests.data_context.store.test_configuration_store",
        "overwrite_existing": False,
        "store_name": "my_configuration_store",
    }


@pytest.mark.unit
def test_self_check(capsys) -> None:
    store = SampleConfigurationStore(store_name="my_configuration_store")

    report_obj = store.self_check(pretty_print=True)

    keys = [f"key{char}" for char in string.ascii_uppercase]

    assert report_obj == {
        "config": {
            "class_name": "SampleConfigurationStore",
            "module_name": "tests.data_context.store.test_configuration_store",
            "overwrite_existing": False,
            "store_name": "my_configuration_store",
        },
        "keys": keys,
        "len_keys": len(keys),
    }

    stdout = capsys.readouterr().out

    messages = [
        "Checking for existing keys...",
        f"{len(keys)} keys found",
    ]

    for message in messages:
        assert message in stdout


@pytest.mark.parametrize(
    "name,id,expected_key",
    [
        pytest.param(
            "my_name",
            None,
            ConfigurationIdentifier(configuration_key="my_name"),
            id="name",
        ),
        pytest.param(
            None,
            "abc123",
            GXCloudIdentifier(
                resource_type=GXCloudRESTResource.CHECKPOINT, id="abc123"
            ),
            id="id",
        ),
    ],
)
@pytest.mark.unit
def test_determine_key_constructs_key(
    name: Optional[str], id: Optional[str], expected_key: DataContextKey
) -> None:
    actual_key = ConfigurationStore(store_name="test")._determine_key(name=name, id=id)
    assert actual_key == expected_key


@pytest.mark.parametrize(
    "name,id",
    [
        pytest.param("my_name", "abc123", id="too many args"),
        pytest.param(
            None,
            None,
            id="too few args",
        ),
    ],
)
@pytest.mark.unit
def test_determine_key_raises_error_with_conflicting_args(
    name: Optional[str], id: Optional[str]
) -> None:
    with pytest.raises(AssertionError) as e:
        ConfigurationStore(store_name="test")._determine_key(name=name, id=id)

    assert "Must provide either name or id" in str(e.value)


@pytest.mark.unit
def test_init_with_invalid_configuration_class_raises_error() -> None:
    class InvalidConfigClass:
        pass

    class InvalidConfigurationStore(ConfigurationStore):
        _configuration_class = InvalidConfigClass

    with pytest.raises(DataContextError) as e:
        InvalidConfigurationStore(store_name="my_configuration_store")

    assert (
        "Invalid configuration: A configuration_class needs to inherit from the BaseYamlConfig class."
        in str(e.value)
    )
