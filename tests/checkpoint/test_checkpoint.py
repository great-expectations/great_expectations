import os
import pytest
from copy import deepcopy

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import logging

from great_expectations.checkpoint.checkpoint import (
    LegacyCheckpoint,
    Checkpoint,
)
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    validates_schema,
)
from great_expectations.data_context.types.base import (
    BaseConfig,
    save_checkpoint_config_to_filesystem,
    load_checkpoint_config_from_filesystem,
    delete_checkpoint_config_from_filesystem,
)
import great_expectations.exceptions as ge_exceptions

yaml = YAML()

logger = logging.getLogger(__name__)


def test_checkpoint_instantiates_and_produces_a_validation_result_when_run(filesystem_csv_data_context):

    base_directory = filesystem_csv_data_context.list_datasources()[0]["batch_kwargs_generators"]["subdir_reader"]["base_directory"]
    batch_kwargs = {
        "path": base_directory+"/f1.csv",
        "datasource": "rad_datasource",
        "reader_method": "read_csv",
    }

    checkpoint = LegacyCheckpoint(
        data_context=filesystem_csv_data_context,
        name="my_checkpoint",
        validation_operator_name="action_list_operator",
        batches=[{
            "batch_kwargs": batch_kwargs,
            "expectation_suite_names": [
                "my_suite"
            ]
        }]
    )

    with pytest.raises(
        ge_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        checkpoint.run()

    assert len(filesystem_csv_data_context.validations_store.list_keys()) == 0

    filesystem_csv_data_context.create_expectation_suite("my_suite")
    print(filesystem_csv_data_context.list_datasources())
    results = checkpoint.run()

    assert len(filesystem_csv_data_context.validations_store.list_keys()) == 1


def test_newstyle_checkpoint(filesystem_csv_data_context_v3):
    import yaml

    filesystem_csv_data_context_v3.create_expectation_suite(
        expectation_suite_name="IDs_mapping.warning"
    )

#     my_new_style_checkpoint = instantiate_class_from_config(
#         runtime_environment={
#             "data_context": filesystem_csv_data_context_v3,
#             "name": "my_new_style_checkpoint",
#         },
#         config=yaml.load("""
# class_name: Checkpoint
# module_name: great_expectations.checkpoint.checkpoint
# validation_operator_name: testing
#
# validators:
#   - batch_definition:
#         datasource_name: rad_datasource
#         data_connector: subdir_reader
#         data_asset_name: f1
#     expectation_suite_name: IDs_mapping.warning
# """))
#
#     my_new_style_checkpoint.run()


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
            commented_map.update(CheckpointConfigSchema.dump(self))
            return commented_map

    class CheckpointConfigSchema(Schema):
        class Meta:
            unknown = INCLUDE

        some_param_0 = fields.String()
        some_param_1 = fields.Integer()

        @validates_schema
        def validate_schema(self, data, **kwargs):
            pass

        # noinspection PyUnusedLocal
        @post_load
        def make_checkpoint_config(self, data, **kwargs):
            return CheckpointConfig(**data)

    base_directory: str = str(
        tmp_path_factory.mktemp(
            "test_checkpoint_v3_configuration_store"
        )
    )

    checkpoint_config: CheckpointConfig
    store_name: str

    checkpoint_config = CheckpointConfig(
        some_param_0="test_str_0",
        some_param_1=65
    )
    store_name = "test_checkpoint_config_0"
    save_checkpoint_config_to_filesystem(
        store_name=store_name,
        base_directory=base_directory,
        checkpoint_config=checkpoint_config,
    )

    stored_checkpoint_file_name: str = os.path.join(base_directory, "test_checkpoint_config_0.yml")
    with open(stored_checkpoint_file_name, "r") as f:
        config: dict = yaml.load(f)
        print(f'[ALEX_TEST] CONFIG: {config}')
