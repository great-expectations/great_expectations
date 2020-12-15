# TODO: <Alex>ALEX</Alex>
# import logging
# import os
#
# import pytest
# from ruamel.yaml import YAML
#
# import great_expectations.exceptions as ge_exceptions
# from great_expectations.checkpoint.checkpoint import LegacyCheckpoint
# from great_expectations.core.data_context_key import StringKey
# from great_expectations.data_context.store import CheckpointStore
# from great_expectations.util import gen_directory_tree_str
#
# yaml = YAML()
#
# logger = logging.getLogger(__name__)
#
#
# def test_checkpoint_store(empty_data_context):
#     checkpoint_store = CheckpointStore()
#
#     assert len(checkpoint_store.list_keys()) == 0
#
#     with pytest.raises(TypeError):
#         checkpoint_store.set("my_first_checkpoint", "this is not a checkpoint")
#
#     assert len(checkpoint_store.list_keys()) == 0
#
#     my_checkpoint = LegacyCheckpoint(
#         empty_data_context, "my_checkpoint", [], "my_validation_operator",
#     )
#
#     checkpoint_store.set(StringKey("my_checkpoint"), my_checkpoint)
#
#     assert len(checkpoint_store.list_keys()) == 1
#
#     with pytest.raises(ge_exceptions.InvalidKeyError):
#         assert checkpoint_store.get(StringKey("nonexistent_checkpoint"))
#
#     assert checkpoint_store.get(StringKey("my_checkpoint")) == {
#         "batches": [],
#         "validation_operator_name": "my_validation_operator",
#     }
#
#
# def test_checkpoint_store_with_filesystem_backend(empty_data_context):
#     base_directory = os.path.join(empty_data_context.root_directory, "checkpoints")
#     print(base_directory)
#
#     store_backend_config = {
#         "module_name": "great_expectations.data_context.store",
#         "class_name": "TupleFilesystemStoreBackend",
#         "filepath_suffix": ".yml",
#         "base_directory": base_directory,
#     }
#
#     checkpoint_store = CheckpointStore(store_backend=store_backend_config)
#
#     my_checkpoint = LegacyCheckpoint(
#         empty_data_context, "my_checkpoint", [], "my_validation_operator",
#     )
#
#     checkpoint_store.set(StringKey("my_checkpoint"), my_checkpoint)
#
#     file_tree = gen_directory_tree_str(base_directory)
#     assert (
#         file_tree
#         == """checkpoints/
#     .ge_store_backend_id
#     my_checkpoint.yml
# """
#     )
#
#     assert checkpoint_store.get(StringKey("my_checkpoint")) == {
#         "batches": [],
#         "validation_operator_name": "my_validation_operator",
#     }
