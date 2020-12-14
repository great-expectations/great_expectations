import pytest

from great_expectations.checkpoint.checkpoint import Checkpoint, LegacyCheckpoint
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import DataContextError


def test_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    filesystem_csv_data_context,
):

    base_directory = filesystem_csv_data_context.list_datasources()[0][
        "batch_kwargs_generators"
    ]["subdir_reader"]["base_directory"]
    batch_kwargs = {
        "path": base_directory + "/f1.csv",
        "datasource": "rad_datasource",
        "reader_method": "read_csv",
    }

    checkpoint = LegacyCheckpoint(
        data_context=filesystem_csv_data_context,
        name="my_checkpoint",
        validation_operator_name="action_list_operator",
        batches=[
            {"batch_kwargs": batch_kwargs, "expectation_suite_names": ["my_suite"]}
        ],
    )

    with pytest.raises(DataContextError, match=r"expectation_suite .* not found"):
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
