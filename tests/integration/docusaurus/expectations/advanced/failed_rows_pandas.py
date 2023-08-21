import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.datasource.fluent.interfaces import DataAsset

context = gx.get_context(context_root_dir="./great_expectations", cloud_mode=False)

# validator
visits_asset: DataAsset = context.datasources["visits_datasource"].get_asset("visits")
batch_request: BatchRequest = visits_asset.build_batch_request()

checkpoint: Checkpoint = Checkpoint(
    name="my_checkpoint",
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name="visitors_exp",
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        # {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)

context.add_or_update_checkpoint(checkpoint=checkpoint)

# Example 1
results: CheckpointResult = checkpoint.run()
evrs = results.list_validation_results()
evrs[0]["results"][0]["result"]

# Example 2
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
}
results: CheckpointResult = checkpoint.run()
evrs = results.list_validation_results()
evrs[0]["results"][0]["result"]

# Example 3
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id", "visit_id"],
}
results: CheckpointResult = checkpoint.run()
evrs = results.list_validation_results()
evrs[0]["results"][0]["result"]
