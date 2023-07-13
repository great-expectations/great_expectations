from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.experimental.column_descriptive_metrics.metrics import (
    BatchPointer,
    Metric,
    Metrics,
    Value,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class AssetInspector:
    def __init__(self):
        pass

    def _batch_request_to_batch(self, batch_request: BatchRequest) -> Batch:
        # TODO: Implementation. How do we get the batch from the batch_request?
        #  generally we call asset.get_batch_list(batch_request=batch_request)
        #  but we don't have an asset here so we have to use the batch request to get it,
        #  potentially from the context. Maybe this method doesn't live in the AssetInspector and instead
        #  we use the Batch in the AssetInspector methods that we get outside of the AssetInspector.
        pass

    def get_column_descriptive_metrics(self, batch_request: BatchRequest) -> Metrics:
        self._batch_request_to_batch(batch_request=batch_request)

    def _get_table_row_count_metric(self, batch: Batch) -> Metric:
        (
            MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs={"batch_id": batch.id},
                metric_value_kwargs={},
            ),
        )
        # TODO: Implementation, use metric calculator etc. Metric below is just a placeholder.
        return Metric(
            id="47d3f290-809f-4d94-a3ef-a5a510c3d2d9",
            organization_id="e3989417-2c85-447c-b084-113c9c6f5bbf",
            run_id="539f813e-9899-4b39-80b5-e909622255b3",
            batch_pointer=BatchPointer(
                datasource_name="my_datasource",
                data_asset_name="my_data_asset",
                batch_name="my_batch",
            ),
            metric_name="table.row_count",
            metric_domain_kwargs={"column": "my_column"},
            metric_value_kwargs={},
            column="my_column",
            value=Value(value=100),
            details={},
        )
