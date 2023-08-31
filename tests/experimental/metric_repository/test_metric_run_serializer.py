from uuid import UUID

from great_expectations.experimental.metric_repository.metric_run_serializer import (
    MetricRunSerializer,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnQuantileValuesMetric,
    MetricRun,
)


# TODO: This just tests pydantic right now. Maybe remove this and the serializers.
def test_serialize_metric_run():
    mock_id = UUID("b606af51-df84-49b5-b6a6-aef774b785ac")
    data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
    serializer = MetricRunSerializer()
    ColumnQuantileValuesMetric.update_forward_refs()
    metric_run = MetricRun(
        id=mock_id,
        data_asset_id=data_asset_id,
        metrics=[
            ColumnQuantileValuesMetric(
                id=mock_id,
                batch_id="batch_id",
                metric_name="metric_name",
                value=[0.25, 0.5, 0.75],
                exception=None,
                column="column",
                quantiles=[0.25, 0.5, 0.75],
                allow_relative_error=0.001,
            )
        ],
    )
    serialized_metric_run = serializer.serialize(metric_run)
    assert serialized_metric_run == (
        f"""{{"id": "{mock_id}", "data_asset_id": "{data_asset_id}", "metrics": [{{"id": "{mock_id}", "batch_id": "batch_id", "metric_name": "metric_name", "value": [0.25, 0.5, 0.75], "exception": null, "column": "column", "quantiles": [0.25, 0.5, 0.75], "allow_relative_error": 0.001}}]}}"""
    )
