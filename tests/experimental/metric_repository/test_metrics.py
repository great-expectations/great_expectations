import pytest

from great_expectations.experimental.metric_repository.metrics import Metric

pytestmark = pytest.mark.unit


def test_cannot_init_abstract_metric():
    with pytest.raises(NotImplementedError):
        Metric(
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        )
