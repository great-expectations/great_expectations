import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapDatasetExpectation


class ExpectColumnValuesToBeNull(ColumnMapDatasetExpectation):
    map_metric = "map.null"
    metric_dependencies = "map.null.count"

    @PandasExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        provide_unexpected_metric_values=False,
        provide_unexpected_value_counts=False,
    )
    def _nonnull_count(self, series: pd.Series, runtime_configuration: dict = None):
        return series.isnull()
