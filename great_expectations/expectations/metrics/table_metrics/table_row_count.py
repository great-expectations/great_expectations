from typing import Any, Dict

import pandas as pd

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    DataReference,
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import (
    metric_partial,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


class TableRowCount(TableMetricProvider):
    metric_name = "table.row_count"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        data_reference: DataReference = execution_engine.get_data_reference(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        df: pd.DataFrame = data_reference.data

        return df.shape[0]

    @metric_partial(
        engine=SqlAlchemyExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN,
        domain_type=MetricDomainTypes.TABLE,
    )
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        return sa.func.count(), metric_domain_kwargs, {}

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN,
        domain_type=MetricDomainTypes.TABLE,
    )
    def _spark(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        return F.count(F.lit(1)), metric_domain_kwargs, {}
