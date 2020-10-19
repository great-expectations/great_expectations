from great_expectations.core.metric import Metric
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnAggregateMetric,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.table_metric import (
    TableMetric,
    table_metric,
)


class TableRowCount(TableMetric):
    metric_name = "table.row_count"

    @table_metric(engine=PandasExecutionEngine)
    def _pandas(cls, table, **kwargs):
        return table.shape[0]

    @table_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, table, **kwargs):
        return int(sa.select([sa.func.count()]).select_from(table).scalar())
