from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    MapMetricProvider,
    column_map_condition,
)


class ColumnValuesNonNull(MapMetricProvider):
    condition_metric_name = "column_values.nonnull"
    filter_column_isnull = False

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return ~column.isnull()

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return column != None

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.isNotNull()
