from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


class MulticolumnSumEqual(MulticolumnMapMetricProvider):
    condition_metric_name = "multicolumn_sum.equal"
    condition_value_keys = ("sum_total",)
    condition_domain_keys = ("batch_id", "table", "column_list")

    # TODO: <Alex>ALEX -- temporarily only Pandas and SQLAlchemy implementations are provided (others to follow).</Alex>
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = column_list.sum(axis=1) == sum_total
        return row_wise_cond

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_clause, **kwargs):
        sum_total = kwargs.get("sum_total")
        engine = kwargs.get("_sqlalchemy_engine")
        table = kwargs.get("_table")
        query = sa.select(column_clause).select_from(table)
        with engine.connect() as connection:
            result = connection.execute(query).fetchall()
        row_wise_cond = [sum(row) == sum_total for row in result]
        return [result[i] for i in range(len(result)) if row_wise_cond[i]]
