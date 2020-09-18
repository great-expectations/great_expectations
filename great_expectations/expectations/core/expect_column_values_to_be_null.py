import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapDatasetExpectation


class ExpectColumnValuesToBeNull(ColumnMapDatasetExpectation):
    map_metric = "column_values.null"
    metric_dependencies = "column_values.null.count"

    @PandasExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        filter_column_isnull=False,
    )
    def _nonnull_count(self, series: pd.Series, runtime_configuration: dict = None):
        return series.isnull()

    @SqlAlchemyExecutionEngine.column_map_metric(
        metric_name=map_metric,
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        filter_column_isnull=False,
    )
    def _sqlalchemy_null_map_metric(self, column, runtime_configuration: dict = None):
        import sqlalchemy as sa

        return column.is_(None)

    #
    # @SqlAlchemyExecutionEngine.metric(
    #     metric_name="column_values.null.count",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=tuple(),
    #     metric_dependencies=tuple(),
    #     batchable=True
    # )
    # def _sqlalchemy_null_count(
    #     self,
    #     batches: Dict[str, Batch],
    #     execution_engine: SqlAlchemyExecutionEngine,
    #     metric_domain_kwargs: dict,
    #     metric_value_kwargs: dict,
    #     metrics: dict,
    #     runtime_configuration: dict = None,
    # ):
    #     import sqlalchemy as sa
    #     table = execution_engine._get_selectable(
    #         domain_kwargs=metric_domain_kwargs, batches=batches
    #     )
    #     return sa.func.sum(
    #         sa.case(
    #             [
    #                 (
    #                     sa.column(metric_domain_kwargs["column"]).is_(None),
    #                     1,
    #                 )
    #             ],
    #             else_=0,
    #         )
    #     ), table
