from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import reflection

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric import Metric
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.metric_provider import metric
from great_expectations.expectations.metrics.table_metric import (
    TableMetricProvider,
    aggregate_metric,
)
from great_expectations.expectations.metrics.util import column_reflection_fallback
from great_expectations.validator.validation_graph import MetricConfiguration

try:
    import pyspark.sql.types as sparktypes
except ImportError:
    sparktypes = None


class TableColumns(TableMetricProvider):
    metric_name = "table.columns"

    @metric(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(metric_domain_kwargs)
        cols = df.columns
        return cols.tolist()

    @metric(engine=SqlAlchemyExecutionEngine)
    def _pandas(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        batch_id = metric_domain_kwargs.get("batch_id")
        if batch_id is None:
            if execution_engine.active_batch_data_id is not None:
                batch_id = execution_engine.active_batch_data_id
            else:
                raise GreatExpectationsError(
                    "batch_id could not be determined from domain kwargs and no active_batch_data is loaded into the execution engine"
                )
        batch_data = execution_engine.loaded_batch_data.get(batch_id)

        if batch_data is None:
            raise GreatExpectationsError(
                "the requested batch is not available; please load the batch into the execution engine."
            )

        insp = reflection.Inspector.from_engine(execution_engine.engine)
        try:
            columns = insp.get_columns(batch_data.selectable, schema=batch_data.schema)

        except KeyError:
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            columns = column_reflection_fallback(
                selectable=batch_data.selectable,
                dialect=batch_data.dialect,
                sqlalchemy_engine=execution_engine.engine,
            )

        # Use fallback because for mssql reflection doesn't throw an error but returns an empty list
        if len(columns) == 0:
            columns = column_reflection_fallback(
                selectable=batch_data.selectable,
                dialect=batch_data.dialect,
                sqlalchemy_engine=execution_engine.engine,
            )

        return columns

    @metric(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(metric_domain_kwargs)
        return _get_spark_column_names(df.schema)


def _get_spark_column_names(field, parent_name=""):
    cols = []
    if parent_name != "":
        parent_name = parent_name + "."

    if isinstance(field, sparktypes.StructType):
        for child in field.fields:
            cols += _get_spark_column_names(child, parent_name=parent_name)
    elif isinstance(field, sparktypes.StructField):
        if isinstance(field.dataType, sparktypes.StructType):
            for child in field.dataType.fields:
                cols += _get_spark_column_names(
                    child, parent_name=parent_name + field.name
                )
        else:
            if "." in field.name:
                name = "`" + field.name + "`"
            else:
                name = field.name
            cols.append(parent_name + name)
    else:
        raise ValueError("unrecognized field type")

    return cols
