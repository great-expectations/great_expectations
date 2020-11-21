import logging

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

logger = logging.getLogger(__name__)
import numpy as np

try:
    # import pyspark.sql.types as sparktypes
    # from pyspark.ml.feature import Bucketizer
    # from pyspark.sql import SQLContext, Window
    # from pyspark.sql.functions import (
    #     array,
    #     col,
    #     count,
    #     countDistinct,
    #     datediff,
    #     desc,
    #     expr,
    #     isnan,
    #     lag,
    # )
    from pyspark.sql.functions import lit
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


class ColumnValuesInSet(ColumnMapMetricProvider):
    condition_metric_name = "column_values.in_set"
    condition_value_keys = ("value_set",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, value_set, **kwargs):
        if value_set is None:
            # Vacuously true
            return np.ones(len(column), dtype=np.bool_)
        return column.isin(value_set)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, **kwargs):
        if value_set is None:
            # vacuously true
            return True
        return column.in_(value_set)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value_set, **kwargs):
        if value_set is None:
            # vacuously true
            return F.lit(True)
        return column.isin(value_set)
