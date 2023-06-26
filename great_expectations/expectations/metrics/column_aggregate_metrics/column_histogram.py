import copy
import logging
from typing import TYPE_CHECKING, Any, Dict

import numpy as np

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import (
    functions as F,
)
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import (
    convert_to_json_serializable,
    get_sql_dialect_floating_point_infinity_value,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
)
from great_expectations.expectations.metrics.metric_provider import metric_value

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class ColumnHistogram(ColumnAggregateMetricProvider):
    metric_name = "column.histogram"
    value_keys = ("bins",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: PLR0913
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column = accessor_domain_kwargs["column"]
        bins = metric_value_kwargs["bins"]
        column_series: pd.Series = df[column]
        column_null_elements_cond: pd.Series = column_series.isnull()
        column_nonnull_elements: pd.Series = column_series[~column_null_elements_cond]
        hist, bin_edges = np.histogram(column_nonnull_elements, bins, density=False)
        return list(hist)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        """return a list of counts corresponding to bins

        Args:
            column: the name of the column for which to get the histogram
            bins: tuple of bin edges for which to get histogram values; *must* be tuple to support caching
        """
        selectable, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column = accessor_domain_kwargs["column"]
        bins = metric_value_kwargs["bins"]

        if isinstance(bins, np.ndarray):
            bins = bins.tolist()
        else:
            bins = list(bins)

        case_conditions = []
        if len(bins) == 1 and not (
            (
                bins[0]
                == get_sql_dialect_floating_point_infinity_value(
                    schema="api_np", negative=True
                )
            )
            or (
                bins[0]
                == get_sql_dialect_floating_point_infinity_value(
                    schema="api_cast", negative=True
                )
            )
            or (
                bins[0]
                == get_sql_dialect_floating_point_infinity_value(
                    schema="api_np", negative=False
                )
            )
            or (
                bins[0]
                == get_sql_dialect_floating_point_infinity_value(
                    schema="api_cast", negative=False
                )
            )
        ):
            # Single-valued column data are modeled using "impulse" (or "sample") distributions (on open interval).
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        (
                            sa.and_(
                                float(bins[0] - np.finfo(float).eps)
                                < sa.column(column),
                                sa.column(column)
                                < float(bins[0] + np.finfo(float).eps),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("bin_0")
            )
            query = (
                sa.select(*case_conditions)
                .where(
                    sa.column(column) != None,  # noqa: E711
                )
                .select_from(selectable)
            )

            # Run the data through convert_to_json_serializable to ensure we do not have Decimal types
            return convert_to_json_serializable(
                list(execution_engine.execute_query(query).fetchone())
            )

        idx = 0

        # If we have an infinite lower bound, don't express that in sql
        if (
            bins[0]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            bins[0]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            case_conditions.append(
                sa.func.sum(
                    sa.case((sa.column(column) < bins[idx + 1], 1), else_=0)
                ).label(f"bin_{str(idx)}")
            )
            idx += 1

        negative_boundary: float
        positive_boundary: float
        for idx in range(  # noqa: B020 # loop-variable-overrides-iterator
            idx, len(bins) - 2
        ):
            negative_boundary = float(bins[idx])
            positive_boundary = float(bins[idx + 1])
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        (
                            sa.and_(
                                negative_boundary <= sa.column(column),
                                sa.column(column) < positive_boundary,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label(f"bin_{str(idx)}")
            )

        if (
            bins[-1]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            bins[-1]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            negative_boundary = float(bins[-2])
            case_conditions.append(
                sa.func.sum(
                    sa.case((negative_boundary <= sa.column(column), 1), else_=0)
                ).label(f"bin_{str(len(bins) - 1)}")
            )
        else:
            negative_boundary = float(bins[-2])
            positive_boundary = float(bins[-1])
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        (
                            sa.and_(
                                negative_boundary <= sa.column(column),
                                sa.column(column) <= positive_boundary,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label(f"bin_{str(len(bins) - 1)}")
            )

        query = (
            sa.select(*case_conditions)
            .where(
                sa.column(column) != None,  # noqa: E711
            )
            .select_from(selectable)
        )

        # Run the data through convert_to_json_serializable to ensure we do not have Decimal types
        return convert_to_json_serializable(
            list(execution_engine.execute_query(query).fetchone())
        )

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        bins = metric_value_kwargs["bins"]
        column = metric_domain_kwargs["column"]

        """return a list of counts corresponding to bins"""
        bins = list(
            copy.deepcopy(bins)
        )  # take a copy since we are inserting and popping

        if bins[0] == -np.inf or bins[0] == -float("inf"):
            added_min = False
            bins[0] = -float("inf")
        else:
            added_min = True
            bins.insert(0, -float("inf"))

        if bins[-1] == np.inf or bins[-1] == float("inf"):
            added_max = False
            bins[-1] = float("inf")
        else:
            added_max = True
            bins.append(float("inf"))

        temp_column = df.select(column).where(F.col(column).isNotNull())
        bucketizer = pyspark.Bucketizer(
            splits=bins, inputCol=column, outputCol="buckets"
        )
        bucketed = bucketizer.setHandleInvalid("skip").transform(temp_column)

        # This is painful to do, but: bucketizer cannot handle values outside of a range
        # (hence adding -/+ infinity above)

        # Further, it *always* follows the numpy convention of lower_bound <= bin < upper_bound
        # for all but the last bin

        # But, since the last bin in our case will often be +infinity, we need to
        # find the number of values exactly equal to the upper bound to add those

        # We'll try for an optimization by asking for it at the same time
        if added_max:
            upper_bound_count = (
                temp_column.select(column).filter(F.col(column) == bins[-2]).count()
            )
        else:
            upper_bound_count = 0

        hist_rows = bucketed.groupBy("buckets").count().collect()
        # Spark only returns buckets that have nonzero counts.
        hist = [0] * (len(bins) - 1)
        for row in hist_rows:
            hist[int(row["buckets"])] = row["count"]

        hist[-2] += upper_bound_count

        if added_min:
            below_bins = hist.pop(0)
            bins.pop(0)
            if below_bins > 0:
                logger.warning("Discarding histogram values below lowest bin.")

        if added_max:
            above_bins = hist.pop(-1)
            bins.pop(-1)
            if above_bins > 0:
                logger.warning("Discarding histogram values above highest bin.")

        return hist
