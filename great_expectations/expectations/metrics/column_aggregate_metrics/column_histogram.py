import copy
import logging
import traceback
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from great_expectations.core.util import (
    convert_to_json_serializable,
    get_sql_dialect_floating_point_infinity_value,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
)
from great_expectations.expectations.metrics.import_manager import Bucketizer, F, sa
from great_expectations.expectations.metrics.metric_provider import metric_value

# TODO: <Alex>ALEX</Alex>
logger = logging.getLogger(__name__)
# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
try:
    from trino.exceptions import TrinoUserError
except ImportError:
    TrinoUserError = None

try:
    from sqlalchemy.exc import ProgrammingError
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.elements import Label, TextClause, WithinGroup
    from sqlalchemy.sql.selectable import CTE
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    ProgrammingError = None
    Select = None
    Label = None
    TextClause = None
    WithinGroup = None
    CTE = None

try:
    from sqlalchemy.engine.row import Row
except ImportError:
    try:
        from sqlalchemy.engine.row import RowProxy

        Row = RowProxy
    except ImportError:
        logger.debug(
            "Unable to load SqlAlchemy Row class; please upgrade you sqlalchemy installation to the latest version."
        )
        RowProxy = None
        Row = None
# TODO: <Alex>ALEX</Alex>


class ColumnHistogram(ColumnAggregateMetricProvider):
    metric_name = "column.histogram"
    value_keys = ("bins",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
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
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
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

        case_conditions = []
        idx = 0
        # TODO: <Alex>ALEX</Alex>
        if isinstance(bins, np.ndarray):
            bins = bins.tolist()
        elif isinstance(bins, int):
            print(
                f"\n[ALEX_TEST] [WOUTPUT] ORIGINAL_BINS:\n{bins} ; TYPE: {str(type(bins))}"
            )
            sqlalchemy_engine = execution_engine.engine
            bins_query: Select = (
                sa.select(sa.column(column))
                .distinct()
                .where(sa.column(column) != None)
                .order_by(sa.column(column).asc())
                .select_from(selectable)
            )
            try:
                bins_results: List[Row] = sqlalchemy_engine.execute(
                    bins_query
                ).fetchall()
                print(
                    f"\n[ALEX_TEST] [WOUTPUT] BINS_RESULTS:\n{bins_results} ; TYPE: {str(type(bins_results))}"
                )
                print(
                    f"\n[ALEX_TEST] [WOUTPUT] BINS_RESULTS[0]:\n{bins_results[0]} ; TYPE: {str(type(bins_results[0]))}"
                )
                bin_result: Row
                bins = np.asarray([bin_result[0] for bin_result in bins_results])
                print(
                    f"\n[ALEX_TEST] [WOUTPUT] COMPUTED_BINS:\n{bins} ; TYPE: {str(type(bins))}"
                )
                # TODO: <Alex>ALEX</Alex>
                bins = np.histogram_bin_edges(bins, bins="auto", range=(0, 1))
                bins = bins.tolist()
                print(
                    f"\n[ALEX_TEST] [WOUTPUT] FINAL_BINS:\n{bins} ; TYPE: {str(type(bins))}"
                )
                # TODO: <Alex>ALEX</Alex>
            except ProgrammingError as pe:
                exception_message: str = "An SQL syntax Exception occurred."
                exception_traceback: str = traceback.format_exc()
                exception_message += f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
                logger.error(exception_message)
                raise pe
        else:
            bins = list(bins)
        # TODO: <Alex>ALEX</Alex>

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
                    sa.case([(sa.column(column) < bins[idx + 1], 1)], else_=0)
                ).label(f"bin_{str(idx)}")
            )
            idx += 1

        for idx in range(idx, len(bins) - 2):
            # print(f'\n[ALEX_TEST] [WOUTPUT] BINS[{idx}]:\n{bins[idx]} ; TYPE: {str(type(bins[idx]))}')
            # print(f'\n[ALEX_TEST] [WOUTPUT] COLUMN:\n{column} ; TYPE: {str(type(column))}')
            # print(f'\n[ALEX_TEST] [WOUTPUT] SA.COLUMN(COLUMN):\n{sa.column(column)} ; TYPE: {str(type(sa.column(column)))}')
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.and_(
                                    bins[idx] <= sa.column(column),
                                    sa.column(column) < bins[idx + 1],
                                ),
                                1,
                            )
                        ],
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
            case_conditions.append(
                sa.func.sum(
                    sa.case([(bins[-2] <= sa.column(column), 1)], else_=0)
                ).label(f"bin_{str(len(bins) - 1)}")
            )
        else:
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.and_(
                                    bins[-2] <= sa.column(column),
                                    sa.column(column) <= bins[-1],
                                ),
                                1,
                            )
                        ],
                        else_=0,
                    )
                ).label(f"bin_{str(len(bins) - 1)}")
            )

        query = (
            sa.select(case_conditions)
            .where(
                sa.column(column) != None,
            )
            .select_from(selectable)
        )

        # Run the data through convert_to_json_serializable to ensure we do not have Decimal types
        hist = convert_to_json_serializable(
            list(execution_engine.engine.execute(query).fetchone())
        )
        return hist

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
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
        bucketizer = Bucketizer(splits=bins, inputCol=column, outputCol="buckets")
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
