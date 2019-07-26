from __future__ import division
from six import PY3, string_types

import uuid
from functools import wraps
import inspect
import logging
import warnings
from datetime import datetime
from importlib import import_module

import pandas as pd
import numpy as np

from dateutil.parser import parse

from .dataset import Dataset
from .pandas_dataset import PandasDataset
from great_expectations.data_asset import DataAsset
from great_expectations.data_asset.util import DocInherit, parse_result_format

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import reflection
except ImportError:
    logger.debug("Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support")

try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None

try:
    import snowflake.sqlalchemy.snowdialect
except ImportError:
    snowflake = None


class MetaSqlAlchemyDataset(Dataset):

    def __init__(self, *args, **kwargs):
        super(MetaSqlAlchemyDataset, self).__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        """For SqlAlchemy, this decorator allows individual column_map_expectations to simply return the filter
        that describes the expected condition on their data.

        The decorator will then use that filter to obtain unexpected elements, relevant counts, and return the formatted
        object.
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, column, mostly=None, result_format=None, *args, **kwargs):
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            if result_format['result_format'] == 'COMPLETE':
                warnings.warn("Setting result format to COMPLETE for a SqlAlchemyDataset can be dangerous because it will not limit the number of returned results.")
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format['partial_unexpected_count']

            expected_condition = func(self, column, *args, **kwargs)

            # FIXME Temporary Fix for counting missing values
            # Added to compensate when an ignore_values argument is added to the expectation
            ignore_values = [None]
            if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                ignore_values = []
                # Counting the number of unexpected values can be expensive when there is a large
                # number of np.nan values.
                # This only happens on expect_column_values_to_not_be_null expectations.
                # Since there is no reason to look for most common unexpected values in this case,
                # we will instruct the result formatting method to skip this step.
                result_format['partial_unexpected_count'] = 0 

            count_query = sa.select([
                sa.func.count().label('element_count'),
                sa.func.sum(
                    sa.case([(sa.or_(
                        sa.column(column).in_(ignore_values),
                        # Below is necessary b/c sa.in_() uses `==` but None != None
                        # But we only consider this if None is actually in the list of ignore values
                        sa.column(column).is_(None) if None in ignore_values else False), 1)], else_=0)
                ).label('null_count'),
                sa.func.sum(
                    sa.case([
                        (
                            sa.and_(
                                sa.not_(expected_condition),
                                sa.case([
                                    (
                                        sa.column(column).is_(None),
                                        False
                                    )
                                ], else_=True) if None in ignore_values else True
                            ),
                            1
                        )
                    ], else_=0)
                ).label('unexpected_count')
            ]).select_from(self._table)

            count_results = dict(self.engine.execute(count_query).fetchone())

            # Handle case of empty table gracefully:
            if "element_count" not in count_results or count_results["element_count"] is None:
                count_results["element_count"] = 0
            if "null_count" not in count_results or count_results["null_count"] is None:
                count_results["null_count"] = 0
            if "unexpected_count" not in count_results or count_results["unexpected_count"] is None:
                count_results["unexpected_count"] = 0

            # Retrieve unexpected values
            unexpected_query_results = self.engine.execute(
                sa.select([sa.column(column)]).select_from(self._table).where(
                    sa.and_(sa.not_(expected_condition),
                            sa.or_(
                                # SA normally evaluates `== None` as `IS NONE`. However `sa.in_()`
                                # replaces `None` as `NULL` in the list and incorrectly uses `== NULL`
                                sa.case([
                                    (
                                        sa.column(column).is_(None),
                                        False
                                    )
                                ], else_=True) if None in ignore_values else False,
                                # Ignore any other values that are in the ignore list
                                sa.column(column).in_(ignore_values) == False))
                ).limit(unexpected_count_limit)
            )

            nonnull_count = count_results['element_count'] - \
                count_results['null_count']

            if "output_strftime_format" in kwargs:
                output_strftime_format = kwargs["output_strftime_format"]
                maybe_limited_unexpected_list = []
                for x in unexpected_query_results.fetchall():
                    if isinstance(x[column], string_types):
                        col = parse(x[column])
                    else:
                        col = x[column]
                    maybe_limited_unexpected_list.append(datetime.strftime(col, output_strftime_format))
            else:
                maybe_limited_unexpected_list = [x[column] for x in unexpected_query_results.fetchall()]

            success_count = nonnull_count - count_results['unexpected_count']
            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly)

            return_obj = self._format_map_output(
                result_format,
                success,
                count_results['element_count'],
                nonnull_count,
                count_results['unexpected_count'],
                maybe_limited_unexpected_list,
                None,
            )

            if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                # These results are unnecessary for the above expectations
                del return_obj['result']['unexpected_percent_nonmissing']
                try:
                    del return_obj['result']['partial_unexpected_counts']
                    del return_obj['result']['partial_unexpected_list']
                except KeyError:
                    pass

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper


class SqlAlchemyDataset(MetaSqlAlchemyDataset):

    @classmethod
    def from_dataset(cls, dataset=None):
        if isinstance(dataset, SqlAlchemyDataset):
            return cls(table_name=str(dataset._table.name), engine=dataset.engine)
        else:
            raise ValueError("from_dataset requires a SqlAlchemy dataset")

    def __init__(self, table_name=None, engine=None, connection_string=None,
                 custom_sql=None, schema=None, *args, **kwargs):

        if custom_sql and not table_name:
            # dashes are special characters in most databases so use undercores
            table_name = str(uuid.uuid4()).replace("-", "_")

        if table_name is None:
            raise ValueError("No table_name provided.")

        self._table = sa.Table(table_name, sa.MetaData(), schema=schema)

        if engine is None and connection_string is None:
            raise ValueError("Engine or connection_string must be provided.")

        if engine is not None:
            self.engine = engine
        else:
            try:
                self.engine = sa.create_engine(connection_string)
            except Exception as err:
                # Currently we do no error handling if the engine doesn't work out of the box.
                raise err

        # Get the dialect **for purposes of identifying types**
        if self.engine.dialect.name.lower() in ["postgresql", "mysql", "sqlite", "oracle", "mssql", "oracle"]:
            # These are the officially included and supported dialects by sqlalchemy
            self.dialect = import_module("sqlalchemy.dialects." + self.engine.dialect.name)
        elif self.engine.dialect.name.lower() == "snowflake":
            self.dialect = import_module("snowflake.sqlalchemy.snowdialect")
        elif self.engine.dialect.name.lower() == "redshift":
            self.dialect = import_module("sqlalchemy_redshift.dialect")
        else:
            self.dialect = None

        if schema is not None and custom_sql is not None:
            # temporary table will be written to temp schema, so don't allow
            # a user-defined schema
            raise ValueError("Cannot specify both schema and custom_sql.")

        if custom_sql:
            self.create_temporary_table(table_name, custom_sql)

        try:
            insp = reflection.Inspector.from_engine(self.engine)
            self.columns = insp.get_columns(table_name, schema=schema)
        except KeyError:
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            self.columns = self.column_reflection_fallback()

        # Only call super once connection is established and table_name and columns known to allow autoinspection
        super(SqlAlchemyDataset, self).__init__(*args, **kwargs)

    def head(self, n=5):
        """Returns a *PandasDataset* with the first *n* rows of the given Dataset"""
        return PandasDataset(
            pd.read_sql(
                sa.select(["*"]).select_from(self._table).limit(n),
                con=self.engine
            ), 
            expectation_suite=self.get_expectation_suite(
                discard_failed_expectations=False,
                discard_result_format_kwargs=False,
                discard_catch_exceptions_kwargs=False,
                discard_include_config_kwargs=False
            )
        )

    def get_row_count(self):
        count_query = sa.select([sa.func.count()]).select_from(
            self._table)
        return self.engine.execute(count_query).scalar()

    def get_table_columns(self):
        return [col['name'] for col in self.columns]

    def get_column_nonnull_count(self, column):
        ignore_values = [None]
        count_query = sa.select([
            sa.func.count().label('element_count'),
            sa.func.sum(
                sa.case([(sa.or_(
                    sa.column(column).in_(ignore_values),
                    # Below is necessary b/c sa.in_() uses `==` but None != None
                    # But we only consider this if None is actually in the list of ignore values
                    sa.column(column).is_(None) if None in ignore_values else False), 1)], else_=0)
            ).label('null_count'),
        ]).select_from(self._table)
        count_results = dict(self.engine.execute(count_query).fetchone())
        element_count = count_results['element_count']
        null_count = count_results['null_count'] or 0
        return element_count - null_count

    def get_column_sum(self, column):
        return self.engine.execute(
            sa.select([sa.func.sum(sa.column(column))]).select_from(
                self._table)
        ).scalar()

    def get_column_max(self, column, parse_strings_as_datetimes=False):
        if parse_strings_as_datetimes:
            raise NotImplementedError
        return self.engine.execute(
            sa.select([sa.func.max(sa.column(column))]).select_from(
                self._table)
        ).scalar()

    def get_column_min(self, column, parse_strings_as_datetimes=False):
        if parse_strings_as_datetimes:
            raise NotImplementedError
        return self.engine.execute(
            sa.select([sa.func.min(sa.column(column))]).select_from(
                self._table)
        ).scalar()
    
    def get_column_value_counts(self, column):
        results = self.engine.execute(
            sa.select([
                sa.column(column).label("value"),
                sa.func.count(sa.column(column)).label("count"),
            ]).where(sa.column(column) != None) \
              .group_by(sa.column(column)) \
              .select_from(self._table)).fetchall()
        series = pd.Series(
            [row[1] for row in results],
            index=pd.Index(
                data=[row[0] for row in results],
                name="value"
            ),
            name="count"
        )
        series.sort_index(inplace=True)
        return series

    def get_column_mean(self, column):
        return self.engine.execute(
            sa.select([sa.func.avg(sa.column(column))]).select_from(
                self._table)
        ).scalar()

    def get_column_unique_count(self, column):
        return self.engine.execute(
            sa.select([sa.func.count(sa.func.distinct(sa.column(column)))]).select_from(
                self._table)
        ).scalar()

    def get_column_median(self, column):
        nonnull_count = self.get_column_nonnull_count(column)
        element_values = self.engine.execute(
            sa.select([sa.column(column)]).order_by(sa.column(column)).where(
                sa.column(column) != None
            ).offset(max(nonnull_count // 2 - 1, 0)).limit(2).select_from(self._table)
        )

        column_values = list(element_values.fetchall())

        if len(column_values) == 0:
            column_median = None
        elif nonnull_count % 2 == 0:
            # An even number of column values: take the average of the two center values
            column_median = (
                column_values[0][0] +  # left center value
                column_values[1][0]    # right center value
            ) / 2.0  # Average center values
        else:
            # An odd number of column values, we can just take the center value
            column_median = column_values[1][0]  # True center value
        return column_median

    def get_column_quantiles(self, column, quantiles):
        selects = []
        for quantile in quantiles:
            selects.append(
                sa.func.percentile_disc(quantile).within_group(sa.column(column).asc())
            )
        quantiles = self.engine.execute(sa.select(selects).select_from(self._table)).fetchone()
        return list(quantiles)

    def get_column_stdev(self, column):
        res = self.engine.execute(sa.select([
                sa.func.stddev_samp(sa.column(column))
            ]).select_from(self._table).where(sa.column(column) != None)).fetchone()
        return float(res[0])

    def get_column_hist(self, column, bins):
        """return a list of counts corresponding to bins

        Args:
            column: the name of the column for which to get the histogram
            bins: tuple of bin edges for which to get histogram values; *must* be tuple to support caching
        """
        case_conditions = []
        idx = 0
        bins = list(bins)

        # If we have an infinte lower bound, don't express that in sql
        if (bins[0] == -np.inf) or (bins[0] == -float("inf")):
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (sa.column(column) < bins[idx+1], 1)
                        ], else_=0
                    )
                ).label("bin_" + str(idx))
            )
            idx += 1

        for idx in range(idx, len(bins)-2):
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (sa.and_(
                                bins[idx] <= sa.column(column),
                                sa.column(column) < bins[idx+1]
                            ), 1)
                        ], else_=0
                    )
                ).label("bin_" + str(idx))
            )

        if (bins[-1] == np.inf) or (bins[-1] == float("inf")):
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (bins[-2] <= sa.column(column), 1)
                        ], else_=0
                    )
                ).label("bin_" + str(len(bins)-1))
            )
        else:    
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (sa.and_(
                                bins[-2] <= sa.column(column),
                                sa.column(column) <= bins[-1]
                            ), 1)
                        ], else_=0
                    )
                ).label("bin_" + str(len(bins)-1))
            )

        query = sa.select(
            case_conditions
        )\
        .where(
            sa.column(column) != None,
        )\
        .select_from(self._table)

        hist = list(self.engine.execute(query).fetchone())
        return hist

    def get_column_count_in_range(self, column, min_val=None, max_val=None, min_strictly=False, max_strictly=True):
        if min_val is None and max_val is None:
            raise ValueError('Must specify either min or max value')
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError('Min value must be <= to max value')

        min_condition = None
        max_condition = None
        if min_val is not None:
            if min_strictly:
                min_condition = sa.column(column) > min_val
            else:
                min_condition = sa.column(column) >= min_val
        if max_val is not None:
            if max_strictly:
                max_condition = sa.column(column) < max_val
            else:
                max_condition = sa.column(column) <= max_val
        
        if min_condition is not None and max_condition is not None:
            condition = sa.and_(min_condition, max_condition)
        elif min_condition is not None:
            condition = min_condition
        else:
            condition = max_condition

        query = query = sa.select([
                    sa.func.count((sa.column(column)))
                ]) \
                .where(
                    sa.and_(
                        sa.column(column) != None,
                        condition
                    )
                ) \
                .select_from(self._table)
        
        return self.engine.execute(query).scalar()

    def create_temporary_table(self, table_name, custom_sql):
        """
        Create Temporary table based on sql query. This will be used as a basis for executing expectations.
        WARNING: this feature is new in v0.4.
        It hasn't been tested in all SQL dialects, and may change based on community feedback.
        :param custom_sql:
        """
        stmt = "CREATE TEMPORARY TABLE \"{table_name}\" AS {custom_sql}".format(
            table_name=table_name, custom_sql=custom_sql)
        self.engine.execute(stmt)

    def column_reflection_fallback(self):
        """If we can't reflect the table, use a query to at least get column names."""
        sql = sa.select([sa.text("*")]).select_from(self._table)
        col_names = self.engine.execute(sql).keys()
        col_dict = [{'name': col_name} for col_name in col_names]
        return col_dict

    ###
    ###
    ###
    #
    # Column Map Expectation Implementations
    #
    ###
    ###
    ###

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_null(self,
                                        column,
                                        mostly=None,
                                        result_format=None, include_config=False, catch_exceptions=None, meta=None
                                        ):

        return sa.column(column) == None

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_be_null(self,
                                            column,
                                            mostly=None,
                                            result_format=None, include_config=False, catch_exceptions=None, meta=None
                                            ):

        return sa.column(column) != None

    @DocInherit
    @DataAsset.expectation(['column', 'type_', 'mostly'])
    def expect_column_values_to_be_of_type(
        self,
        column,
        type_,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if mostly is not None:
            raise ValueError("SqlAlchemyDataset does not support column map semantics for column types")

        try:
            col_data = [col for col in self.columns if col["name"] == column][0]
            col_type = type(col_data["type"])
        except IndexError:
            raise ValueError("Unrecognized column: %s" % column)
        except KeyError:
            raise ValueError("No database type data available for column: %s" % column)

        try:
            # Our goal is to be as explicit as possible. We will match the dialect
            # if that is possible. If there is no dialect available, we *will*
            # match against a top-level SqlAlchemy type if that's possible.
            #
            # This is intended to be a conservative approach.
            #
            # In particular, we *exclude* types that would be valid under an ORM
            # such as "float" for postgresql with this approach

            if type_ is None:
                # vacuously true
                success = True
            else:
                if self.dialect is None:
                    logger.warning("No sqlalchemy dialect found; relying in top-level sqlalchemy types.")
                    success = issubclass(col_type, getattr(sa, type_))
                else:
                    success = issubclass(col_type, getattr(self.dialect, type_))
                
            return {
                    "success": success,
                    "result": {
                        "observed_value": col_type.__name__
                    }
                }

        except AttributeError:
            raise ValueError("Unrecognized sqlalchemy type: %s" % type_)

    @DocInherit
    @DataAsset.expectation(['column', 'type_', 'mostly'])
    def expect_column_values_to_be_in_type_list(
        self,
        column,
        type_list,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if mostly is not None:
            raise ValueError("SqlAlchemyDataset does not support column map semantics for column types")

        try:
            col_data = [col for col in self.columns if col["name"] == column][0]
            col_type = type(col_data["type"])
        except IndexError:
            raise ValueError("Unrecognized column: %s" % column)
        except KeyError:
            raise ValueError("No database type data available for column: %s" % column)

        # Our goal is to be as explicit as possible. We will match the dialect
        # if that is possible. If there is no dialect available, we *will*
        # match against a top-level SqlAlchemy type.
        #
        # This is intended to be a conservative approach.
        #
        # In particular, we *exclude* types that would be valid under an ORM
        # such as "float" for postgresql with this approach

        if type_list is None:
            success = True
        else:
            if self.dialect is None:
                logger.warning("No sqlalchemy dialect found; relying in top-level sqlalchemy types.")
                types = []
                for type_ in type_list:
                    try:
                        type_class = getattr(sa, type_)
                        types.append(type_class)
                    except AttributeError:
                        logger.debug("Unrecognized type: %s" % type_)
                if len(types) == 0:
                    logger.warning("No recognized sqlalchemy types in type_list")
                types = tuple(types)
            else:
                types = []
                for type_ in type_list:
                    try:
                        type_class = getattr(self.dialect, type_)
                        types.append(type_class)
                    except AttributeError:
                        logger.debug("Unrecognized type: %s" % type_)
                if len(types) == 0:
                    logger.warning("No recognized sqlalchemy types in type_list for dialect %s" %
                                   self.dialect.__name__)
                types = tuple(types)
            success = issubclass(col_type, types)

        return {
                "success": success,
                "result": {
                    "observed_value": col_type.__name__
                }
        }

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_in_set(self,
                                          column,
                                          value_set,
                                          mostly=None,
                                          parse_strings_as_datetimes=None,
                                          result_format=None, include_config=False, catch_exceptions=None, meta=None
                                          ):
        if value_set is None:
            # vacuously true
            return True

        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        return sa.column(column).in_(tuple(parsed_value_set))

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(self,
                                              column,
                                              value_set,
                                              mostly=None,
                                              parse_strings_as_datetimes=None,
                                              result_format=None, include_config=False, catch_exceptions=None, meta=None
                                              ):
        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        return sa.column(column).notin_(tuple(parsed_value_set))

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_between(self,
                                           column,
                                           min_value=None,
                                           max_value=None,
                                           allow_cross_type_comparisons=None,
                                           parse_strings_as_datetimes=None,
                                           output_strftime_format=None,
                                           mostly=None,
                                           result_format=None, include_config=False, catch_exceptions=None, meta=None
                                           ):
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

        if min_value != None and max_value != None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is None:
            return sa.column(column) <= max_value

        elif max_value is None:
            return min_value <= sa.column(column)

        else:
            return sa.and_(
                min_value <= sa.column(column),
                sa.column(column) <= max_value
            )

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_value_lengths_to_equal(self,
                                             column,
                                             value,
                                             mostly=None,
                                             result_format=None, include_config=False, catch_exceptions=None, meta=None
                                             ):
        return sa.func.length(sa.column(column)) == value

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_value_lengths_to_be_between(self,
                                                  column,
                                                  min_value=None,
                                                  max_value=None,
                                                  mostly=None,
                                                  result_format=None, include_config=False, catch_exceptions=None, meta=None
                                                  ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # Assert that min_value and max_value are integers
        try:
            if min_value is not None and not float(min_value).is_integer():
                raise ValueError("min_value and max_value must be integers")

            if max_value is not None and not float(max_value).is_integer():
                raise ValueError("min_value and max_value must be integers")

        except ValueError:
            raise ValueError("min_value and max_value must be integers")

        if min_value is not None and max_value is not None:
            return sa.and_(sa.func.length(sa.column(column)) >= min_value,
                           sa.func.length(sa.column(column)) <= max_value)

        elif min_value is None and max_value is not None:
            return sa.func.length(sa.column(column)) <= max_value

        elif min_value is not None and max_value is None:
            return sa.func.length(sa.column(column)) >= min_value

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_unique(self, column, mostly=None,
                                          result_format=None, include_config=False, catch_exceptions=None, meta=None):
        # Duplicates are found by filtering a group by query
        dup_query = sa.select([sa.column(column)]).\
            select_from(self._table).\
            group_by(sa.column(column)).\
            having(sa.func.count(sa.column(column)) > 1)

        return sa.column(column).notin_(dup_query)

    def _get_dialect_regex_fn(self, positive=True):
        try:
            # postgres
            if isinstance(self.engine.dialect, sa.dialects.postgresql.dialect):
                return "~" if positive else "!~"
        except AttributeError:
            pass

        try:
            # redshift
            if isinstance(self.engine.dialect, sqlalchemy_redshift.dialect.RedshiftDialect):
                return "~" if positive else "!~"
        except (AttributeError, TypeError):  # TypeError can occur if the driver was not installed and so is None
            pass
        try:
            # Mysql
            if isinstance(self.engine.dialect, sa.dialects.mysql.dialect):
                return "REGEXP" if positive else "NOT REGEXP"
        except AttributeError:
            pass

        try:
            # Snowflake
            if isinstance(self.engine.dialect, snowflake.sqlalchemy.snowdialect.SnowflakeDialect):
                return "RLIKE" if positive else "NOT RLIKE"
        except (AttributeError, TypeError):  # TypeError can occur if the driver was not installed and so is None
            pass

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_regex(
            self,
            column,
            regex,
            mostly=None,
            result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        regex_fn = self._get_dialect_regex_fn(positive=True)
        if regex_fn is None:
            logger.warning("Regex is not supported for dialect %s" % str(self.engine.dialect))
            raise NotImplementedError

        return sa.text(column + " " + regex_fn + " '" + regex + "'")

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_regex(
            self,
            column,
            regex,
            mostly=None,
            result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        regex_fn = self._get_dialect_regex_fn(positive=False)
        if regex_fn is None:
            logger.warning("Regex is not supported for dialect %s" % str(self.engine.dialect))
            raise NotImplementedError

        return sa.text(column + " " + regex_fn + " '" + regex + "'")

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_regex_list(self,
                                                 column,
                                                 regex_list,
                                                 match_on="any",
                                                 mostly=None,
                                                 result_format=None, include_config=False, catch_exceptions=None, meta=None
                                                 ):

        if match_on not in ["any", "all"]:
            raise ValueError("match_on must be any or all")

        regex_fn = self._get_dialect_regex_fn(positive=True)
        if regex_fn is None:
            logger.warning("Regex is not supported for dialect %s" % str(self.engine.dialect))
            raise NotImplementedError

        if match_on == "any":
            condition = \
                sa.or_(
                    *[sa.text(column + " " + regex_fn + " '" + regex + "'") for regex in regex_list]
                )
        else:
            condition = \
                sa.and_(
                    *[sa.text(column + " " + regex_fn + " '" + regex + "'") for regex in regex_list]
                )
        return condition

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_regex_list(self, column, regex_list,
                                                     mostly=None,
                                                     result_format=None, include_config=False, catch_exceptions=None, meta=None):

        regex_fn = self._get_dialect_regex_fn(positive=False)
        if regex_fn is None:
            logger.warning("Regex is not supported for dialect %s" % str(self.engine.dialect))
            raise NotImplementedError

        return sa.and_(
            *[sa.text(column + " " + regex_fn + " '" + regex + "'") for regex in regex_list]
        )
