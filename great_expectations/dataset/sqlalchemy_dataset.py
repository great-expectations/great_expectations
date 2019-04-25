from __future__ import division

from functools import wraps
import inspect
from six import PY3
from six import string_types
import sys
import warnings

import sqlalchemy as sa
from sqlalchemy.engine import reflection
from dateutil.parser import parse
from datetime import datetime
from numbers import Number

if sys.version_info.major == 2:  # If python 2
    from itertools import izip_longest as zip_longest
elif sys.version_info.major == 3:  # If python 3
    from itertools import zip_longest

from .dataset import Dataset
from great_expectations.data_asset.util import DocInherit, parse_result_format

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
                result_format['partial_unexpected_count'] = 0  # Optimization to avoid meaningless computation for these expectations

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
                maybe_limited_unexpected_list = [x[column]
                                             for x in unexpected_query_results.fetchall()]

            success_count = nonnull_count - count_results['unexpected_count']
            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly)

            return_obj = self._format_map_output(
                result_format, success,
                count_results['element_count'], nonnull_count,
                count_results['unexpected_count'],
                maybe_limited_unexpected_list, None
            )

            if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                # These results are unnecessary for the above expectations
                del return_obj['result']['unexpected_percent_nonmissing']
                try:
                    del return_obj['result']['partial_unexpected_counts']
                except KeyError:
                    pass

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper

    @classmethod
    def column_aggregate_expectation(cls, func):
        """Constructs an expectation using column-aggregate semantics.
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, column, result_format=None, *args, **kwargs):

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            evaluation_result = func(self, column, *args, **kwargs)

            if 'success' not in evaluation_result:
                raise ValueError(
                    "Column aggregate expectation failed to return required information: success")

            if 'result' not in evaluation_result:
                raise ValueError(
                    "Column aggregate expectation failed to return required information: result")

            if 'observed_value' not in evaluation_result['result']:
                raise ValueError(
                    "Column aggregate expectation failed to return required information: result.observed_value")

            return_obj = {
                'success': bool(evaluation_result['success'])
            }

            if result_format['result_format'] == 'BOOLEAN_ONLY':
                return return_obj

            # Use the element and null count information from a column_aggregate_expectation if it is needed
            # it anyway to avoid an extra trip to the database

            if 'element_count' not in evaluation_result and 'null_count' not in evaluation_result:
                count_query = sa.select([
                    sa.func.count().label('element_count'),
                    sa.func.sum(
                        sa.case([(sa.column(column) == None, 1)], else_=0)
                    ).label('null_count'),
                ]).select_from(self._table)

                count_results = dict(
                    self.engine.execute(count_query).fetchone())

                # Handle case of empty table gracefully:
                if "element_count" not in count_results or count_results["element_count"] is None:
                    count_results["element_count"] = 0
                if "null_count" not in count_results or count_results["null_count"] is None:
                    count_results["null_count"] = 0

                return_obj['result'] = {
                    'observed_value': evaluation_result['result']['observed_value'],
                    "element_count": count_results['element_count'],
                    "missing_count": count_results['null_count'],
                    "missing_percent": count_results['null_count'] / count_results['element_count'] if count_results['element_count'] > 0 else None
                }
            else:
                return_obj['result'] = {
                    'observed_value': evaluation_result['result']['observed_value'],
                    "element_count": evaluation_result["element_count"],
                    "missing_count": evaluation_result["null_count"],
                    "missing_percent": evaluation_result['null_count'] / evaluation_result['element_count'] if evaluation_result['element_count'] > 0 else None
                }

            if result_format['result_format'] == 'BASIC':
                return return_obj

            if 'details' in evaluation_result['result']:
                return_obj['result']['details'] = evaluation_result['result']['details']

            if result_format['result_format'] in ["SUMMARY", "COMPLETE"]:
                return return_obj

            raise ValueError("Unknown result_format %s." %
                             (result_format['result_format'],))

        return inner_wrapper


class SqlAlchemyDataset(MetaSqlAlchemyDataset):

    def __init__(self, table_name=None, engine=None, connection_string=None,
                 custom_sql=None, schema=None, *args, **kwargs):
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

        if schema is not None and custom_sql is not None:
            # temporary table will be written to temp schema, so don't allow
            # a user-defined schema
            raise ValueError("Cannot specify both schema and custom_sql.")

        if custom_sql:
            self.create_temporary_table(table_name, custom_sql)


        try:
            insp = reflection.Inspector.from_engine(engine)
            self.columns = insp.get_columns(table_name, schema=schema)
        except KeyError:
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            self.columns = self.column_reflection_fallback()

        # Only call super once connection is established and table_name and columns known to allow autoinspection
        super(SqlAlchemyDataset, self).__init__(*args, **kwargs)

    def create_temporary_table(self, table_name, custom_sql):
        """
        Create Temporary table based on sql query. This will be used as a basis for executing expectations.
        WARNING: this feature is new in v0.4.
        It hasn't been tested in all SQL dialects, and may change based on community feedback.
        :param custom_sql:
        """
        stmt = "CREATE TEMPORARY TABLE {table_name} AS {custom_sql}".format(
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
    # Table level implementations
    #
    ###
    ###
    ###

    @DocInherit
    @Dataset.expectation(['value'])
    def expect_table_row_count_to_equal(self,
                                        value=None,
                                        result_format=None, include_config=False, catch_exceptions=None, meta=None
                                        ):
        # Assert that min_value and max_value are integers
        try:
            if value is not None:
                float(value).is_integer()

        except ValueError:
            raise ValueError("value must an integer")

        if value is None:
            raise ValueError("value must be provided")

        count_query = sa.select([sa.func.count()]).select_from(
            self._table)
        row_count = self.engine.execute(count_query).scalar()

        return {
            'success': row_count == value,
            'result': {
                'observed_value': row_count
            }
        }

    @DocInherit
    @Dataset.expectation(['min_value', 'max_value'])
    def expect_table_row_count_to_be_between(self,
                                             min_value=0,
                                             max_value=None,
                                             result_format=None, include_config=False, catch_exceptions=None, meta=None
                                             ):
        # Assert that min_value and max_value are integers
        try:
            if min_value is not None:
                float(min_value).is_integer()

            if max_value is not None:
                float(max_value).is_integer()

        except ValueError:
            raise ValueError("min_value and max_value must be integers")

        count_query = sa.select([sa.func.count()]).select_from(
            self._table)
        row_count = self.engine.execute(count_query).scalar()

        if min_value != None and max_value != None:
            outcome = row_count >= min_value and row_count <= max_value

        elif min_value == None and max_value != None:
            outcome = row_count <= max_value

        elif min_value != None and max_value == None:
            outcome = row_count >= min_value

        return {
            'success': outcome,
            'result': {
                'observed_value': row_count
            }
        }

    @DocInherit
    @Dataset.expectation(['column_list'])
    def expect_table_columns_to_match_ordered_list(self, column_list,
                                                   result_format=None, include_config=False, catch_exceptions=None, meta=None):
        """
        Checks if observed columns are in the expected order. The expectations will fail if columns are out of expected
        order, columns are missing, or additional columns are present. On failure, details are provided on the location
        of the unexpected column(s).
        """
        observed_columns = [col['name'] for col in self.columns]

        if observed_columns == list(column_list):
            return {
                "success": True
            }
        else:
            # In the case of differing column lengths between the defined expectation and the observed column set, the
            # max is determined to generate the column_index.
            number_of_columns = max(len(column_list), len(observed_columns))
            column_index = range(number_of_columns)

            # Create a list of the mismatched details
            compared_lists = list(zip_longest(column_index, list(column_list), observed_columns))
            mismatched = [{"Expected Column Position": i,
                           "Expected": k,
                           "Found": v} for i, k, v in compared_lists if k != v]
            return {
                "success": False,
                "details": {"mismatched": mismatched}
            }

    @DocInherit
    @Dataset.expectation(['column'])
    def expect_column_to_exist(self,
                               column, column_index=None, result_format=None, include_config=False,
                               catch_exceptions=None, meta=None
                               ):

        col_names = [col['name'] for col in self.columns]

        if column_index is None:
            success = column in col_names
        else:
            try:
                col_index = col_names.index(column)
                success = (column_index == col_index)
            except ValueError:
                success = False

        return {
            'success': success
        }

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
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_in_set(self,
                                          column,
                                          value_set,
                                          mostly=None,
                                          parse_strings_as_datetimes=None,
                                          result_format=None, include_config=False, catch_exceptions=None, meta=None
                                          ):
        if parse_strings_as_datetimes:
            parsed_value_set = [parse(value) if isinstance(value, string_types) else value for value in value_set]
        else:
            parsed_value_set = value_set
        return sa.column(column).in_(tuple(parsed_value_set))

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(self,
                                              column,
                                              value_set,
                                              mostly=None,
                                              result_format=None, include_config=False, catch_exceptions=None, meta=None
                                              ):
        return sa.column(column).notin_(tuple(value_set))

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

    ###
    ###
    ###
    #
    # Column Aggregate Expectation Implementations
    #
    ###
    ###
    ###

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_max_to_be_between(self,
                                        column,
                                        min_value=None,
                                        max_value=None,
                                        parse_strings_as_datetimes=None,
                                        output_strftime_format=None,
                                        result_format=None, include_config=False, catch_exceptions=None, meta=None
                                        ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        col_max = self.engine.execute(
            sa.select([sa.func.max(sa.column(column))]).select_from(
                self._table)
        ).scalar()

        col_max_out = col_max
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            if isinstance(col_max, string_types):
                col_max = parse(col_max)

            if output_strftime_format:
                col_max_out = datetime.strftime(col_max, output_strftime_format)

        # Handle possible missing values
        if col_max is None:
            return {
                'success': False,
                'result': {
                    'observed_value': col_max_out
                }
            }
        else:
            if min_value is not None and max_value is not None:
                success = (min_value <= col_max) and (col_max <= max_value)

            elif min_value is None and max_value is not None:
                success = (col_max <= max_value)

            elif min_value is not None and max_value is None:
                success = (min_value <= col_max)

            return {
                'success': success,
                'result': {
                    'observed_value': col_max_out
                }
            }

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_min_to_be_between(self,
                                        column,
                                        min_value=None,
                                        max_value=None,
                                        parse_strings_as_datetimes=None,
                                        output_strftime_format=None,
                                        result_format=None, include_config=False, catch_exceptions=None, meta=None
                                        ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        col_min = self.engine.execute(
            sa.select([sa.func.min(sa.column(column))]).select_from(
                self._table)
        ).scalar()

        col_min_out = col_min
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            if isinstance(col_min, string_types):
                col_min = parse(col_min)

            if output_strftime_format:
                col_min_out = datetime.strftime(col_min, output_strftime_format)


        # Handle possible missing values
        if col_min is None:
            return {
                'success': False,
                'result': {
                    'observed_value': col_min_out
                }
            }
        else:
            if min_value is not None and max_value is not None:
                success = (min_value <= col_min) and (col_min <= max_value)

            elif min_value is None and max_value is not None:
                success = (col_min <= max_value)

            elif min_value is not None and max_value is None:
                success = (min_value <= col_min)

            return {
                'success': success,
                'result': {
                    'observed_value': col_min_out
                }
            }

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_sum_to_be_between(self,
                                        column,
                                        min_value=None,
                                        max_value=None,
                                        result_format=None, include_config=False, catch_exceptions=None, meta=None
                                        ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        col_sum = self.engine.execute(
            sa.select([sa.func.sum(sa.column(column))]).select_from(
                self._table)
        ).scalar()

        # Handle possible missing values
        if col_sum is None:
            return {
                'success': False,
                'result': {
                    'observed_value': col_sum
                }
            }
        else:
            if min_value is not None and max_value is not None:
                success = (min_value <= col_sum) and (col_sum <= max_value)

            elif min_value is None and max_value is not None:
                success = (col_sum <= max_value)

            elif min_value is not None and max_value is None:
                success = (min_value <= col_sum)

            return {
                'success': success,
                'result': {
                    'observed_value': col_sum
                }
            }

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_mean_to_be_between(self,
                                         column,
                                         min_value=None,
                                         max_value=None,
                                         result_format=None, include_config=False, catch_exceptions=None, meta=None
                                         ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is not None and not isinstance(min_value, (Number)):
            raise ValueError("min_value must be a number")

        if max_value is not None and not isinstance(max_value, (Number)):
            raise ValueError("max_value must be a number")

        col_avg = self.engine.execute(
            sa.select([sa.func.avg(sa.column(column))]).select_from(
                self._table)
        ).scalar()

        # Handle possible missing values
        if col_avg is None:
            return {
                'success': False,
                'result': {
                    'observed_value': col_avg
                }
            }
        else:
            if min_value != None and max_value != None:
                success = (min_value <= col_avg) and (col_avg <= max_value)

            elif min_value == None and max_value != None:
                success = (col_avg <= max_value)

            elif min_value != None and max_value == None:
                success = (min_value <= col_avg)

            return {
                'success': success,
                'result': {
                    'observed_value': col_avg
                }
            }

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_median_to_be_between(self,
                                           column,
                                           min_value=None,
                                           max_value=None,
                                           result_format=None, include_config=False, catch_exceptions=None, meta=None
                                           ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # Inspiration from https://stackoverflow.com/questions/942620/missing-median-aggregate-function-in-django
        count_query = self.engine.execute(
            sa.select([
                sa.func.count().label("element_count"),
                sa.func.sum(
                    sa.case([(sa.column(column) == None, 1)], else_=0)
                ).label('null_count')
            ]).select_from(self._table)
        )

        counts = dict(count_query.fetchone())

        # Handle empty counts
        if "element_count" not in counts or counts["element_count"] is None:
            counts["element_count"] = 0
        if "null_count" not in counts or counts["null_count"] is None:
            counts["null_count"] = 0

        # The number of non-null/non-ignored values
        nonnull_count = counts['element_count'] - counts['null_count']

        element_values = self.engine.execute(
            sa.select([sa.column(column)]).order_by(sa.column(column)).where(
                sa.column(column) != None
            ).offset(max(nonnull_count // 2 - 1, 0)).limit(2).select_from(self._table)
        )

        column_values = list(element_values.fetchall())

        if len(column_values) == 0:
            return {
                'success': False,
                'result': {
                    'observed_value': None
                }
            }
        else:
            if nonnull_count % 2 == 0:
                # An even number of column values: take the average of the two center values
                column_median = (
                    column_values[0][0] +  # left center value
                    column_values[1][0]        # right center value
                ) / 2.0  # Average center values
            else:
                # An odd number of column values, we can just take the center value
                column_median = column_values[1][0]  # True center value

            return {
                'success':
                    ((min_value is None) or (min_value <= column_median)) and
                    ((max_value is None) or (column_median <= max_value)),
                'result': {
                    'observed_value': column_median
                }
            }

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_unique(self, column, mostly=None,
                                          result_format=None, include_config=False, catch_exceptions=None, meta=None):
        # Duplicates are found by filtering a group by query
        dup_query = sa.select([sa.column(column)]).\
            select_from(self._table).\
            group_by(sa.column(column)).\
            having(sa.func.count(sa.column(column)) > 1)

        return sa.column(column).notin_(dup_query)

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_unique_value_count_to_be_between(self, column, min_value=None, max_value=None,
                                                       result_format=None, include_config=False, catch_exceptions=None, meta=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        unique_value_count = self.engine.execute(
            sa.select([sa.func.count(sa.func.distinct(sa.column(column)))]).select_from(
                self._table)
        ).scalar()

        # Handle possible missing values
        if unique_value_count is None:
            return {
                'success': False,
                'result': {
                    'observed_value': unique_value_count
                }
            }
        else:
            return {
                "success": (
                    ((min_value is None) or (min_value <= unique_value_count)) and
                    ((max_value is None) or (unique_value_count <= max_value))
                ),
                "result": {
                    "observed_value": unique_value_count
                }
            }

    @DocInherit
    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value=0, max_value=1,
                                                                result_format=None, include_config=False, catch_exceptions=None, meta=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        count_query = self.engine.execute(
            sa.select([
                sa.func.count().label('element_count'),
                sa.func.sum(
                    sa.case([(sa.column(column) == None, 1)], else_=0)
                ).label('null_count'),
                sa.func.count(sa.func.distinct(sa.column(column))
                              ).label('unique_value_count')
            ]).select_from(self._table)
        )

        counts = count_query.fetchone()

        if counts['element_count'] - counts['null_count'] > 0:
            proportion_unique = counts['unique_value_count'] / \
                (counts['element_count'] - counts['null_count'])
        else:
            proportion_unique = None

        return {
            "success": (
                ((min_value is None) or (min_value <= proportion_unique)) and
                ((max_value is None) or (proportion_unique <= max_value))
            ),
            "element_count": counts["element_count"],
            "null_count": counts["null_count"],
            "result": {
                "observed_value": proportion_unique
            }
        }
