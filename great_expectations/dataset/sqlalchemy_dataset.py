from great_expectations.dataset import DataSet

from functools import wraps
import inspect

from .util import DocInherit, parse_result_format

import sqlalchemy as sa
from sqlalchemy.engine import reflection

from numbers import Number


class MetaSqlAlchemyDataSet(DataSet):

    def __init__(self, *args, **kwargs):
        super(MetaSqlAlchemyDataSet, self).__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        """For SqlAlchemy, this decorator allows individual column_map_expectations to simply return the filter
        that describes the expected condition on their data.

        The decorator will then use that filter to obtain unexpected elements, relevant counts, and return the formatted
        object.
        """

        @cls.expectation(inspect.getargspec(func)[0][1:])
        @wraps(func)
        def inner_wrapper(self, column, mostly=None, result_format=None, *args, **kwargs):
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            if result_format['result_obj_format'] == 'COMPLETE':
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format['partial_unexpected_count']

            expected_condition = func(self, column, *args, **kwargs)

            count_query = sa.select([
                sa.func.count().label('element_count'),
                sa.func.sum(
                    sa.case([(sa.column(column) == None, 1)], else_=0)
                ).label('null_count'),
                sa.func.sum(
                    sa.case([(sa.not_(expected_condition), 1)], else_=0)
                ).label('unexpected_count')
            ]).select_from(sa.table(self.table_name))

            count_results = self.engine.execute(count_query).fetchone()

            unexpected_query_results = self.engine.execute(
                sa.select([sa.column(column)]).select_from(sa.table(self.table_name)).where(sa.not_(expected_condition)).limit(unexpected_count_limit)
            )

            nonnull_count = count_results['element_count'] - count_results['null_count']
            maybe_limited_unexpected_list = [x[column] for x in unexpected_query_results.fetchall()]
            success_count = nonnull_count - count_results['unexpected_count']
            success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
                result_format, success,
                count_results['element_count'], nonnull_count,
                maybe_limited_unexpected_list, None
            )

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper


    @classmethod
    def column_aggregate_expectation(cls, func):
        """Constructs an expectation using column-aggregate semantics.
        """
        @cls.expectation(inspect.getargspec(func)[0][1:])
        @wraps(func)
        def inner_wrapper(self, column, result_format = None, *args, **kwargs):

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            evaluation_result = func(self, column, *args, **kwargs)

            if 'success' not in evaluation_result:
                raise ValueError("Column aggregate expectation failed to return required information: success")

            if ('result_obj' not in evaluation_result) or ('observed_value' not in evaluation_result['result_obj']):
                raise ValueError("Column aggregate expectation failed to return required information: observed_value")

            return_obj = {
                'success': bool(evaluation_result['success'])
            }

            if result_format['result_obj_format'] == 'BOOLEAN_ONLY':
                return return_obj

            count_query = sa.select([
                sa.func.count().label('element_count'),
                sa.func.sum(
                    sa.case([(sa.column(column) == None, 1)], else_=0)
                ).label('null_count'),
            ]).select_from(sa.table(self.table_name))

            count_results = self.engine.execute(count_query).fetchone()

            return_obj['result_obj'] = {
                'observed_value': evaluation_result['result_obj']['observed_value'],
                "element_count": count_results['element_count'],
                "missing_count": count_results['null_count'],
                "missing_percent": count_results['null_count'] * 1.0 / count_results['element_count'] if count_results['element_count'] > 0 else None
            }

            if result_format['result_obj_format'] == 'BASIC':
                return return_obj

            if 'details' in evaluation_result['result_obj']:
                return_obj['result_obj']['details'] = evaluation_result['result_obj']['details']

            if result_format['result_obj_format'] in ["SUMMARY", "COMPLETE"]:
                return return_obj

            raise ValueError("Unknown result_format %s." % (result_format['result_obj_format'],))

        return inner_wrapper


class SqlAlchemyDataSet(MetaSqlAlchemyDataSet):

    def __init__(self, table_name=None, engine=None, connection_string=None):
        super(SqlAlchemyDataSet, self).__init__()

        if table_name is None:
            raise ValueError("No table_name provided.")

        self.table_name = table_name

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

        insp = reflection.Inspector.from_engine(engine)
        self.columns = insp.get_columns(self.table_name)

    def add_default_expectations(self):
        """
        The default behavior for PandasDataSet is to explicitly include expectations that every column present upon initialization exists.
        """
        for col in self.columns:
            self.append_expectation({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": col["name"]
                }
            })

    def _is_numeric_column(self, column):
        for col in self.columns:
            if (col['name'] == column and
                isinstance(col['type'],
                           (sa.types.Integer, sa.types.BigInteger, sa.types.Float, sa.types.Numeric, sa.types.SmallInteger, sa.types.Boolean)
                           )
            ):
                return True

        return False

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
    @DataSet.expectation(['value'])
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

        count_query = sa.select([sa.func.count()]).select_from(sa.table(self.table_name))
        row_count = self.engine.execute(count_query).scalar()

        return {
            'success': row_count == value,
            'result_obj': {
                'observed_value': row_count
            }
        }

    @DocInherit
    @DataSet.expectation(['min_value', 'max_value'])
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

        count_query = sa.select([sa.func.count()]).select_from(sa.table(self.table_name))
        row_count = self.engine.execute(count_query).scalar()

        if min_value != None and max_value != None:
            outcome = row_count >= min_value and row_count <= max_value

        elif min_value == None and max_value != None:
            outcome = row_count <= max_value

        elif min_value != None and max_value == None:
            outcome = row_count >= min_value

        return {
            'success': outcome,
            'result_obj': {
                'observed_value': row_count
            }
        }

    @DocInherit
    @DataSet.expectation(['column'])
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
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        return sa.column(column) == None

    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        return sa.column(column) != None


    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        return sa.column(column).in_(tuple(values_set))

    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        allow_cross_type_comparisons=None,
        parse_strings_as_datetimes=None,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if parse_strings_as_datetimes is not None:
            raise ValueError("parse_strings_as_datetimes is not currently supported in SqlAlchemy.")

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
    @MetaSqlAlchemyDataSet.column_aggregate_expectation
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

        if parse_strings_as_datetimes:
            raise ValueError("parse_strings_as_datetimes is not supported in SqlAlchemy")

        col_max = self.engine.execute(
            sa.select([sa.func.max(sa.column(column))]).select_from(sa.table(self.table_name))
        ).scalar()

        if min_value != None and max_value != None:
            success = (min_value <= col_max) and (col_max <= max_value)

        elif min_value == None and max_value != None:
            success = (col_max <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_max)

        return {
            'success' : success,
            'result_obj': {
                'observed_value' : col_max
            }
        }


    @DocInherit
    @MetaSqlAlchemyDataSet.column_aggregate_expectation
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

        if parse_strings_as_datetimes:
            raise ValueError("parse_strings_as_datetimes is not supported in SqlAlchemy")

        col_min = self.engine.execute(
            sa.select([sa.func.min(sa.column(column))]).select_from(sa.table(self.table_name))
        ).scalar()

        if min_value != None and max_value != None:
            success = (min_value <= col_min) and (col_min <= max_value)

        elif min_value == None and max_value != None:
            success = (col_min <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_min)

        return {
            'success' : success,
            'result_obj': {
                'observed_value' : col_min
            }
        }

    @DocInherit
    @MetaSqlAlchemyDataSet.column_aggregate_expectation
    def expect_column_sum_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        col_sum = self.engine.execute(
            sa.select([sa.func.sum(sa.column(column))]).select_from(sa.table(self.table_name))
        ).scalar()

        if min_value != None and max_value != None:
            success = (min_value <= col_sum) and (col_sum <= max_value)

        elif min_value == None and max_value != None:
            success = (col_sum <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_sum)

        return {
            'success' : success,
            'result_obj': {
                'observed_value' : col_sum
            }
        }

    @DocInherit
    @MetaSqlAlchemyDataSet.column_aggregate_expectation
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

        if not self._is_numeric_column(column):
            raise ValueError("column is not numeric")

        col_avg = self.engine.execute(
            sa.select([sa.func.avg(sa.column(column))]).select_from(sa.table(self.table_name))
        ).scalar()

        if min_value != None and max_value != None:
            success = (min_value <= col_avg) and (col_avg <= max_value)

        elif min_value == None and max_value != None:
            success = (col_avg <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_avg)

        return {
            'success': success,
            'result_obj': {
                'observed_value': col_avg
            }
        }