from great_expectations.dataset import DataSet

from functools import wraps
import inspect

from .util import DocInherit, parse_result_format

from sqlalchemy import MetaData, select, table, or_, and_, not_, case
from sqlalchemy import func as sa_func
from sqlalchemy import column as sa_column
from sqlalchemy.engine import reflection


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
            else:
                result_format = parse_result_format(result_format)

            if 'partial_unexpected_count' in result_format:
                unexpected_count_limit = result_format['partial_unexpected_count']
            else:
                if result_format['result_obj_format'] == 'COMPLETE':
                    unexpected_count_limit = None
                else:
                    unexpected_count_limit = 20

            unexpected_condition = func(self, column, *args, **kwargs)

            count_query = select([
                sa_func.count().label('element_count'),
                sa_func.sum(
                    case([(sa_column(column) == None, 1)], else_=0)
                ).label('null_count'),
                sa_func.sum(
                    case([(unexpected_condition, 1)], else_=0)
                ).label('unexpected_count')
            ]).select_from(table(self.table_name))

            count_results = self.engine.execute(count_query).fetchone()

            unexpected_query_results = self.engine.execute(
                select([sa_column(column)]).select_from(table(self.table_name)).where(unexpected_condition).limit(unexpected_count_limit)
            )

            nonnull_count = count_results['element_count'] - count_results['null_count']
            partial_unexpected_list = [x[column] for x in unexpected_query_results.fetchall()]
            success_count = nonnull_count - count_results['unexpected_count']
            success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
                result_format, success,
                count_results['element_count'], nonnull_count,
                partial_unexpected_list, None
            )

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper




class SqlAlchemyDataSet(MetaSqlAlchemyDataSet):

    # def __init__(self, connection_string, table_name):
    def __init__(self, engine, table_name):
        super(SqlAlchemyDataSet, self).__init__()
        # We are intentionally not adding default expectations here, thinking about the future of non-tabular datasets
        self.table_name = table_name
        # self.engine = create_engine(connection_string)
        self.engine = engine

        insp = reflection.Inspector.from_engine(engine)
        self.columns = insp.get_columns(self.table_name)



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

        count_query = select([sa_func.count()]).select_from(table(self.table_name))
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

        count_query = select([sa_func.count()]).select_from(table(self.table_name))
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
            column,
            result_format=None, include_config=False, catch_exceptions=None, meta=None
        ):

        col_names = [col['name'] for col in self.columns]

        return {
            'success': column in col_names
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

        return sa_column(column) != None

    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        return sa_column(column) == None


    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        return not_(sa_column(column).in_(tuple(values_set)))

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

        if min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        return not_(and_(
                    min_value <= sa_column(column),
                    sa_column(column) <= max_value
            ))


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
    @DataSet.expectation(['column', 'min_value', 'max_value', 'parse_strings_as_datetimes', 'output_strftime_format'])
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
            select([sa_func.max(sa_column(column))]).select_from(table(self.table_name))
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