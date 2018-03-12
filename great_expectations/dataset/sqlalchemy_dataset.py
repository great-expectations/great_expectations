from great_expectations.dataset import DataSet

from functools import wraps
import inspect

from .util import DocInherit, parse_result_format

from sqlalchemy import MetaData, select, table, or_, and_, not_, case
from sqlalchemy import func as sa_func
from sqlalchemy import column as sa_column
from sqlalchemy.engine import create_engine


class MetaSqlAlchemyDataSet(DataSet):

    def __init__(self, *args, **kwargs):
        super(MetaSqlAlchemyDataSet, self).__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        """TBD: How should this work?

        Firstly, of course, the full unexpected list is expensive and should only be gathered if really needed
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

            results = self.engine.execute(count_query).fetchone()

            unexpected_query_results = self.engine.execute(
                select([sa_column(column)]).select_from(table(self.table_name)).where(unexpected_condition).limit(unexpected_count_limit)
            )

            nonnull_count = results['element_count'] - results['null_count']
            partial_unexpected_list = [x[column] for x in unexpected_query_results.fetchall()]
            success_count = nonnull_count - results['unexpected_count']
            success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
                result_format, success,
                results['element_count'], nonnull_count,
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
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        unexpected_query = select([sa_column(column)]).select_from(table(self.table_name)).where(
            not_(
                sa_column(column) == None
            ))

        unexpected_count_query = select([sa_func.count()]).select_from(table(self.table_name)).where(
            not_(
                sa_column(column) == None
            ))

        return unexpected_query, unexpected_count_query

    @DocInherit
    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        unexpected_query = select([sa_column(column)]).select_from(table(self.table_name)).where(
            not_(
                sa_column(column) != None
            ))

        unexpected_count_query = select([sa_func.count()]).select_from(table(self.table_name)).where(
            not_(
                sa_column(column) != None
            ))

        return unexpected_query, unexpected_count_query

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
    def expect_column_values_to_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        # unexpected_query = select([sa_column(column)]).select_from(table(self.table_name)).where(
        #     not_(sa_column(column).in_(tuple(values_set))))
        #
        # unexpected_count_query = select([sa_func.count()]).select_from(table(self.table_name)).where(
        #     not_(sa_column(column).in_(tuple(values_set))))
        #
        # return unexpected_query, unexpected_count_query

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

        # unexpected_query = select([sa_column(column)]).select_from(table(self.table_name)).where(
        #     # and_(
        #     #     sa_column != None,
        #         not_(and_(
        #             min_value <= sa_column(column),
        #             sa_column(column) <= max_value
        #         )))
        #     # )
        #
        # unexpected_count_query = select([sa_func.count()]).select_from(table(self.table_name)).where(
        #     # and_(
        #     #     sa_column != None,
        #         not_(and_(
        #             min_value <= sa_column(column),
        #             sa_column(column) <= max_value
        #         )))
        #     # )
        #
        # return unexpected_query, unexpected_count_query

        return not_(and_(
                    min_value <= sa_column(column),
                    sa_column(column) <= max_value
            ))
