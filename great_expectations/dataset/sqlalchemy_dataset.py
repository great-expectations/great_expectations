from great_expectations.dataset import DataSet

from functools import wraps
import inspect

from sqlalchemy import MetaData, select, table, or_, and_
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

            count_query = select([sa_func.count()]).select_from(table(self.table_name))
            null_count_query = select([sa_func.count()]).select_from(table(self.table_name)).where(
                sa_column(column) == None)

            null_count = self.engine.execute(null_count_query).scalar()
            element_count = self.engine.execute(count_query).scalar()

            nonnull_count = element_count - null_count

            result = func(self, column, *args, **kwargs)

            success_count = nonnull_count - result['unexpected_count']

            success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
                result_format, success,
                element_count, nonnull_count,
                result['partial_unexpected_list'], None
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
        self.unexpected_count_limit = 20

    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        test = select([sa_column(column)]).select_from(table(self.table_name)).where(
            sa_column(column).notin_(tuple(values_set)))

        test_count = select([sa_func.count()]).select_from(table(self.table_name)).where(
            sa_column(column).notin_(tuple(values_set)))

        test_results = self.engine.execute(test.limit(self.unexpected_count_limit))

        unexpected_count = self.engine.execute(test_count).scalar()
        partial_unexpected_list = [x[column] for x in test_results.fetchall()]

        return {
            'unexpected_count': unexpected_count,
            # element_count (above)
            # missing_count (above)
            # missing_percent (above)
            # unexpected_percent (computable)
            # unexpected_percent_nonmissing (computable)
            'partial_unexpected_list': partial_unexpected_list
            # partial_unexpected_index_list
            # partial_unexpected_counts
            # unexpected_index_list
            # unexpected_list

            # We will NOT support:
            # - partial_unexpected_index_list (no index concept guaranteed in sql)
            # - unexpected_index_list (no index concept guaranteed in sql)
        }

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
        if min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        unexpected = select([sa_column(column)]).select_from(table(self.table_name)).where(or_(
            min_value > sa_column(column),
            sa_column(column) > max_value
            ))

        unexpected_count = select([sa_func.count()]).select_from(table(self.table_name)).where(or_(
            min_value > sa_column(column),
            sa_column(column) > max_value
            ))

        unexpected_limited = self.engine.execute(unexpected.limit(self.unexpected_count_limit))
        unexpected_count = self.engine.execute(unexpected_count).scalar()
        partial_unexpected_list = [x[column] for x in unexpected_limited.fetchall()]

        return {
            'unexpected_count': unexpected_count,
            'partial_unexpected_list': partial_unexpected_list
        }

    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        unexpected = select([sa_column(column)]).select_from(table(self.table_name)).where(
            sa_column(column) != None
        )
        unexpected_count = select([sa_func.count()]).select_from(table(self.table_name)).where(
            sa_column(column) != None
        )

        unexpected_limited = self.engine.execute(unexpected.limit(self.unexpected_count_limit))
        unexpected_count = self.engine.execute(unexpected_count).scalar()

        partial_unexpected_list = [x[column] for x in unexpected_limited.fetchall()]

        return {
            'unexpected_count': unexpected_count,
            'partial_unexpected_list': partial_unexpected_list
        }

    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        unexpected = select([sa_column(column)]).select_from(table(self.table_name)).where(
            sa_column(column) == None
        )
        unexpected_count = select([sa_func.count()]).select_from(table(self.table_name)).where(
            sa_column(column) == None
        )

        unexpected_limited = self.engine.execute(unexpected.limit(self.unexpected_count_limit))
        unexpected_count = self.engine.execute(unexpected_count).scalar()

        partial_unexpected_list = [x[column] for x in unexpected_limited.fetchall()]

        return {
            'unexpected_count': unexpected_count,
            'partial_unexpected_list': partial_unexpected_list
        }


    @MetaSqlAlchemyDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):

        unexpected = select([sa_column(column)]).select_from(table(self.table_name)).where(
            sa_column(column) == None
        )
        unexpected_count = select([sa_func.count()]).select_from(table(self.table_name)).where(
            sa_column(column) == None
        )

        unexpected_limited = self.engine.execute(unexpected.limit(self.unexpected_count_limit))
        unexpected_count = self.engine.execute(unexpected_count).scalar()

        partial_unexpected_list = [x[column] for x in unexpected_limited.fetchall()]

        return {
            'unexpected_count': unexpected_count,
            'partial_unexpected_list': partial_unexpected_list
        }