import warnings
from .base import DatasetProfiler


class BasicDatasetProfiler(DatasetProfiler):
    """BasicDatasetProfiler is inspired by the beloved pandas_profiling project.

    The profiler examines a batch of data and creates a report that answers the basic questions
    most data practitioners would ask about a dataset during exploratory data analysis.
    The profiler reports how unique the values in the column are, as well as the percentage of empty values in it.
    Based on the column's type it provides a description of the column by computing a number of statistics,
    such as min, max, mean and median, for numeric columns, and distribution of values, when appropriate.
    """

    @classmethod
    def _get_column_type(cls, df, column):
        # list of types is used to support pandas and sqlalchemy
        try:
            if df.expect_column_values_to_be_in_type_list(column, type_list=["int", "INTEGER", "BIGINT"])["success"]:
                type_ = "int"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=["float", "DOUBLE_PRECISION"])["success"]:
                type_ = "float"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=["string", "VARCHAR", "TEXT"])["success"]:
                type_ = "string"

            else:
                type_ = "unknown"
        except NotImplementedError:
            type_ = "unknown"

        return type_

    @classmethod
    def _get_column_cardinality(cls, df, column):

        num_unique = df.expect_column_unique_value_count_to_be_between(column, 0, None)[
            'result']['observed_value']
        pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(
            column, 0, None)['result']['observed_value']


        if pct_unique == 1.0:
            cardinality = "unique"

        elif pct_unique > .1:
            cardinality = "very many"

        elif pct_unique > .02:
            cardinality = "many"

        else:
            cardinality = "complicated"
            if num_unique == 0:
                cardinality = "none"

            elif num_unique == 1:
                cardinality = "one"

            elif num_unique == 2:
                cardinality = "two"

            elif num_unique < 60:
                cardinality = "very few"

            elif num_unique < 1000:
                cardinality = "few"

            else:
                cardinality = "many"
        print('col: {0:s}, num_unique: {1:d}, pct_unique: {2:f}, card: {3:s}'.format(column, num_unique,pct_unique, cardinality))

        return cardinality

    @classmethod
    def _get_value_set(cls, df, column):
        partition_object = {
            "values": ["GE_DUMMY_VAL"],
            "weights": [1.0]
        }

        df.expect_column_kl_divergence_to_be_less_than(column, partition_object=partition_object,
                                                   threshold=0, result_format='COMPLETE')

    @classmethod
    def _profile(cls, dataset):


        df = dataset

        for column in df.get_table_columns():
            df.expect_column_to_exist(column)

            type_ = cls._get_column_type(df, column)
            cardinality= cls._get_column_cardinality(df, column)
            df.expect_column_values_to_not_be_null(column, mostly=0)
            df.expect_column_values_to_be_in_set(
                column, [], result_format="SUMMARY")

            if type_ == "int":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)
                    try:
                        df.expect_column_values_to_be_increasing(column)
                    except NotImplementedError:
                        warnings.warn("NotImplementedError: expect_column_values_to_be_increasing")

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=[], result_format="SUMMARY")
                else:
                    df.expect_column_min_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_max_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_mean_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_median_to_be_between(column, min_value=0, max_value=0)

            elif type_ == "float":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)
                    try:
                        df.expect_column_values_to_be_increasing(column)
                    except NotImplementedError:
                        warnings.warn("NotImplementedError: expect_column_values_to_be_increasing")

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=[], result_format="SUMMARY")

                else:
                    df.expect_column_min_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_max_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_mean_to_be_between(column, min_value=0, max_value=0)
                    df.expect_column_median_to_be_between(column, min_value=0, max_value=0)

            elif type_ == "string":
                # Check for leading and tralining whitespace.
                #!!! It would be nice to build additional Expectations here, but
                #!!! the default logic for remove_expectations prevents us.
                df.expect_column_values_to_not_match_regex(column, r"^\s+|\s+$")

                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=[], result_format="SUMMARY")
                else:
                    # print(column, type_, cardinality)
                    pass

            else:
                # print("??????", column, type_, cardinality)
                pass

        # FIXME: this is temporary hack. This expectation is failing since we are passing an empty set as an arg.
        # Need to figure out how to pass universal set instead. The hack is supressing the success_on_last_run param
        # in order to keep this expectation in the suite.
        for ind, e in enumerate(df._expectation_suite.expectations):
            if 'success_on_last_run' in e and e['success_on_last_run'] == False and e['expectation_type'] == 'expect_column_distinct_values_to_be_in_set':
                df._expectation_suite.expectations[ind]['success_on_last_run'] = True

        return df.get_expectation_suite(suppress_warnings=True)
