import logging
from .base import DatasetProfiler

logger = logging.getLogger(__name__)


class BasicDatasetProfiler(DatasetProfiler):
    """BasicDatasetProfiler is inspired by the beloved pandas_profiling project.

    The profiler examines a batch of data and creates a report that answers the basic questions
    most data practitioners would ask about a dataset during exploratory data analysis.
    The profiler reports how unique the values in the column are, as well as the percentage of empty values in it.
    Based on the column's type it provides a description of the column by computing a number of statistics,
    such as min, max, mean and median, for numeric columns, and distribution of values, when appropriate.
    """
    INT_TYPE_NAMES = {"INTEGER", "int", "INT", "TINYINT", "BYTEINT", "SMALLINT", "BIGINT", "IntegerType", "LongType", "DECIMAL"}
    FLOAT_TYPE_NAMES = {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE_PRECISION", "NUMERIC", "FloatType", "DoubleType", "float"}
    STRING_TYPE_NAMES = {"CHAR", "VARCHAR", "TEXT", "StringType", "string", "str"}
    BOOLEAN_TYPE_NAMES = {"BOOLEAN", "BOOL", "bool", "BooleanType"}
    DATETIME_TYPE_NAMES = {"DATETIME", "DATE", "TIMESTAMP", "DateType", "TimestampType", "datetime64", "Timestamp"}

    @classmethod
    def _get_column_type(cls, df, column):
        # list of types is used to support pandas and sqlalchemy
        df.set_config_value("interactive_evaluation", True)
        try:
            if df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.INT_TYPE_NAMES))).success:
                type_ = "int"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.FLOAT_TYPE_NAMES))).success:
                type_ = "float"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.STRING_TYPE_NAMES))).success:
                type_ = "string"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.BOOLEAN_TYPE_NAMES))).success:
                type_ = "bool"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.DATETIME_TYPE_NAMES))).success:
                type_ = "datetime"

            else:
                df.expect_column_values_to_be_in_type_list(column, type_list=None)
                type_ = "unknown"
        except NotImplementedError:
            type_ = "unknown"

        df.set_config_value('interactive_evaluation', False)
        return type_

    @classmethod
    def _get_column_cardinality(cls, df, column):
        num_unique = None
        pct_unique = None
        df.set_config_value("interactive_evaluation", True)

        try:
            num_unique = df.expect_column_unique_value_count_to_be_between(column, None, None).result['observed_value']
            pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(
                column, None, None).result['observed_value']
        except KeyError:  # if observed_value value is not set
            logger.error("Failed to get cardinality of column {0:s} - continuing...".format(column))

        if num_unique is None or num_unique == 0 or pct_unique is None:
            cardinality = "none"

        elif pct_unique == 1.0:
            cardinality = "unique"

        elif pct_unique > .1:
            cardinality = "very many"

        elif pct_unique > .02:
            cardinality = "many"

        else:
            cardinality = "complicated"
            if num_unique == 1:
                cardinality = "one"

            elif num_unique == 2:
                cardinality = "two"

            elif num_unique < 60:
                cardinality = "very few"

            elif num_unique < 1000:
                cardinality = "few"

            else:
                cardinality = "many"
        # print('col: {0:s}, num_unique: {1:s}, pct_unique: {2:s}, card: {3:s}'.format(column, str(num_unique), str(pct_unique), cardinality))

        df.set_config_value('interactive_evaluation', False)

        return cardinality

    @classmethod
    def _profile(cls, dataset):
        df = dataset

        df.set_default_expectation_argument("catch_exceptions", True)

        df.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        df.expect_table_columns_to_match_ordered_list(None)
        df.set_config_value('interactive_evaluation', False)

        columns = df.get_table_columns()

        meta_columns = {}
        for column in columns:
            meta_columns[column] = {"description": ""}

        number_of_columns = len(columns)
        for i, column in enumerate(columns):
            logger.info("            Preparing column {} of {}: {}".format(i+1, number_of_columns, column))

            # df.expect_column_to_exist(column)

            type_ = cls._get_column_type(df, column)
            cardinality = cls._get_column_cardinality(df, column)
            df.expect_column_values_to_not_be_null(column, mostly=0.5) # The renderer will show a warning for columns that do not meet this expectation
            df.expect_column_values_to_be_in_set(column, [], result_format="SUMMARY")

            if type_ == "int":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)
                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                elif cardinality in ["many", "very many", "unique"]:
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_stdev_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column,
                                                                   quantile_ranges={
                                                                       "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                                                                       "value_ranges": [[None, None], [None, None], [None, None], [None, None], [None, None]]
                                                                   }
                                                                   )
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
                                                           threshold=None, result_format='COMPLETE')
                else: # unknown cardinality - skip
                    pass
            elif type_ == "float":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")

                elif cardinality in ["many", "very many", "unique"]:
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column,
                                                                   quantile_ranges={
                                                                       "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                                                                       "value_ranges": [[None, None], [None, None], [None, None], [None, None], [None, None]]
                                                                   }
                                                                   )
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,

                                                           threshold=None, result_format='COMPLETE')
                else:  # unknown cardinality - skip
                    pass

            elif type_ == "string":
                # Check for leading and trailing whitespace.
                #!!! It would be nice to build additional Expectations here, but
                #!!! the default logic for remove_expectations prevents us.
                df.expect_column_values_to_not_match_regex(column, r"^\s+|\s+$")

                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                else:
                    # print(column, type_, cardinality)
                    pass

            elif type_ == "datetime":
                df.expect_column_min_to_be_between(column, min_value=None, max_value=None)

                df.expect_column_max_to_be_between(column, min_value=None, max_value=None)

                # Re-add once kl_divergence has been modified to support datetimes
                # df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
                #                                            threshold=None, result_format='COMPLETE')

                if cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")



            else:
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                else:
                    # print(column, type_, cardinality)
                    pass

        df.set_config_value("interactive_evaluation", True)
        expectation_suite = df.get_expectation_suite(suppress_warnings=True, discard_failed_expectations=False)
        expectation_suite.meta["columns"] = meta_columns

        return expectation_suite
