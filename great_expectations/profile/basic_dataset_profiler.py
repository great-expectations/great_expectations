from .base import DatasetProfiler


class BasicDatasetProfiler(DatasetProfiler):
    """A profiler inspired by the beloved pandas_profiling project
    """

    @classmethod
    def _get_column_type(cls, df, column):
        try:
            if df.expect_column_values_to_be_of_type(column, "int")["success"]:
                type_ = "int"

            elif df.expect_column_values_to_be_of_type(column, "float")["success"]:
                type_ = "float"

            elif df.expect_column_values_to_be_of_type(column, "string")["success"]:
                type_ = "string"

            else:
                type_ = "unknown"
        except NotImplementedError:
            type_ = "unknown"

        return type_

    @classmethod
    def _get_column_cardinality(cls, df, column):

        num_rows = df.expect_table_row_count_to_be_between(min_value=0, max_value=None)[
            'result']['observed_value']
        num_unique = df.expect_column_unique_value_count_to_be_between(column, 0, None)[
            'result']['observed_value']
        pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(
            column, 0, None)['result']['observed_value']

        if pct_unique == 1.0:
            cardinality = "unique"

        elif pct_unique > .1:
            cardinality = "many"

        elif pct_unique > .02:
            cardinality = "lots"

        else:
            cardinality = "complicated"
            if num_unique == 0:
                cardinality = "none"

            elif num_unique == 1:
                cardinality = "one"

            elif num_unique == 2:
                cardinality = "two"

            elif num_unique < 20:
                cardinality = "very few"

            elif num_unique < 200:
                cardinality = "few"

            else:
                cardinality = "unknown"
                print(
                    column, '\t',
                    num_unique,
                    pct_unique
                )

        return cardinality

    @classmethod
    def _profile(cls, dataset):
        df = dataset

        for column in df.get_table_columns():
            df.expect_column_to_exist(column)

            type_ = cls._get_column_type(df, column)
            cardinality= cls._get_column_cardinality(df, column)
            df.expect_column_values_to_not_be_null(column)
            df.expect_column_values_to_be_in_set(
                column, [], result_format="SUMMARY")

            if type_ == "int":
                df.expect_column_values_to_not_be_in_set(column, [0])

                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)
                    df.expect_column_values_to_be_increasing(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    # TODO: df.expect_column_values_to_not_be_in_set(column, value_set=????)
                    # Need to figure out how to get the value set
                    pass

                else:
                    # print(column, cardinality)
                    pass

            elif type_ == "float":
                #         print(column, type_, cardinality)
                pass

            elif type_ == "string":
                # Check for leading and tralining whitespace.
                #!!! It would be nice to build additional Expectations here, but
                #!!! the default logic for remove_expectations prevents us.
                df.expect_column_values_to_not_match_regex(column, r"^\s+|\s+$")

                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:

                    #             print(df[column].value_counts())
                    pass

                else:
                    # print(column, type_, cardinality)
                    pass

            else:
                # print("??????", column, type_, cardinality)
                pass

        return df.get_expectation_suite(suppress_warnings=True)
