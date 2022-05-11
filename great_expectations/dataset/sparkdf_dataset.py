import copy
import inspect
import json
import logging
import warnings
from collections import OrderedDict
from datetime import datetime
from functools import reduce, wraps
from typing import List

import jsonschema
import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.data_asset import DataAsset
from great_expectations.data_asset.util import DocInherit, parse_result_format
from great_expectations.dataset.dataset import Dataset
from great_expectations.dataset.pandas_dataset import PandasDataset

logger = logging.getLogger(__name__)
try:
    import pyspark.sql.types as sparktypes
    from pyspark.ml.feature import Bucketizer
    from pyspark.sql import SQLContext, Window
    from pyspark.sql.functions import (
        array,
        col,
        count,
        countDistinct,
        datediff,
        desc,
        expr,
        isnan,
        lag,
    )
    from pyspark.sql.functions import length as length_
    from pyspark.sql.functions import (
        lit,
        monotonically_increasing_id,
        stddev_samp,
        struct,
        udf,
        when,
    )
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


class MetaSparkDFDataset(Dataset):
    "MetaSparkDFDataset is a thin layer between Dataset and SparkDFDataset.\n    This two-layer inheritance is required to make @classmethod decorators work.\n    Practically speaking, that means that MetaSparkDFDataset implements     expectation decorators, like `column_map_expectation` and `column_aggregate_expectation`,     and SparkDFDataset implements the expectation methods themselves.\n"

    def __init__(self, *args, **kwargs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'Constructs an expectation using column-map semantics.\n\n\n        The MetaSparkDFDataset implementation replaces the "column" parameter supplied by the user with a Spark Dataframe\n        with the actual column data. The current approach for functions implementing expectation logic is to append\n        a column named "__success" to this dataframe and return to this decorator.\n\n        See :func:`column_map_expectation <great_expectations.Dataset.base.Dataset.column_map_expectation>`         for full documentation of this function.\n        '
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self, column, mostly=None, result_format=None, *args, **kwargs
        ):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            "\n            This whole decorator is pending a re-write. Currently there is are huge performance issues\n            when the # of unexpected elements gets large (10s of millions). Additionally, there is likely\n            easy optimization opportunities by coupling result_format with how many different transformations\n            are done on the dataset, as is done in sqlalchemy_dataset.\n            "
            eval_col = f"__eval_col_{column.replace('.', '__').replace('`', '_')}"
            self.spark_df = self.spark_df.withColumn(eval_col, col(column))
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]
            result_format = parse_result_format(result_format)
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]
            col_df = self.spark_df.select(col(eval_col))
            col_df.persist()
            element_count = self.get_row_count()
            if func.__name__ not in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                col_df = col_df.filter(col_df[0].isNotNull())
                nonnull_count = self.get_column_nonnull_count(eval_col)
            else:
                nonnull_count = element_count
            success_df = func(self, col_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()
            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                maybe_limited_unexpected_list = []
            else:
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    row[eval_col] for row in unexpected_df.collect()
                ]
                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if val is None:
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if isinstance(val, str):
                                val = parse(val)
                            parsed_maybe_limited_unexpected_list.append(
                                datetime.strftime(val, output_strftime_format)
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list
            (success, percent_success) = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )
            return_obj = self._format_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                unexpected_count,
                maybe_limited_unexpected_list,
                unexpected_index_list=None,
            )
            if func.__name__ in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                del return_obj["result"]["unexpected_percent_nonmissing"]
                del return_obj["result"]["missing_count"]
                del return_obj["result"]["missing_percent"]
                try:
                    del return_obj["result"]["partial_unexpected_counts"]
                except KeyError:
                    pass
            col_df.unpersist()
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper

    @classmethod
    def column_pair_map_expectation(cls, func):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        The column_pair_map_expectation decorator handles boilerplate issues surrounding the common pattern of evaluating\n        truthiness of some condition on a per row basis across a pair of columns.\n        "
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self,
            column_A,
            column_B,
            mostly=None,
            ignore_row_if="both_values_are_missing",
            result_format=None,
            *args,
            **kwargs,
        ):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            eval_col_A = f"__eval_col_A_{column_A.replace('.', '__').replace('`', '_')}"
            eval_col_B = f"__eval_col_B_{column_B.replace('.', '__').replace('`', '_')}"
            self.spark_df = self.spark_df.withColumn(
                eval_col_A, col(column_A)
            ).withColumn(eval_col_B, col(column_B))
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]
            result_format = parse_result_format(result_format)
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]
            cols_df = self.spark_df.select(eval_col_A, eval_col_B).withColumn(
                "__row", monotonically_increasing_id()
            )
            cols_df.cache()
            element_count = self.get_row_count()
            if ignore_row_if == "both_values_are_missing":
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(eval_col_A),
                    "`{0}` AS `B_{0}`".format(eval_col_B),
                    "ISNULL(`{}`) AND ISNULL(`{}`) AS `__null_val`".format(
                        eval_col_A, eval_col_B
                    ),
                )
            elif ignore_row_if == "either_value_is_missing":
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(eval_col_A),
                    "`{0}` AS `B_{0}`".format(eval_col_B),
                    f"ISNULL(`{eval_col_A}`) OR ISNULL(`{eval_col_B}`) AS `__null_val`",
                )
            elif ignore_row_if == "never":
                '\n                TODO: <Alex>Note: The value of the "ignore_row_if" directive in the commented out line above is correct.\n                However, fixing the error would constitute a breaking change.  Hence, the documentation is updated now\n                (8/16/2021), while the implementation is corrected as part of the Expectations V3 API release.\n                </Alex>\n'
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(eval_col_A),
                    "`{0}` AS `B_{0}`".format(eval_col_B),
                    lit(False).alias("__null_val"),
                )
            else:
                raise ValueError("Unknown value of ignore_row_if: %s", (ignore_row_if,))
            nonnull_df = boolean_mapped_null_values.filter("__null_val = False")
            nonnull_count = nonnull_df.count()
            col_A_df = nonnull_df.select("__row", f"`A_{eval_col_A}`")
            col_B_df = nonnull_df.select("__row", f"`B_{eval_col_B}`")
            success_df = func(self, col_A_df, col_B_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()
            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                maybe_limited_unexpected_list = []
            else:
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    (row[f"A_{eval_col_A}"], row[f"B_{eval_col_B}"])
                    for row in unexpected_df.collect()
                ]
                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if (val is None) or ((val[0] is None) or (val[1] is None)):
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if isinstance(val[0], str) and isinstance(val[1], str):
                                val = (parse(val[0]), parse(val[1]))
                            parsed_maybe_limited_unexpected_list.append(
                                (
                                    datetime.strftime(val[0], output_strftime_format),
                                    datetime.strftime(val[1], output_strftime_format),
                                )
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list
            (success, percent_success) = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )
            return_obj = self._format_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                unexpected_count,
                maybe_limited_unexpected_list,
                unexpected_index_list=None,
            )
            cols_df.unpersist()
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper

    @classmethod
    def multicolumn_map_expectation(cls, func):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        The multicolumn_map_expectation decorator handles boilerplate issues surrounding the common pattern of\n        evaluating truthiness of some condition on a per row basis across a set of columns.\n        "
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self,
            column_list,
            mostly=None,
            ignore_row_if="all_values_are_missing",
            result_format=None,
            *args,
            **kwargs,
        ):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            eval_cols = []
            for col_name in column_list:
                eval_col = f"__eval_col_{col_name.replace('.', '__').replace('`', '_')}"
                eval_cols.append(eval_col)
                self.spark_df = self.spark_df.withColumn(eval_col, col(col_name))
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]
            result_format = parse_result_format(result_format)
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]
            temp_df = self.spark_df.select(*eval_cols)
            temp_df.cache()
            element_count = self.get_row_count()
            if ignore_row_if == "all_values_are_missing":
                boolean_mapped_skip_values = temp_df.select(
                    [
                        *eval_cols,
                        reduce(
                            (lambda a, b: (a & b)), [col(c).isNull() for c in eval_cols]
                        ).alias("__null_val"),
                    ]
                )
            elif ignore_row_if == "any_value_is_missing":
                boolean_mapped_skip_values = temp_df.select(
                    [
                        *eval_cols,
                        reduce(
                            (lambda a, b: (a | b)), [col(c).isNull() for c in eval_cols]
                        ).alias("__null_val"),
                    ]
                )
            elif ignore_row_if == "never":
                boolean_mapped_skip_values = temp_df.select(
                    [*eval_cols, lit(False).alias("__null_val")]
                )
            else:
                raise ValueError("Unknown value of ignore_row_if: %s", (ignore_row_if,))
            nonnull_df = boolean_mapped_skip_values.filter("__null_val = False")
            nonnull_count = nonnull_df.count()
            cols_df = nonnull_df.select(*eval_cols)
            success_df = func(self, cols_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()
            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                maybe_limited_unexpected_list = []
            else:
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    OrderedDict(
                        (col_name, row[eval_col_name])
                        for (col_name, eval_col_name) in zip(column_list, eval_cols)
                    )
                    for row in unexpected_df.collect()
                ]
                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if (val is None) or (not all(v for (k, v) in val)):
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if all(isinstance(v, str) for (k, v) in val):
                                val = OrderedDict((k, parse(v)) for (k, v) in val)
                            parsed_maybe_limited_unexpected_list.append(
                                OrderedDict(
                                    (
                                        k,
                                        datetime.strftime(v, output_strftime_format),
                                    )
                                    for (k, v) in val
                                )
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list
            (success, percent_success) = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )
            return_obj = self._format_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                unexpected_count,
                maybe_limited_unexpected_list,
                unexpected_index_list=None,
            )
            temp_df.unpersist()
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper


class SparkDFDataset(MetaSparkDFDataset):
    '\n    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.\n\n    --ge-feature-maturity-info--\n\n        id: validation_engine_pyspark_self_managed\n        title: Validation Engine - pyspark - Self-Managed\n        icon:\n        short_description: Use Spark DataFrame to validate data\n        description: Use Spark DataFrame to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Production\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Moderate\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A -> see relevant Datasource evaluation\n            documentation_completeness: Complete\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_databricks\n        title: Validation Engine - Databricks\n        icon:\n        short_description: Use Spark DataFrame in a Databricks cluster to validate data\n        description: Use Spark DataFrame in a Databricks cluster to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Beta\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Low (dbfs-specific handling)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Minimal (we\'ve tested a bit, know others have used it)\n            documentation_completeness: Moderate (need docs on managing project configuration via dbfs/etc.)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_emr_spark\n        title: Validation Engine - EMR - Spark\n        icon:\n        short_description: Use Spark DataFrame in an EMR cluster to validate data\n        description: Use Spark DataFrame in an EMR cluster to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Experimental\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Low (need to provide guidance on "known good" paths, and we know there are many "knobs" to tune that we have not explored/tested)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n        id: validation_engine_spark_other\n        title: Validation Engine - Spark - Other\n        icon:\n        short_description: Use Spark DataFrame to validate data\n        description: Use Spark DataFrame to validate data\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_spark_dataframe_as_a_batch.html\n        maturity: Experimental\n        maturity_details:\n            api_stability: Stable\n            implementation_completeness: Other (we haven\'t tested possibility, known glue deployment)\n            unit_test_coverage: N/A -> implementation not different\n            integration_infrastructure_test_coverage: Unknown\n            documentation_completeness: Low (must install specific/latest version but do not have docs to that effect or of known useful paths)\n            bug_risk: Low/Moderate\n            expectation_completeness: Moderate\n\n    --ge-feature-maturity-info--\n'

    @classmethod
    def from_dataset(cls, dataset=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(dataset, SparkDFDataset):
            return cls(spark_df=dataset.spark_df)
        else:
            raise ValueError("from_dataset requires a SparkDFDataset dataset")

    def __init__(self, spark_df, *args, **kwargs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.spark_df = spark_df
        self._persist = kwargs.pop("persist", True)
        if self._persist:
            self.spark_df.persist()
        super().__init__(*args, **kwargs)

    def head(self, n=5):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Returns a *PandasDataset* with the first *n* rows of the given Dataset"
        return PandasDataset(
            self.spark_df.limit(n).toPandas(),
            expectation_suite=self.get_expectation_suite(
                discard_failed_expectations=False,
                discard_result_format_kwargs=False,
                discard_catch_exceptions_kwargs=False,
                discard_include_config_kwargs=False,
            ),
        )

    def get_row_count(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.count()

    def get_column_count(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return len(self.spark_df.columns)

    def get_table_columns(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.columns

    def get_column_nonnull_count(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.filter(col(column).isNotNull()).count()

    def get_column_mean(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        types = dict(self.spark_df.dtypes)
        if types[column] not in ("int", "float", "double", "bigint"):
            raise TypeError("Expected numeric column type for function mean()")
        result = self.spark_df.select(column).groupBy().mean().collect()[0]
        return result[0] if (len(result) > 0) else None

    def get_column_sum(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.select(column).groupBy().sum().collect()[0][0]

    def _describe_column(self, column) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        pass

    def get_column_max(self, column, parse_strings_as_datetimes=False):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: "max"}).collect()
        if (not result) or (not result[0]):
            return None
        return result[0][0]

    def get_column_min(self, column, parse_strings_as_datetimes=False):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: "min"}).collect()
        if (not result) or (not result[0]):
            return None
        return result[0][0]

    def get_column_value_counts(self, column, sort="value", collate=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        if collate is not None:
            raise ValueError("collate parameter is not supported in SparkDFDataset")
        value_counts = (
            self.spark_df.select(column)
            .where(col(column).isNotNull())
            .groupBy(column)
            .count()
        )
        if sort == "value":
            value_counts = value_counts.orderBy(column)
        elif sort == "count":
            value_counts = value_counts.orderBy(desc("count"))
        value_counts = value_counts.collect()
        series = pd.Series(
            [row["count"] for row in value_counts],
            index=pd.Index(data=[row[column] for row in value_counts], name="value"),
            name="count",
        )
        return series

    def get_column_unique_count(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.agg(countDistinct(column)).collect()[0][0]

    def get_column_modes(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "leverages computation done in _get_column_value_counts"
        s = self.get_column_value_counts(column)
        return list(s[(s == s.max())].index)

    def get_column_median(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        result = self.spark_df.approxQuantile(
            column, [0.5, (0.5 + (1 / (2 + (2 * self.get_row_count()))))], 0
        )
        return np.mean(result)

    def get_column_quantiles(self, column, quantiles, allow_relative_error=False):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if allow_relative_error is False:
            allow_relative_error = 0.0
        if (
            (not isinstance(allow_relative_error, float))
            or (allow_relative_error < 0)
            or (allow_relative_error > 1)
        ):
            raise ValueError(
                "SparkDFDataset requires relative error to be False or to be a float between 0 and 1."
            )
        return self.spark_df.approxQuantile(
            column, list(quantiles), allow_relative_error
        )

    def get_column_stdev(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.spark_df.select(stddev_samp(col(column))).collect()[0][0]

    def get_column_hist(self, column, bins):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "return a list of counts corresponding to bins"
        bins = list(copy.deepcopy(bins))
        if (bins[0] == (-np.inf)) or (bins[0] == (-float("inf"))):
            added_min = False
            bins[0] = -float("inf")
        else:
            added_min = True
            bins.insert(0, (-float("inf")))
        if (bins[(-1)] == np.inf) or (bins[(-1)] == float("inf")):
            added_max = False
            bins[(-1)] = float("inf")
        else:
            added_max = True
            bins.append(float("inf"))
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        bucketizer = Bucketizer(splits=bins, inputCol=column, outputCol="buckets")
        bucketed = bucketizer.setHandleInvalid("skip").transform(temp_column)
        if added_max:
            upper_bound_count = (
                temp_column.select(column).filter(col(column) == bins[(-2)]).count()
            )
        else:
            upper_bound_count = 0
        hist_rows = bucketed.groupBy("buckets").count().collect()
        hist = [0] * (len(bins) - 1)
        for row in hist_rows:
            hist[int(row["buckets"])] = row["count"]
        hist[(-2)] += upper_bound_count
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

    def get_column_count_in_range(
        self, column, min_val=None, max_val=None, strict_min=False, strict_max=True
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (min_val is None) and (max_val is None):
            raise ValueError("Must specify either min or max value")
        if (min_val is not None) and (max_val is not None) and (min_val > max_val):
            raise ValueError("Min value must be <= to max value")
        result = self.spark_df.select(column)
        if min_val is not None:
            if strict_min:
                result = result.filter(col(column) > min_val)
            else:
                result = result.filter(col(column) >= min_val)
        if max_val is not None:
            if strict_max:
                result = result.filter(col(column) < max_val)
            else:
                result = result.filter(col(column) <= max_val)
        return result.count()

    @staticmethod
    def _apply_dateutil_parse(column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert len(column.columns) == 1, "Expected DataFrame with 1 column"
        col_name = column.columns[0]
        _udf = udf(parse, sparktypes.TimestampType())
        return column.withColumn(col_name, _udf(col_name))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_in_set(
        self,
        column,
        value_set,
        mostly=None,
        parse_strings_as_datetimes=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if value_set is None:
            return column.withColumn("__success", lit(True))
        if parse_strings_as_datetimes:
            column = self._apply_dateutil_parse(column)
            value_set = [
                (parse(value) if isinstance(value, str) else value)
                for value in value_set
            ]
        if None in value_set:
            logger.error(
                "expect_column_values_to_be_in_set cannot support a None in the value_set in spark"
            )
            raise ValueError(
                "expect_column_values_to_be_in_set cannot support a None in the value_set in spark"
            )
        return column.withColumn("__success", column[0].isin(value_set))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(
        self,
        column,
        value_set,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if None in value_set:
            logger.error(
                "expect_column_values_to_not_be_in_set cannot support a None in the value_set in spark"
            )
            raise ValueError(
                "expect_column_values_to_not_be_in_set cannot support a None in the value_set in spark"
            )
        return column.withColumn("__success", (~column[0].isin(value_set)))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        strict_min=False,
        strict_max=False,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        allow_cross_type_comparisons=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if allow_cross_type_comparisons:
            raise ValueError("Cross-type comparisons are not valid for SparkDFDataset")
        if parse_strings_as_datetimes:
            min_value = parse(min_value)
            max_value = parse(max_value)
        if (min_value is None) and (max_value is None):
            raise ValueError("min_value and max_value cannot both be None")
        elif min_value is None:
            if strict_max:
                return column.withColumn(
                    "__success",
                    when((column[0] < max_value), lit(True)).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when((column[0] <= max_value), lit(True)).otherwise(lit(False)),
                )
        elif max_value is None:
            if strict_min:
                return column.withColumn(
                    "__success",
                    when((column[0] > min_value), lit(True)).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when((column[0] >= min_value), lit(True)).otherwise(lit(False)),
                )
        else:
            if min_value > max_value:
                raise ValueError("minvalue cannot be greater than max_value")
            if strict_min and strict_max:
                return column.withColumn(
                    "__success",
                    when(
                        ((min_value < column[0]) & (column[0] < max_value)), lit(True)
                    ).otherwise(lit(False)),
                )
            elif strict_min:
                return column.withColumn(
                    "__success",
                    when(
                        ((min_value < column[0]) & (column[0] <= max_value)), lit(True)
                    ).otherwise(lit(False)),
                )
            elif strict_max:
                return column.withColumn(
                    "__success",
                    when(
                        ((min_value <= column[0]) & (column[0] < max_value)), lit(True)
                    ).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when(
                        ((min_value <= column[0]) & (column[0] <= max_value)), lit(True)
                    ).otherwise(lit(False)),
                )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_value_lengths_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (min_value is None) and (max_value is None):
            return column.withColumn("__success", lit(True))
        elif min_value is None:
            return column.withColumn(
                "__success",
                when((length_(column[0]) <= max_value), lit(True)).otherwise(
                    lit(False)
                ),
            )
        elif max_value is None:
            return column.withColumn(
                "__success",
                when((length_(column[0]) >= min_value), lit(True)).otherwise(
                    lit(False)
                ),
            )
        return column.withColumn(
            "__success",
            when(
                ((min_value <= length_(column[0])) & (length_(column[0]) <= max_value)),
                lit(True),
            ).otherwise(lit(False)),
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_unique(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn(
            "__success", (count(lit(1)).over(Window.partitionBy(column[0])) <= 1)
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_value_lengths_to_equal(
        self,
        column,
        value,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn(
            "__success",
            when((length_(column[0]) == value), lit(True)).otherwise(lit(False)),
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_strftime_format(
        self,
        column,
        strftime_format,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            datetime.strptime(
                datetime.strftime(datetime.now(), strftime_format), strftime_format
            )
        except ValueError as e:
            raise ValueError(f"Unable to use provided strftime_format. {e.message}")

        def is_parseable_by_format(val):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            try:
                datetime.strptime(val, strftime_format)
                return True
            except TypeError:
                raise TypeError(
                    "Values passed to expect_column_values_to_match_strftime_format must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                )
            except ValueError:
                return False

        success_udf = udf(is_parseable_by_format)
        return column.withColumn("__success", success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn("__success", column[0].isNotNull())

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn("__success", column[0].isNull())

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_json_schema(
        self,
        column,
        json_schema,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")

        def matches_json_schema(val):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            try:
                val_json = json.loads(val)
                jsonschema.validate(val_json, json_schema)
                return True
            except jsonschema.ValidationError:
                return False
            except jsonschema.SchemaError:
                raise
            except:
                raise

        matches_json_schema_udf = udf(matches_json_schema, sparktypes.StringType())
        return column.withColumn("__success", matches_json_schema_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_json_parseable(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")

        def is_json(val):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            try:
                json.loads(val)
                return True
            except:
                return False

        is_json_udf = udf(is_json, sparktypes.StringType())
        return column.withColumn("__success", is_json_udf(column[0]))

    @DocInherit
    @DataAsset.expectation(["column", "type_", "mostly"])
    def expect_column_values_to_be_of_type(
        self,
        column,
        type_,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        eval_col = f"__eval_col_{column.replace('.', '__').replace('`', '_')}"
        self.spark_df = self.spark_df.withColumn(eval_col, col(column))
        if mostly is not None:
            raise ValueError(
                "SparkDFDataset does not support column map semantics for column types"
            )
        try:
            col_df = self.spark_df.select(eval_col)
            col_data = [f for f in col_df.schema.fields if (f.name == eval_col)][0]
            col_type = type(col_data.dataType)
        except IndexError:
            raise ValueError(f"Unrecognized column: {column}")
        except KeyError:
            raise ValueError(f"No type data available for column: {column}")
        try:
            if type_ is None:
                success = True
            else:
                success = issubclass(col_type, getattr(sparktypes, type_))
            return {"success": success, "result": {"observed_value": col_type.__name__}}
        except AttributeError:
            raise ValueError(f"Unrecognized spark type: {type_}")

    @DocInherit
    @DataAsset.expectation(["column", "type_list", "mostly"])
    def expect_column_values_to_be_in_type_list(
        self,
        column,
        type_list: List[str],
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        eval_col = f"__eval_col_{column.replace('.', '__').replace('`', '_')}"
        self.spark_df = self.spark_df.withColumn(eval_col, col(column))
        if mostly is not None:
            raise ValueError(
                "SparkDFDataset does not support column map semantics for column types"
            )
        try:
            col_df = self.spark_df.select(eval_col)
            col_data = [f for f in col_df.schema.fields if (f.name == eval_col)][0]
            col_type = type(col_data.dataType)
        except IndexError:
            raise ValueError(f"Unrecognized column: {column}")
        except KeyError:
            raise ValueError(f"No database type data available for column: {column}")
        if type_list is None:
            success = True
        else:
            types = []
            for type_ in type_list:
                try:
                    type_class = getattr(sparktypes, type_)
                    types.append(type_class)
                except AttributeError:
                    logger.debug(f"Unrecognized type: {type_}")
            if len(types) == 0:
                raise ValueError("No recognized spark types in type_list")
            types = tuple(types)
            success = issubclass(col_type, types)
        return {"success": success, "result": {"observed_value": col_type.__name__}}

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn("__success", column[0].rlike(regex))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn("__success", (~column[0].rlike(regex)))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_regex_list(
        self,
        column,
        regex_list,
        match_on="any",
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if match_on == "any":
            return column.withColumn("__success", column[0].rlike("|".join(regex_list)))
        elif match_on == "all":
            formatted_regex_list = [f"(?={regex})" for regex in regex_list]
            return column.withColumn(
                "__success", column[0].rlike("".join(formatted_regex_list))
            )
        else:
            raise ValueError("match_on must be either 'any' or 'all'")

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_match_regex_list(
        self,
        column,
        regex_list,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return column.withColumn("__success", (~column[0].rlike("|".join(regex_list))))

    @DocInherit
    @MetaSparkDFDataset.column_pair_map_expectation
    def expect_column_pair_values_to_be_equal(
        self,
        column_A,
        column_B,
        ignore_row_if="both_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_A_name = column_A.schema.names[1]
        column_B_name = column_B.schema.names[1]
        join_df = column_A.join(
            column_B, (column_A["__row"] == column_B["__row"]), how="inner"
        )
        return join_df.withColumn(
            "__success",
            when((col(column_A_name) == col(column_B_name)), True).otherwise(False),
        )

    @DocInherit
    @MetaSparkDFDataset.column_pair_map_expectation
    def expect_column_pair_values_A_to_be_greater_than_B(
        self,
        column_A,
        column_B,
        or_equal=None,
        parse_strings_as_datetimes=None,
        allow_cross_type_comparisons=None,
        ignore_row_if="both_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if allow_cross_type_comparisons:
            raise NotImplementedError
        column_A_name = column_A.schema.names[1]
        column_B_name = column_B.schema.names[1]
        if parse_strings_as_datetimes:
            _udf = udf(parse, sparktypes.TimestampType())
            (timestamp_column_A, timestamp_column_B) = (
                f"__ts_{column_A_name}",
                f"__ts_{column_B_name}",
            )
            temp_column_A = column_A.withColumn(timestamp_column_A, _udf(column_A_name))
            temp_column_B = column_B.withColumn(timestamp_column_B, _udf(column_B_name))
            (column_A_name, column_B_name) = (timestamp_column_A, timestamp_column_B)
        else:
            temp_column_A = column_A
            temp_column_B = column_B
        join_df = temp_column_A.join(
            temp_column_B,
            (temp_column_A["__row"] == temp_column_B["__row"]),
            how="inner",
        )
        if or_equal:
            return join_df.withColumn(
                "__success",
                when((col(column_A_name) >= col(column_B_name)), True).otherwise(False),
            )
        else:
            return join_df.withColumn(
                "__success",
                when((col(column_A_name) > col(column_B_name)), True).otherwise(False),
            )

    @DocInherit
    @MetaSparkDFDataset.column_pair_map_expectation
    def expect_column_pair_values_to_be_in_set(
        self,
        column_A,
        column_B,
        value_pairs_set,
        ignore_row_if="both_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_A_name = column_A.schema.names[1]
        column_B_name = column_B.schema.names[1]
        join_df = column_A.join(
            column_B, (column_A["__row"] == column_B["__row"]), how="inner"
        )
        join_df = join_df.withColumn(
            "combine_AB", array(col(column_A_name), col(column_B_name))
        )
        value_set_df = (
            SQLContext(self.spark_df._sc)
            .createDataFrame(value_pairs_set, ["col_A", "col_B"])
            .select(array("col_A", "col_B").alias("set_AB"))
        )
        return join_df.join(
            value_set_df, (join_df["combine_AB"] == value_set_df["set_AB"]), "left"
        ).withColumn(
            "__success", when(col("set_AB").isNull(), lit(False)).otherwise(lit(True))
        )

    def expect_multicolumn_values_to_be_unique(
        self,
        column_list,
        mostly=None,
        ignore_row_if="all_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        deprecation_warning = "expect_multicolumn_values_to_be_unique is deprecated as of v0.13.4 and will be removed in v0.16. Please use expect_select_column_values_to_be_unique_within_record instead."
        warnings.warn(deprecation_warning, DeprecationWarning)
        return self.expect_select_column_values_to_be_unique_within_record(
            column_list=column_list,
            mostly=mostly,
            ignore_row_if=ignore_row_if,
            result_format=result_format,
            include_config=include_config,
            catch_exceptions=catch_exceptions,
            meta=meta,
        )

    @DocInherit
    @MetaSparkDFDataset.multicolumn_map_expectation
    def expect_select_column_values_to_be_unique_within_record(
        self,
        column_list,
        mostly=None,
        ignore_row_if="all_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_names = column_list.schema.names[:]
        conditions = []
        for i in range(0, (len(column_names) - 1)):
            conditions.append(
                ~col(column_names[i]).eqNullSafe(col(column_names[(i + 1)]))
            )
        return column_list.withColumn(
            "__success", reduce((lambda a, b: (a & b)), conditions)
        )

    @DocInherit
    @MetaSparkDFDataset.multicolumn_map_expectation
    def expect_compound_columns_to_be_unique(
        self,
        column_list,
        mostly=None,
        ignore_row_if="all_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_names = column_list.schema.names[:]
        return column_list.withColumn(
            "__success",
            (count(lit(1)).over(Window.partitionBy(struct(*column_names))) <= 1),
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_increasing(
        self,
        column,
        strictly=False,
        mostly=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_name = column.schema.names[0]
        na_types = [
            isinstance(column.schema[column_name].dataType, typ)
            for typ in [
                sparktypes.LongType,
                sparktypes.DoubleType,
                sparktypes.IntegerType,
            ]
        ]
        if any(na_types):
            column = column.filter(~isnan(column[0]))
        if parse_strings_as_datetimes:
            column = self._apply_dateutil_parse(column)
            column = column.withColumn("constant", lit("constant")).withColumn(
                "lag", lag(column[0]).over(Window.orderBy(col("constant")))
            )
            column = column.withColumn("diff", datediff(col(column_name), col("lag")))
        else:
            column = (
                column.withColumn("constant", lit("constant"))
                .withColumn("lag", lag(column[0]).over(Window.orderBy(col("constant"))))
                .withColumn("diff", (column[0] - col("lag")))
            )
        column = column.withColumn(
            "diff", when(col("diff").isNull(), 1).otherwise(col("diff"))
        )
        if strictly:
            return column.withColumn(
                "__success", when((col("diff") >= 1), lit(True)).otherwise(lit(False))
            )
        else:
            return column.withColumn(
                "__success", when((col("diff") >= 0), lit(True)).otherwise(lit(False))
            )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_decreasing(
        self,
        column,
        strictly=False,
        mostly=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_name = column.schema.names[0]
        na_types = [
            isinstance(column.schema[column_name].dataType, typ)
            for typ in [
                sparktypes.LongType,
                sparktypes.DoubleType,
                sparktypes.IntegerType,
            ]
        ]
        if any(na_types):
            column = column.filter(~isnan(column[0]))
        if parse_strings_as_datetimes:
            column = self._apply_dateutil_parse(column)
            column = column.withColumn("constant", lit("constant")).withColumn(
                "lag", lag(column[0]).over(Window.orderBy(col("constant")))
            )
            column = column.withColumn("diff", datediff(col(column_name), col("lag")))
        else:
            column = (
                column.withColumn("constant", lit("constant"))
                .withColumn("lag", lag(column[0]).over(Window.orderBy(col("constant"))))
                .withColumn("diff", (column[0] - col("lag")))
            )
        column = column.withColumn(
            "diff", when(col("diff").isNull(), (-1)).otherwise(col("diff"))
        )
        if strictly:
            return column.withColumn(
                "__success",
                when((col("diff") <= (-1)), lit(True)).otherwise(lit(False)),
            )
        else:
            return column.withColumn(
                "__success", when((col("diff") <= 0), lit(True)).otherwise(lit(False))
            )

    @DocInherit
    @MetaSparkDFDataset.multicolumn_map_expectation
    def expect_multicolumn_sum_to_equal(
        self,
        column_list,
        sum_total,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        " Multi-Column Map Expectation\n\n        Expects that the sum of row values is the same for each row, summing only values in columns specified in\n        column_list, and equal to the specific value, sum_total.\n\n        Args:\n            column_list (List[str]):                 Set of columns to be checked\n            sum_total (int):                 expected sum of columns\n        "
        expression = "+".join([f"COALESCE({col}, 0)" for col in column_list.columns])
        column_list = column_list.withColumn("actual_total", expr(expression))
        return column_list.withColumn(
            "__success",
            when((col("actual_total") == sum_total), lit(True)).otherwise(lit(False)),
        )
