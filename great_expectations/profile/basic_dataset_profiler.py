
import logging
from great_expectations.core.profiler_types_mapping import ProfilerTypeMapping
from great_expectations.profile.base import DatasetProfiler, ProfilerCardinality, ProfilerDataType
try:
    from sqlalchemy.exc import OperationalError
except ModuleNotFoundError:
    OperationalError = RuntimeError
logger = logging.getLogger(__name__)

class BasicDatasetProfilerBase(DatasetProfiler):
    'BasicDatasetProfilerBase provides basic logic of inferring the type and the cardinality of columns\n    that is used by the dataset profiler classes that extend this class.\n    '
    INT_TYPE_NAMES = ProfilerTypeMapping.INT_TYPE_NAMES
    FLOAT_TYPE_NAMES = ProfilerTypeMapping.FLOAT_TYPE_NAMES
    STRING_TYPE_NAMES = ProfilerTypeMapping.STRING_TYPE_NAMES
    BOOLEAN_TYPE_NAMES = ProfilerTypeMapping.BOOLEAN_TYPE_NAMES
    DATETIME_TYPE_NAMES = ProfilerTypeMapping.DATETIME_TYPE_NAMES

    @classmethod
    def _get_column_type(cls, df, column):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        df.set_config_value('interactive_evaluation', True)
        try:
            if df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(ProfilerTypeMapping.INT_TYPE_NAMES))).success:
                type_ = ProfilerDataType.INT
            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(ProfilerTypeMapping.FLOAT_TYPE_NAMES))).success:
                type_ = ProfilerDataType.FLOAT
            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(ProfilerTypeMapping.STRING_TYPE_NAMES))).success:
                type_ = ProfilerDataType.STRING
            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(ProfilerTypeMapping.BOOLEAN_TYPE_NAMES))).success:
                type_ = ProfilerDataType.BOOLEAN
            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(ProfilerTypeMapping.DATETIME_TYPE_NAMES))).success:
                type_ = ProfilerDataType.DATETIME
            else:
                df.expect_column_values_to_be_in_type_list(column, type_list=None)
                type_ = ProfilerDataType.UNKNOWN
        except NotImplementedError:
            type_ = ProfilerDataType.UNKNOWN
        df.set_config_value('interactive_evaluation', False)
        return type_

    @classmethod
    def _get_column_cardinality(cls, df, column):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        num_unique = None
        pct_unique = None
        df.set_config_value('interactive_evaluation', True)
        try:
            num_unique = df.expect_column_unique_value_count_to_be_between(column, None, None).result['observed_value']
            pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(column, None, None).result['observed_value']
        except KeyError:
            logger.error(f'Failed to get cardinality of column {column:s} - continuing...')
        if ((num_unique is None) or (num_unique == 0) or (pct_unique is None)):
            cardinality = ProfilerCardinality.NONE
        elif (pct_unique == 1.0):
            cardinality = ProfilerCardinality.UNIQUE
        elif (pct_unique > 0.1):
            cardinality = ProfilerCardinality.VERY_MANY
        elif (pct_unique > 0.02):
            cardinality = ProfilerCardinality.MANY
        elif (num_unique == 1):
            cardinality = ProfilerCardinality.ONE
        elif (num_unique == 2):
            cardinality = ProfilerCardinality.TWO
        elif (num_unique < 60):
            cardinality = ProfilerCardinality.VERY_FEW
        elif (num_unique < 1000):
            cardinality = ProfilerCardinality.FEW
        else:
            cardinality = ProfilerCardinality.MANY
        df.set_config_value('interactive_evaluation', False)
        return cardinality

class BasicDatasetProfiler(BasicDatasetProfilerBase):
    "BasicDatasetProfiler is inspired by the beloved pandas_profiling project.\n\n    The profiler examines a batch of data and creates a report that answers the basic questions\n    most data practitioners would ask about a dataset during exploratory data analysis.\n    The profiler reports how unique the values in the column are, as well as the percentage of empty values in it.\n    Based on the column's type it provides a description of the column by computing a number of statistics,\n    such as min, max, mean and median, for numeric columns, and distribution of values, when appropriate.\n    "

    @classmethod
    def _profile(cls, dataset, configuration=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        df = dataset
        df.set_default_expectation_argument('catch_exceptions', True)
        df.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        df.expect_table_columns_to_match_ordered_list(None)
        df.set_config_value('interactive_evaluation', False)
        columns = df.get_table_columns()
        meta_columns = {}
        for column in columns:
            meta_columns[column] = {'description': ''}
        number_of_columns = len(columns)
        for (i, column) in enumerate(columns):
            logger.info(f'            Preparing column {(i + 1)} of {number_of_columns}: {column}')
            type_ = cls._get_column_type(df, column)
            cardinality = cls._get_column_cardinality(df, column)
            df.expect_column_values_to_not_be_null(column, mostly=0.5)
            df.expect_column_values_to_be_in_set(column, [], result_format='SUMMARY')
            if (type_ == ProfilerDataType.INT):
                if (cardinality == ProfilerCardinality.UNIQUE):
                    df.expect_column_values_to_be_unique(column)
                elif (cardinality in [ProfilerCardinality.ONE, ProfilerCardinality.TWO, ProfilerCardinality.VERY_FEW, ProfilerCardinality.FEW]):
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format='SUMMARY')
                elif (cardinality in [ProfilerCardinality.MANY, ProfilerCardinality.VERY_MANY, ProfilerCardinality.UNIQUE]):
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_stdev_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column, quantile_ranges={'quantiles': [0.05, 0.25, 0.5, 0.75, 0.95], 'value_ranges': [[None, None], [None, None], [None, None], [None, None], [None, None]]})
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None, threshold=None, result_format='COMPLETE')
                else:
                    pass
            elif (type_ == ProfilerDataType.FLOAT):
                if (cardinality == ProfilerCardinality.UNIQUE):
                    df.expect_column_values_to_be_unique(column)
                elif (cardinality in [ProfilerCardinality.ONE, ProfilerCardinality.TWO, ProfilerCardinality.VERY_FEW, ProfilerCardinality.FEW]):
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format='SUMMARY')
                elif (cardinality in [ProfilerCardinality.MANY, ProfilerCardinality.VERY_MANY, ProfilerCardinality.UNIQUE]):
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column, quantile_ranges={'quantiles': [0.05, 0.25, 0.5, 0.75, 0.95], 'value_ranges': [[None, None], [None, None], [None, None], [None, None], [None, None]]})
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None, threshold=None, result_format='COMPLETE')
                else:
                    pass
            elif (type_ == ProfilerDataType.STRING):
                df.expect_column_values_to_not_match_regex(column, '^\\s+|\\s+$')
                if (cardinality == ProfilerCardinality.UNIQUE):
                    df.expect_column_values_to_be_unique(column)
                elif (cardinality in [ProfilerCardinality.ONE, ProfilerCardinality.TWO, ProfilerCardinality.VERY_FEW, ProfilerCardinality.FEW]):
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format='SUMMARY')
                else:
                    pass
            elif (type_ == ProfilerDataType.DATETIME):
                df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                if (cardinality in [ProfilerCardinality.ONE, ProfilerCardinality.TWO, ProfilerCardinality.VERY_FEW, ProfilerCardinality.FEW]):
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format='SUMMARY')
            elif (cardinality == ProfilerCardinality.UNIQUE):
                df.expect_column_values_to_be_unique(column)
            elif (cardinality in [ProfilerCardinality.ONE, ProfilerCardinality.TWO, ProfilerCardinality.VERY_FEW, ProfilerCardinality.FEW]):
                df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format='SUMMARY')
            else:
                pass
        df.set_config_value('interactive_evaluation', True)
        expectation_suite = df.get_expectation_suite(suppress_warnings=True, discard_failed_expectations=False)
        expectation_suite.meta['columns'] = meta_columns
        return expectation_suite
