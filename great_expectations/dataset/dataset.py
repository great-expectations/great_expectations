
import inspect
import logging
from datetime import datetime
from functools import lru_cache, wraps
from itertools import zip_longest
from numbers import Number
from typing import Any, List, Optional, Set, Union
import numpy as np
import pandas as pd
from dateutil.parser import parse
from scipy import stats
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.data_asset.util import DocInherit, parse_result_format
from great_expectations.dataset.util import build_categorical_partition_object, build_continuous_partition_object, is_valid_categorical_partition_object, is_valid_partition_object
logger = logging.getLogger(__name__)
try:
    from sqlalchemy.sql import quoted_name
except:
    logger.debug('Unable to load quoted name from SqlAlchemy; install optional sqlalchemy dependency for support')
    quoted_name = None

class MetaDataset(DataAsset):
    '\n    Holds expectation decorators.\n    '

    @classmethod
    def column_map_expectation(cls, func) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Constructs an expectation using column-map semantics.\n\n        The column_map_expectation decorator handles boilerplate issues surrounding the common pattern of evaluating\n        truthiness of some condition on a per-row basis.\n\n        Args:\n            func (function):                 The function implementing a row-wise expectation. The function should take a column of data and                 return an equally-long column of boolean values corresponding to the truthiness of the                 underlying expectation.\n\n        Notes:\n            column_map_expectation intercepts and takes action based on the following parameters:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n            column_map_expectation *excludes null values* from being passed to the function\n\n            Depending on the `result_format` selected, column_map_expectation can additional data to a return object,             including `element_count`, `nonnull_values`, `nonnull_count`, `success_count`, `unexpected_list`, and             `unexpected_index_list`.             See :func:`_format_map_output <great_expectations.data_asset.dataset.Dataset._format_map_output>`\n\n        See also:\n            :func:`expect_column_values_to_be_in_set             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_set>`             for an example of a column_map_expectation\n        '
        raise NotImplementedError

    @classmethod
    def column_aggregate_expectation(cls, func):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Constructs an expectation using column-aggregate semantics.\n\n        The column_aggregate_expectation decorator handles boilerplate issues surrounding the common pattern of         evaluating truthiness of some condition on an aggregated-column basis.\n\n        Args:\n            func (function):                 The function implementing an expectation using an aggregate property of a column.                 The function should take a column of data and return the aggregate value it computes.\n\n        Notes:\n            column_aggregate_expectation *excludes null values* from being passed to the function\n\n        See also:\n            :func:`expect_column_mean_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_mean_to_be_between>`             for an example of a column_aggregate_expectation\n        '
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, column=None, result_format=None, row_condition=None, condition_parser=None, *args, **kwargs):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            if (result_format is None):
                result_format = self.default_expectation_args['result_format']
            result_format = parse_result_format(result_format)
            if (row_condition and self._supports_row_condition):
                self = self.query(row_condition, parser=condition_parser).reset_index(drop=True)
            element_count = self.get_row_count()
            if kwargs.get('column'):
                column = kwargs.get('column')
            if (column is not None):
                if (hasattr(self, 'engine') and self.batch_kwargs.get('use_quoted_name') and quoted_name):
                    column = quoted_name(column, quote=True)
                nonnull_count = self.get_column_nonnull_count(kwargs.get('column', column))
                args = tuple((column, *args))
            elif (kwargs.get('column_A') and kwargs.get('column_B')):
                try:
                    nonnull_count = (self[kwargs.get('column_A')].notnull() & self[kwargs.get('column_B')].notnull()).sum()
                except TypeError:
                    nonnull_count = None
            else:
                raise ValueError('The column_aggregate_expectation wrapper requires either column or both column_A and column_B as input.')
            if nonnull_count:
                null_count = (element_count - nonnull_count)
            else:
                null_count = None
            evaluation_result = func(self, *args, **kwargs)
            if ('success' not in evaluation_result):
                raise ValueError('Column aggregate expectation failed to return required information: success')
            if (('result' not in evaluation_result) or ('observed_value' not in evaluation_result['result'])):
                raise ValueError('Column aggregate expectation failed to return required information: observed_value')
            return_obj = {'success': bool(evaluation_result['success'])}
            if (result_format['result_format'] == 'BOOLEAN_ONLY'):
                return return_obj
            return_obj['result'] = {'observed_value': evaluation_result['result']['observed_value'], 'element_count': element_count}
            if null_count:
                return_obj['result']['missing_count'] = null_count
                if (element_count > 0):
                    return_obj['result']['missing_percent'] = ((null_count * 100.0) / element_count)
                else:
                    return_obj['result']['missing_percent'] = None
            else:
                return_obj['result']['missing_count'] = None
                return_obj['result']['missing_percent'] = None
            if (result_format['result_format'] == 'BASIC'):
                return return_obj
            if ('details' in evaluation_result['result']):
                return_obj['result']['details'] = evaluation_result['result']['details']
            if (result_format['result_format'] in ['SUMMARY', 'COMPLETE']):
                return return_obj
            raise ValueError(f"Unknown result_format {result_format['result_format']}.")
        return inner_wrapper

class Dataset(MetaDataset):
    _data_asset_type = 'Dataset'
    _supports_row_condition = False
    hashable_getters = ['get_column_min', 'get_column_max', 'get_column_mean', 'get_column_modes', 'get_column_median', 'get_column_quantiles', 'get_column_nonnull_count', 'get_column_stdev', 'get_column_sum', 'get_column_unique_count', 'get_column_value_counts', 'get_row_count', 'get_column_count', 'get_table_columns', 'get_column_count_in_range']

    def __init__(self, *args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.caching = kwargs.pop('caching', True)
        super().__init__(*args, **kwargs)
        if self.caching:
            for func in self.hashable_getters:
                caching_func = lru_cache(maxsize=None)(getattr(self, func))
                setattr(self, func, caching_func)

    @classmethod
    def from_dataset(cls, dataset=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'This base implementation naively passes arguments on to the real constructor, which\n        is suitable really when a constructor knows to take its own type. In general, this should be overridden'
        return cls(dataset)

    def get_row_count(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: int, table row count'
        raise NotImplementedError

    def get_column_count(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: int, table column count'
        raise NotImplementedError

    def get_table_columns(self) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: List[str], list of column names'
        raise NotImplementedError

    def get_column_nonnull_count(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: int'
        raise NotImplementedError

    def get_column_mean(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: float'
        raise NotImplementedError

    def get_column_value_counts(self, column, sort='value', collate=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get a series containing the frequency counts of unique values from the named column.\n\n        Args:\n            column: the column for which to obtain value_counts\n            sort (string): must be one of "value", "count", or "none".\n                - if "value" then values in the resulting partition object will be sorted lexigraphically\n                - if "count" then values will be sorted according to descending count (frequency)\n                - if "none" then values will not be sorted\n            collate (string): the collate (sort) method to be used on supported backends (SqlAlchemy only)\n\n\n        Returns:\n            pd.Series of value counts for a column, sorted according to the value requested in sort\n        '
        raise NotImplementedError

    def get_column_sum(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: float'
        raise NotImplementedError

    def get_column_max(self, column, parse_strings_as_datetimes=False) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: Any'
        raise NotImplementedError

    def get_column_min(self, column, parse_strings_as_datetimes=False) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: Any'
        raise NotImplementedError

    def get_column_unique_count(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: int'
        raise NotImplementedError

    def get_column_modes(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: List[Any], list of modes (ties OK)'
        raise NotImplementedError

    def get_column_median(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: Any'
        raise NotImplementedError

    def get_column_quantiles(self, column, quantiles, allow_relative_error=False) -> List[Any]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get the values in column closest to the requested quantiles\n        Args:\n            column (string): name of column\n            quantiles (tuple of float): the quantiles to return. quantiles             *must* be a tuple to ensure caching is possible\n\n        Returns:\n            List[Any]: the nearest values in the dataset to those quantiles\n        '
        raise NotImplementedError

    def get_column_stdev(self, column) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: float'
        raise NotImplementedError

    def get_column_partition(self, column, bins='uniform', n_bins=10, allow_relative_error=False):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Get a partition of the range of values in the specified column.\n\n        Args:\n            column: the name of the column\n            bins: 'uniform' for evenly spaced bins or 'quantile' for bins spaced according to quantiles\n            n_bins: the number of bins to produce\n            allow_relative_error: passed to get_column_quantiles, set to False for only precise\n                values, True to allow approximate values on systems with only binary choice (e.g. Redshift), and to a\n                value between zero and one for systems that allow specification of relative error (e.g.\n                SparkDFDataset).\n\n        Returns:\n            A list of bins\n        "
        if (bins == 'uniform'):
            (min_, max_) = self.get_column_quantiles(column, (0.0, 1.0), allow_relative_error=allow_relative_error)
            bins = np.linspace(start=float(min_), stop=float(max_), num=(n_bins + 1))
        elif (bins in ['ntile', 'quantile', 'percentile']):
            bins = self.get_column_quantiles(column, tuple(np.linspace(start=0, stop=1, num=(n_bins + 1))), allow_relative_error=allow_relative_error)
        elif (bins == 'auto'):
            nonnull_count = self.get_column_nonnull_count(column)
            sturges = np.log2((nonnull_count + 1))
            (min_, _25, _75, max_) = self.get_column_quantiles(column, (0.0, 0.25, 0.75, 1.0), allow_relative_error=allow_relative_error)
            iqr = (_75 - _25)
            if (iqr < 1e-10):
                n_bins = sturges
            else:
                fd = ((2 * float(iqr)) / (nonnull_count ** (1 / 3)))
                n_bins = max(int(np.ceil(sturges)), int(np.ceil((float((max_ - min_)) / fd))))
            bins = np.linspace(start=float(min_), stop=float(max_), num=(n_bins + 1))
        else:
            raise ValueError('Invalid parameter for bins argument')
        return bins

    def get_column_hist(self, column, bins) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get a histogram of column values\n        Args:\n            column: the column for which to generate the histogram\n            bins (tuple): the bins to slice the histogram. bins *must* be a tuple to ensure caching is possible\n\n        Returns: List[int], a list of counts corresponding to bins'
        raise NotImplementedError

    def get_column_count_in_range(self, column, min_val=None, max_val=None, strict_min=False, strict_max=True) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Returns: int'
        raise NotImplementedError

    def get_crosstab(self, column_A, column_B, bins_A=None, bins_B=None, n_bins_A=None, n_bins_B=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get crosstab of column_A and column_B, binning values if necessary'
        raise NotImplementedError

    def test_column_map_expectation_function(self, function, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Test a column map expectation function\n\n        Args:\n            function (func): The function to be tested. (Must be a valid column_map_expectation function.)\n            *args          : Positional arguments to be passed the the function\n            **kwargs       : Keyword arguments to be passed the the function\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n        Notes:\n            This function is a thin layer to allow quick testing of new expectation functions, without having to             define custom classes, etc. To use developed expectations from the command-line tool, you'll still need to             define custom classes, etc.\n\n            Check out :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations` for more information.\n        "
        new_function = self.column_map_expectation(function)
        return new_function(self, *args, **kwargs)

    def test_column_aggregate_expectation_function(self, function, *args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "Test a column aggregate expectation function\n\n        Args:\n            function (func): The function to be tested. (Must be a valid column_aggregate_expectation function.)\n            *args          : Positional arguments to be passed the the function\n            **kwargs       : Keyword arguments to be passed the the function\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n        Notes:\n            This function is a thin layer to allow quick testing of new expectation functions, without having to             define custom classes, etc. To use developed expectations from the command-line tool, you'll still need to             define custom classes, etc.\n\n            Check out :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations` for more information.\n        "
        new_function = self.column_aggregate_expectation(function)
        return new_function(self, *args, **kwargs)

    @DocInherit
    @DataAsset.expectation(['column'])
    def expect_column_to_exist(self, column, column_index=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the specified column to exist.\n\n        expect_column_to_exist is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            column (str):                 The column name.\n\n        Other Parameters:\n            column_index (int or None):                 If not None, checks the order of the columns. The expectation will fail if the                 column is not in location column_index (zero-indexed).\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.                 For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        columns = self.get_table_columns()
        if (column in columns):
            return {'success': ((column_index is None) or (columns.index(column) == column_index))}
        else:
            return {'success': False}

    @DocInherit
    @DataAsset.expectation(['column_list'])
    def expect_table_columns_to_match_ordered_list(self, column_list, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the columns to exactly match a specified list.\n\n        expect_table_columns_to_match_ordered_list is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            column_list (list of str):                 The column names, in the correct order.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        columns = self.get_table_columns()
        if ((column_list is None) or (list(columns) == list(column_list))):
            return {'success': True, 'result': {'observed_value': list(columns)}}
        else:
            number_of_columns = max(len(column_list), len(columns))
            column_index = range(number_of_columns)
            compared_lists = list(zip_longest(column_index, list(column_list), list(columns)))
            mismatched = [{'Expected Column Position': i, 'Expected': k, 'Found': v} for (i, k, v) in compared_lists if (k != v)]
            return {'success': False, 'result': {'observed_value': list(columns), 'details': {'mismatched': mismatched}}}

    @DocInherit
    @DataAsset.expectation(['column_set', 'exact_match'])
    def expect_table_columns_to_match_set(self, column_set: Optional[Union[(Set[str], List[str])]], exact_match: bool=True, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the columns to match a specified set.\n\n        expect_table_columns_to_match_set is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            column_set (set of str or list of str):                 The column names you wish to check. If given a list, it will be converted to                 a set before processing. Column names are case sensitive.\n            exact_match (bool):                 Whether to make sure there are no extra columns in either the dataset or in                 the column_set.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        column_set = (set(column_set) if (column_set is not None) else set())
        dataset_columns_list = self.get_table_columns()
        dataset_columns_set = set(dataset_columns_list)
        if (((column_set is None) and (exact_match is not True)) or (dataset_columns_set == column_set)):
            return {'success': True, 'result': {'observed_value': dataset_columns_list}}
        else:
            unexpected_list = sorted(list((dataset_columns_set - column_set)))
            missing_list = sorted(list((column_set - dataset_columns_set)))
            observed_value = sorted(dataset_columns_list)
            mismatched = {}
            if (len(unexpected_list) > 0):
                mismatched['unexpected'] = unexpected_list
            if (len(missing_list) > 0):
                mismatched['missing'] = missing_list
            result = {'observed_value': observed_value, 'details': {'mismatched': mismatched}}
            return_success = {'success': True, 'result': result}
            return_failed = {'success': False, 'result': result}
            if exact_match:
                return return_failed
            elif (len(missing_list) > 0):
                return return_failed
            else:
                return return_success

    @DocInherit
    @DataAsset.expectation(['min_value', 'max_value'])
    def expect_table_column_count_to_be_between(self, min_value=None, max_value=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the number of columns to be between two values.\n\n        expect_table_column_count_to_be_between is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Keyword Args:\n            min_value (int or None):                 The minimum number of columns, inclusive.\n            max_value (int or None):                 The maximum number of columns, inclusive.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            * min_value and max_value are both inclusive.\n            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable columns               has no minimum.\n            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable columns               has no maximum.\n\n        See Also:\n            expect_table_column_count_to_equal\n        '
        try:
            if (min_value is not None):
                if (not float(min_value).is_integer()):
                    raise ValueError('min_value must be integer')
            if (max_value is not None):
                if (not float(max_value).is_integer()):
                    raise ValueError('max_value must be integer')
        except ValueError:
            raise ValueError('min_value and max_value must be integers')
        column_count = self.get_column_count()
        if (min_value is not None):
            above_min = (column_count >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            below_max = (column_count <= max_value)
        else:
            below_max = True
        outcome = (above_min and below_max)
        return {'success': outcome, 'result': {'observed_value': column_count}}

    @DocInherit
    @DataAsset.expectation(['value'])
    def expect_table_column_count_to_equal(self, value, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the number of columns to equal a value.\n\n        expect_table_column_count_to_equal is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            value (int):                 The expected number of columns.\n\n        Other Parameters:\n            result_format (string or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            expect_table_column_count_to_be_between\n        '
        try:
            if (not float(value).is_integer()):
                raise ValueError('value must be an integer')
        except ValueError:
            raise ValueError('value must be an integer')
        column_count = self.get_column_count()
        return {'success': (column_count == value), 'result': {'observed_value': column_count}}

    @DocInherit
    @DataAsset.expectation(['min_value', 'max_value'])
    def expect_table_row_count_to_be_between(self, min_value=None, max_value=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the number of rows to be between two values.\n\n        expect_table_row_count_to_be_between is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Keyword Args:\n            min_value (int or None):                 The minimum number of rows, inclusive.\n            max_value (int or None):                 The maximum number of rows, inclusive.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            * min_value and max_value are both inclusive.\n            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has               no minimum.\n            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has               no maximum.\n\n        See Also:\n            expect_table_row_count_to_equal\n        '
        try:
            if (min_value is not None):
                if (not float(min_value).is_integer()):
                    raise ValueError('min_value must be integer')
            if (max_value is not None):
                if (not float(max_value).is_integer()):
                    raise ValueError('max_value must be integer')
        except ValueError:
            raise ValueError('min_value and max_value must be integers')
        if ((min_value is not None) and (max_value is not None) and (min_value > max_value)):
            raise ValueError('min_value cannot be greater than max_value')
        row_count = self.get_row_count()
        if (min_value is not None):
            above_min = (row_count >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            below_max = (row_count <= max_value)
        else:
            below_max = True
        outcome = (above_min and below_max)
        return {'success': outcome, 'result': {'observed_value': row_count}}

    @DocInherit
    @DataAsset.expectation(['value'])
    def expect_table_row_count_to_equal(self, value, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the number of rows to equal a value.\n\n        expect_table_row_count_to_equal is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            value (int):                 The expected number of rows.\n\n        Other Parameters:\n            result_format (string or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            expect_table_row_count_to_be_between\n        '
        try:
            if (not float(value).is_integer()):
                raise ValueError('value must be an integer')
        except ValueError:
            raise ValueError('value must be an integer')
        row_count = self.get_row_count()
        return {'success': (row_count == value), 'result': {'observed_value': row_count}}

    def expect_column_values_to_be_unique(self, column, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect each column value to be unique.\n\n        This expectation detects duplicates. All duplicated values are counted as exceptions.\n\n        For example, `[1, 2, 3, 3, 3]` will return `[3, 3, 3]` in `result.exceptions_list`, with         `unexpected_percent = 60.0`.\n\n        expect_column_values_to_be_unique is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n        '
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self, column, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to not be null.\n\n        To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an\n        np.NaN in pandas. Empty strings don\'t count as null unless they have been coerced to a null type.\n\n        expect_column_values_to_not_be_null is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_be_null             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_null>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_null(self, column, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to be null.\n\n        expect_column_values_to_be_null is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_not_be_null             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_null>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_of_type(self, column, type_, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect a column to contain values of a specified data type.\n\n        expect_column_values_to_be_of_type is a :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,\n        and also for PandasDataset where the column dtype and provided type_ are unambiguous constraints (any dtype\n        except \'object\' or dtype of \'object\' with type_ specified as \'object\').\n\n        For PandasDataset columns with dtype of \'object\' expect_column_values_to_be_of_type is a\n        :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will\n        independently check each row\'s type.\n\n        Args:\n            column (str):                 The column name.\n            type\\_ (str):                 A string representing the data type that each column should have as entries. Valid types are defined\n                by the current backend implementation and are dynamically loaded. For example, valid types for\n                PandasDataset include any numpy dtype values (such as \'int64\') or native python types (such as \'int\'),\n                whereas valid types for a SqlAlchemyDataset include types named by the current driver such as \'INTEGER\'\n                in most SQL dialects and \'TEXT\' in dialects such as postgresql. Valid types for SparkDFDataset include\n                \'StringType\', \'BooleanType\' and other pyspark-defined type names.\n\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See also:\n            :func:`expect_column_values_to_be_in_type_list             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_type_list>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_in_type_list(self, column, type_list, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect a column to contain values from a specified type list.\n\n        expect_column_values_to_be_in_type_list is a :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,\n        and also for PandasDataset where the column dtype provides an unambiguous constraints (any dtype except\n        \'object\'). For PandasDataset columns with dtype of \'object\' expect_column_values_to_be_of_type is a\n        :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will\n        independently check each row\'s type.\n\n        Args:\n            column (str):                 The column name.\n            type_list (str):                 A list of strings representing the data type that each column should have as entries. Valid types are\n                defined by the current backend implementation and are dynamically loaded. For example, valid types for\n                PandasDataset include any numpy dtype values (such as \'int64\') or native python types (such as \'int\'),\n                whereas valid types for a SqlAlchemyDataset include types named by the current driver such as \'INTEGER\'\n                in most SQL dialects and \'TEXT\' in dialects such as postgresql. Valid types for SparkDFDataset include\n                \'StringType\', \'BooleanType\' and other pyspark-defined type names.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See also:\n            :func:`expect_column_values_to_be_of_type             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_of_type>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_in_set(self, column, value_set, mostly=None, parse_strings_as_datetimes=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect each column value to be in a given set.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3]\n            >>> my_df.expect_column_values_to_be_in_set(\n                "my_col",\n                [2,3]\n            )\n            {\n              "success": false\n              "result": {\n                "unexpected_count": 1\n                "unexpected_percent": 16.66666666666666666,\n                "unexpected_percent_nonmissing": 16.66666666666666666,\n                "partial_unexpected_list": [\n                  1\n                ],\n              },\n            }\n\n        expect_column_values_to_be_in_set is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            value_set (set-like):                 A set of objects used for comparison.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n            parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed as                 datetimes before making comparisons.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_not_be_in_set             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_in_set>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self, column, value_set, mostly=None, parse_strings_as_datetimes=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to not be in the set.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3]\n            >>> my_df.expect_column_values_to_not_be_in_set(\n                "my_col",\n                [1,2]\n            )\n            {\n              "success": false\n              "result": {\n                "unexpected_count": 3\n                "unexpected_percent": 50.0,\n                "unexpected_percent_nonmissing": 50.0,\n                "partial_unexpected_list": [\n                  1, 2, 2\n                ],\n              },\n            }\n\n        expect_column_values_to_not_be_in_set is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            value_set (set-like):                 A set of objects used for comparison.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_be_in_set             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_set>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, allow_cross_type_comparisons=None, parse_strings_as_datetimes=False, output_strftime_format=None, mostly=None, row_condition=None, condition_parser=None, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be between a minimum value and a maximum value (inclusive).\n\n        expect_column_values_to_be_between is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            min_value (comparable type or None): The minimum value for a column entry.\n            max_value (comparable type or None): The maximum value for a column entry.\n\n        Keyword Args:\n            strict_min (boolean):\n                If True, values must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, values must be strictly smaller than max_value, default=False\n             allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and                string). Otherwise, attempting such comparisons will raise an exception.\n            parse_strings_as_datetimes (boolean or None) : If True, parse min_value, max_value, and all non-null column                values to datetimes before making comparisons.\n            output_strftime_format (str or None):                 A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.\n\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound, and there is no minimum value checked.\n            * If max_value is None, then min_value is treated as a lower bound, and there is no maximum value checked.\n\n        See Also:\n            :func:`expect_column_value_lengths_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_be_between>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_increasing(self, column, strictly=None, parse_strings_as_datetimes=False, mostly=None, row_condition=None, condition_parser=None, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to be increasing.\n\n        By default, this expectation only works for numeric or datetime data.\n        When `parse_strings_as_datetimes=True`, it can also parse strings to datetimes.\n\n        If `strictly=True`, then this expectation is only satisfied if each consecutive value\n        is strictly increasing--equal values are treated as failures.\n\n        expect_column_values_to_be_increasing is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            strictly (Boolean or None):                 If True, values must be strictly greater than previous values\n            parse_strings_as_datetimes (boolean or None) :                 If True, all non-null column values to datetimes before making comparisons\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without\n                modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_be_decreasing             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_decreasing>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_decreasing(self, column, strictly=None, parse_strings_as_datetimes=False, mostly=None, row_condition=None, condition_parser=None, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to be decreasing.\n\n        By default, this expectation only works for numeric or datetime data.\n        When `parse_strings_as_datetimes=True`, it can also parse strings to datetimes.\n\n        If `strictly=True`, then this expectation is only satisfied if each consecutive value\n        is strictly decreasing--equal values are treated as failures.\n\n        expect_column_values_to_be_decreasing is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            strictly (Boolean or None):                 If True, values must be strictly greater than previous values\n            parse_strings_as_datetimes (boolean or None) :                 If True, all non-null column values to datetimes before making comparisons\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_be_increasing             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_increasing>`\n\n        '
        raise NotImplementedError

    def expect_column_value_lengths_to_be_between(self, column, min_value=None, max_value=None, mostly=None, row_condition=None, condition_parser=None, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be strings with length between a minimum value and a maximum value (inclusive).\n\n        This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.\n\n        expect_column_value_lengths_to_be_between is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            min_value (int or None):                 The minimum value for a column entry length.\n            max_value (int or None):                 The maximum value for a column entry length.\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            * min_value and max_value are both inclusive.\n            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has               no minimum.\n            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has               no maximum.\n\n        See Also:\n            :func:`expect_column_value_lengths_to_equal             <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_equal>`\n\n        '
        raise NotImplementedError

    def expect_column_value_lengths_to_equal(self, column, value, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be strings with length equal to the provided value.\n\n        This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.\n\n        expect_column_values_to_be_between is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            value (int or None):                 The expected value for a column entry length.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_value_lengths_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_be_between>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_match_regex(self, column, regex, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be strings that match a given regular expression. Valid matches can be found         anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat",         "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".\n\n        expect_column_values_to_match_regex is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            regex (str):                 The regular expression the column entries should match.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_not_match_regex             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex>`\n\n            :func:`expect_column_values_to_match_regex_list             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be strings that do NOT match a given regular expression. The regex must not match         any portion of the provided string. For example, "[at]+" would identify the following strings as expected:         "fish", "dog", and the following as unexpected: "cat", "hat".\n\n        expect_column_values_to_not_match_regex is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            regex (str):                 The regular expression the column entries should NOT match.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_match_regex             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex>`\n\n            :func:`expect_column_values_to_match_regex_list             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self, column, regex_list, match_on='any', mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column entries to be strings that can be matched to either any of or all of a list of regular\n        expressions. Matches can be anywhere in the string.\n\n        expect_column_values_to_match_regex_list is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            regex_list (list):                 The list of regular expressions which the column entries should match\n\n        Keyword Args:\n            match_on= (string):                 "any" or "all".\n                Use "any" if the value should match at least one regular expression in the list.\n                Use "all" if it should match each regular expression in the list.\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_match_regex             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex>`\n\n            :func:`expect_column_values_to_not_match_regex             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_not_match_regex_list(self, column, regex_list, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can\n        be anywhere in the string.\n\n        expect_column_values_to_not_match_regex_list is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            regex_list (list):                 The list of regular expressions which the column entries should not match\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_match_regex_list             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_match_strftime_format(self, column, strftime_format, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be strings representing a date or time with a given format.\n\n        expect_column_values_to_match_strftime_format is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            strftime_format (str):                 A strftime format string to use for matching\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be parsable using dateutil.\n\n        expect_column_values_to_be_dateutil_parseable is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_column_values_to_be_json_parseable(self, column, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be data written in JavaScript Object Notation.\n\n        expect_column_values_to_be_json_parseable is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_match_json_schema             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_json_schema>`\n\n        '
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self, column, json_schema, mostly=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column entries to be JSON objects matching a given JSON schema.\n\n        expect_column_values_to_match_json_schema is a         :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n\n        Keyword Args:\n            mostly (None or a float between 0 and 1):                 Return `"success": True` if at least mostly fraction of values match the expectation.                 For more detail, see :ref:`mostly`.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_values_to_be_json_parseable             <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_json_parseable>`\n\n\n            The `JSON-schema docs <http://json-schema.org/>`_.\n        '
        raise NotImplementedError

    def expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(self, column, distribution, p_value=0.05, params=None, result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect the column values to be distributed similarly to a scipy distribution. \n        This expectation compares the provided column to the specified continuous distribution with a parametric         Kolmogorov-Smirnov test. The K-S test compares the provided column to the cumulative density function (CDF) of         the specified scipy distribution. If you don\'t know the desired distribution shape parameters, use the         `ge.dataset.util.infer_distribution_parameters()` utility function to estimate them.\n\n        It returns \'success\'=True if the p-value from the K-S test is greater than or equal to the provided p-value.\n\n        ``expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than`` is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            distribution (str):                 The scipy distribution name. See: `<https://docs.scipy.org/doc/scipy/reference/stats.html>`_ Currently\n                supported distributions are listed in the Notes section below.\n            p_value (float):                 The threshold p-value for a passing test. Default is 0.05.\n            params (dict or list) :                 A dictionary or positional list of shape parameters that describe the distribution you want to test the                data against. Include key values specific to the distribution from the appropriate scipy                 distribution CDF function. \'loc\' and \'scale\' are used as translational parameters.                See `<https://docs.scipy.org/doc/scipy/reference/stats.html#continuous-distributions>`_\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "details":\n                        "expected_params" (dict): The specified or inferred parameters of the distribution to test                         against\n                        "ks_results" (dict): The raw result of stats.kstest()\n                }\n\n            * The Kolmogorov-Smirnov test\'s null hypothesis is that the column is similar to the provided distribution.\n            * Supported scipy distributions:\n\n              * norm\n              * beta\n              * gamma\n              * uniform\n              * chi2\n              * expon\n\n        '
        raise NotImplementedError

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_distinct_values_to_be_in_set(self, column, value_set, parse_strings_as_datetimes=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the set of distinct column values to be contained by a given set.\n\n        The success value for this expectation will match that of expect_column_values_to_be_in_set. However,\n        expect_column_distinct_values_to_be_in_set is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3]\n            >>> my_df.expect_column_distinct_values_to_be_in_set(\n                "my_col",\n                [2, 3, 4]\n            )\n            {\n              "success": false\n              "result": {\n                "observed_value": [1,2,3],\n                "details": {\n                  "value_counts": [\n                    {\n                      "value": 1,\n                      "count": 1\n                    },\n                    {\n                      "value": 2,\n                      "count": 1\n                    },\n                    {\n                      "value": 3,\n                      "count": 1\n                    }\n                  ]\n                }\n              }\n            }\n\n        Args:\n            column (str):                 The column name.\n            value_set (set-like):                 A set of objects used for comparison.\n\n        Keyword Args:\n            parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed             as datetimes before making comparisons.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_distinct_values_to_contain_set             <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_contain_set>`\n\n        '
        observed_value_counts = self.get_column_value_counts(column)
        if (value_set is None):
            success = True
            parsed_observed_value_set = set(observed_value_counts.index)
        else:
            if parse_strings_as_datetimes:
                parsed_value_set = self._parse_value_set(value_set)
                parsed_observed_value_set = set(self._parse_value_set(observed_value_counts.index))
            else:
                parsed_value_set = value_set
                parsed_observed_value_set = set(observed_value_counts.index)
            expected_value_set = set(parsed_value_set)
            success = parsed_observed_value_set.issubset(expected_value_set)
        return {'success': success, 'result': {'observed_value': sorted(list(parsed_observed_value_set)), 'details': {'value_counts': observed_value_counts}}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_distinct_values_to_equal_set(self, column, value_set, parse_strings_as_datetimes=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the set of distinct column values to equal a given set.\n\n        In contrast to expect_column_distinct_values_to_contain_set() this ensures not only that a certain set of         values are present in the column but that these *and only these* values are present.\n\n        expect_column_distinct_values_to_equal_set is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3]\n            >>> my_df.expect_column_distinct_values_to_equal_set(\n                "my_col",\n                [2,3]\n            )\n            {\n              "success": false\n              "result": {\n                "observed_value": [1,2,3]\n              },\n            }\n\n        Args:\n            column (str):                 The column name.\n            value_set (set-like):                 A set of objects used for comparison.\n\n        Keyword Args:\n            parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed as                 datetimes before making comparisons.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_distinct_values_to_contain_set             <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_contain_set>`\n\n        '
        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        observed_value_counts = self.get_column_value_counts(column)
        expected_value_set = set(parsed_value_set)
        observed_value_set = set(observed_value_counts.index)
        return {'success': (observed_value_set == expected_value_set), 'result': {'observed_value': sorted(list(observed_value_set)), 'details': {'value_counts': observed_value_counts}}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_distinct_values_to_contain_set(self, column, value_set, parse_strings_as_datetimes=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the set of distinct column values to contain a given set.\n\n        In contrast to expect_column_values_to_be_in_set() this ensures not that all column values are members of the\n        given set but that values from the set *must* be present in the column.\n\n        expect_column_distinct_values_to_contain_set is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3]\n            >>> my_df.expect_column_distinct_values_to_contain_set(\n                "my_col",\n                [2,3]\n            )\n            {\n            "success": true\n            "result": {\n                "observed_value": [1,2,3]\n            },\n            }\n\n        Args:\n            column (str):                 The column name.\n            value_set (set-like):                 A set of objects used for comparison.\n\n        Keyword Args:\n            parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed as                 datetimes before making comparisons.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            :func:`expect_column_distinct_values_to_equal_set             <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_equal_set>`\n\n        '
        observed_value_counts = self.get_column_value_counts(column)
        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
            observed_value_counts.index = pd.to_datetime(observed_value_counts.index)
        else:
            parsed_value_set = value_set
        expected_value_set = set(parsed_value_set)
        observed_value_set = set(observed_value_counts.index)
        return {'success': observed_value_set.issuperset(expected_value_set), 'result': {'observed_value': sorted(list(observed_value_set)), 'details': {'value_counts': observed_value_counts}}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_mean_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column mean to be between a minimum value and a maximum value (inclusive).\n\n        expect_column_mean_to_be_between is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            min_value (float or None):                 The minimum value for the column mean.\n            max_value (float or None):                 The maximum value for the column mean.\n            strict_min (boolean):\n                If True, the column mean must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the column mean must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The true mean for the column\n                }\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound.\n            * If max_value is None, then min_value is treated as a lower bound.\n\n        See Also:\n            :func:`expect_column_median_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_median_to_be_between>`\n\n            :func:`expect_column_stdev_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_stdev_to_be_between>`\n\n        '
        if ((min_value is not None) and (not isinstance(min_value, Number))):
            raise ValueError('min_value must be a datetime (for datetime columns) or number')
        if ((max_value is not None) and (not isinstance(max_value, Number))):
            raise ValueError('max_value must be a datetime (for datetime columns) or number')
        column_mean = self.get_column_mean(column)
        if (column_mean is None):
            return {'success': False, 'result': {'observed_value': column_mean}}
        if (min_value is not None):
            if strict_min:
                above_min = (column_mean > min_value)
            else:
                above_min = (column_mean >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            if strict_max:
                below_max = (column_mean < max_value)
            else:
                below_max = (column_mean <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': column_mean}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_median_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column median to be between a minimum value and a maximum value.\n\n        expect_column_median_to_be_between is a         :func:`column_aggregate_expectation         <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            min_value (int or None):                 The minimum value for the column median.\n            max_value (int or None):                 The maximum value for the column median.\n            strict_min (boolean):\n                If True, the column median must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the column median must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The true median for the column\n                }\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        See Also:\n            :func:`expect_column_mean_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_mean_to_be_between>`\n\n            :func:`expect_column_stdev_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_stdev_to_be_between>`\n\n        '
        column_median = self.get_column_median(column)
        if (column_median is None):
            return {'success': False, 'result': {'observed_value': None}}
        if (min_value is not None):
            if strict_min:
                above_min = (column_median > min_value)
            else:
                above_min = (column_median >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            if strict_max:
                below_max = (column_median < max_value)
            else:
                below_max = (column_median <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': column_median}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_quantile_values_to_be_between(self, column, quantile_ranges, allow_relative_error=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect specific provided column quantiles to be between provided minimum and maximum values.\n\n        ``quantile_ranges`` must be a dictionary with two keys:\n\n            * ``quantiles``: (list of float) increasing ordered list of desired quantile values\n\n            * ``value_ranges``: (list of lists): Each element in this list consists of a list with two values, a lower               and upper bound (inclusive) for the corresponding quantile.\n\n\n        For each provided range:\n\n            * min_value and max_value are both inclusive.\n            * If min_value is None, then max_value is treated as an upper bound only\n            * If max_value is None, then min_value is treated as a lower bound only\n\n        The length of the quantiles list and quantile_values list must be equal.\n\n        For example:\n        ::\n\n            # my_df.my_col = [1,2,2,3,3,3,4]\n            >>> my_df.expect_column_quantile_values_to_be_between(\n                "my_col",\n                {\n                    "quantiles": [0., 0.333, 0.6667, 1.],\n                    "value_ranges": [[0,1], [2,3], [3,4], [4,5]]\n                }\n            )\n            {\n              "success": True,\n                "result": {\n                  "observed_value": {\n                    "quantiles: [0., 0.333, 0.6667, 1.],\n                    "values": [1, 2, 3, 4],\n                  }\n                  "element_count": 7,\n                  "missing_count": 0,\n                  "missing_percent": 0.0,\n                  "details": {\n                    "success_details": [true, true, true, true]\n                  }\n                }\n              }\n            }\n\n        `expect_column_quantile_values_to_be_between` can be computationally intensive for large datasets.\n\n        expect_column_quantile_values_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            quantile_ranges (dictionary):                 Quantiles and associated value ranges for the column. See above for details.\n            allow_relative_error (boolean):                 Whether to allow relative error in quantile communications on backends that support or require it.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n            details.success_details\n\n        See Also:\n            :func:`expect_column_min_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_min_to_be_between>`\n\n            :func:`expect_column_max_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_max_to_be_between>`\n\n            :func:`expect_column_median_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_median_to_be_between>`\n\n        '
        quantiles = quantile_ranges['quantiles']
        quantile_value_ranges = quantile_ranges['value_ranges']
        if (len(quantiles) != len(quantile_value_ranges)):
            raise ValueError('quantile_values and quantiles must have the same number of elements')
        quantile_vals = self.get_column_quantiles(column, tuple(quantiles), allow_relative_error=allow_relative_error)
        comparison_quantile_ranges = [[((- np.inf) if (lower_bound is None) else lower_bound), (np.inf if (upper_bound is None) else upper_bound)] for (lower_bound, upper_bound) in quantile_value_ranges]
        success_details = [(range_[0] <= quantile_vals[idx] <= range_[1]) for (idx, range_) in enumerate(comparison_quantile_ranges)]
        return {'success': np.all(success_details), 'result': {'observed_value': {'quantiles': quantiles, 'values': quantile_vals}, 'details': {'success_details': success_details}}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_stdev_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column standard deviation to be between a minimum value and a maximum value.\n        Uses sample standard deviation (normalized by N-1).\n\n        expect_column_stdev_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            min_value (float or None):                 The minimum value for the column standard deviation.\n            max_value (float or None):                 The maximum value for the column standard deviation.\n            strict_min (boolean):\n                If True, the column standard deviation must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the column standard deviation must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The true standard deviation for the column\n                }\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        See Also:\n            :func:`expect_column_mean_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_mean_to_be_between>`\n\n            :func:`expect_column_median_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_median_to_be_between>`\n\n        '
        column_stdev = self.get_column_stdev(column)
        if (min_value is not None):
            if strict_min:
                above_min = (column_stdev > min_value)
            else:
                above_min = (column_stdev >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            if strict_max:
                below_max = (column_stdev < max_value)
            else:
                below_max = (column_stdev <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': column_stdev}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_unique_value_count_to_be_between(self, column, min_value=None, max_value=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the number of unique values to be between a minimum value and a maximum value.\n\n        expect_column_unique_value_count_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            min_value (int or None):                 The minimum number of unique values allowed.\n            max_value (int or None):                 The maximum number of unique values allowed.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (int) The number of unique values in the column\n                }\n\n            * min_value and max_value are both inclusive.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        See Also:\n            :func:`expect_column_proportion_of_unique_values_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_proportion_of_unique_values_to_be_between>`\n\n        '
        unique_value_count = self.get_column_unique_count(column)
        if (unique_value_count is None):
            return {'success': False, 'result': {'observed_value': unique_value_count}}
        if (min_value is not None):
            above_min = (unique_value_count >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            below_max = (unique_value_count <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': unique_value_count}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value=0, max_value=1, strict_min=False, strict_max=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the proportion of unique values to be between a minimum value and a maximum value.\n\n        For example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total         values for a proportion of 0.4.\n\n        expect_column_proportion_of_unique_values_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n\n        Args:\n            column (str):                 The column name.\n            min_value (float or None):                 The minimum proportion of unique values. (Proportions are on the range 0 to 1)\n            max_value (float or None):                 The maximum proportion of unique values. (Proportions are on the range 0 to 1)\n            strict_min (boolean):\n                If True, the minimum proportion of unique values must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the maximum proportion of unique values must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The proportion of unique values in the column\n                }\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        See Also:\n            :func:`expect_column_unique_value_count_to_be_between             <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`\n\n        '
        unique_value_count = self.get_column_unique_count(column)
        total_value_count = self.get_column_nonnull_count(column)
        if (total_value_count > 0):
            proportion_unique = (float(unique_value_count) / total_value_count)
        else:
            proportion_unique = None
        if (min_value is not None):
            if strict_min:
                above_min = (proportion_unique > min_value)
            else:
                above_min = (proportion_unique >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            if strict_max:
                below_max = (proportion_unique < max_value)
            else:
                below_max = (proportion_unique <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': proportion_unique}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_most_common_value_to_be_in_set(self, column, value_set, ties_okay=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the most common value to be within the designated value set\n\n        expect_column_most_common_value_to_be_in_set is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name\n            value_set (set-like):                 A list of potential values to match\n\n        Keyword Args:\n            ties_okay (boolean or None):                 If True, then the expectation will still succeed if values outside the designated set are as common                 (but not more common) than designated values\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (list) The most common values in the column\n                }\n\n            `observed_value` contains a list of the most common values.\n            Often, this will just be a single element. But if there\'s a tie for most common among multiple values,\n            `observed_value` will contain a single copy of each most common value.\n\n        '
        mode_list = self.get_column_modes(column)
        intersection_count = len(set(value_set).intersection(mode_list))
        if ties_okay:
            success = (intersection_count > 0)
        else:
            success = ((len(mode_list) == 1) and (intersection_count == 1))
        return {'success': success, 'result': {'observed_value': mode_list}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_sum_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column to sum to be between an min and max value\n\n        expect_column_sum_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name\n            min_value (comparable type or None):                 The minimal sum allowed.\n            max_value (comparable type or None):                 The maximal sum allowed.\n            strict_min (boolean):\n                If True, the minimal sum must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the maximal sum must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (list) The actual column sum\n                }\n\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        '
        column_sum = self.get_column_sum(column)
        if (column_sum is None):
            return {'success': False, 'result': {'observed_value': column_sum}}
        if (min_value is not None):
            if strict_min:
                above_min = (column_sum > min_value)
            else:
                above_min = (column_sum >= min_value)
        else:
            above_min = True
        if (max_value is not None):
            if strict_max:
                below_max = (column_sum < max_value)
            else:
                below_max = (column_sum <= max_value)
        else:
            below_max = True
        success = (above_min and below_max)
        return {'success': success, 'result': {'observed_value': column_sum}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_min_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, parse_strings_as_datetimes=False, output_strftime_format=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column minimum to be between an min and max value\n\n        expect_column_min_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name\n            min_value (comparable type or None):                 The minimal column minimum allowed.\n            max_value (comparable type or None):                 The maximal column minimum allowed.\n            strict_min (boolean):\n                If True, the minimal column minimum must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the maximal column minimum must be strictly smaller than max_value, default=False\n\n        Keyword Args:\n            parse_strings_as_datetimes (Boolean or None):                 If True, parse min_value, max_values, and all non-null column values to datetimes before making                 comparisons.\n            output_strftime_format (str or None):                 A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (list) The actual column min\n                }\n\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        '
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)
            if max_value:
                max_value = parse(max_value)
        column_min = self.get_column_min(column, parse_strings_as_datetimes)
        if (column_min is None):
            success = False
        else:
            if (min_value is not None):
                if isinstance(column_min, datetime):
                    try:
                        min_value = parse(min_value)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Something went wrong when parsing 'min_value': {e}")
                if strict_min:
                    above_min = (column_min > min_value)
                else:
                    above_min = (column_min >= min_value)
            else:
                above_min = True
            if (max_value is not None):
                if isinstance(column_min, datetime):
                    try:
                        max_value = parse(max_value)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Something went wrong when parsing 'max_value': {e}")
                if strict_max:
                    below_max = (column_min < max_value)
                else:
                    below_max = (column_min <= max_value)
            else:
                below_max = True
            success = (above_min and below_max)
        if parse_strings_as_datetimes:
            if output_strftime_format:
                column_min = datetime.strftime(column_min, output_strftime_format)
            else:
                column_min = str(column_min)
        return {'success': success, 'result': {'observed_value': column_min}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_max_to_be_between(self, column, min_value=None, max_value=None, strict_min=False, strict_max=False, parse_strings_as_datetimes=False, output_strftime_format=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the column max to be between an min and max value\n\n        expect_column_max_to_be_between is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name\n            min_value (comparable type or None):                 The minimum number of unique values allowed.\n            max_value (comparable type or None):                 The maximum number of unique values allowed.\n\n        Keyword Args:\n            parse_strings_as_datetimes (Boolean or None):                 If True, parse min_value, max_values, and all non-null column values to datetimes before making                 comparisons.\n            output_strftime_format (str or None):                 A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.\n            strict_min (boolean):\n                If True, the minimal column minimum must be strictly larger than min_value, default=False\n            strict_max (boolean):\n                If True, the maximal column minimum must be strictly smaller than max_value, default=False\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (list) The actual column max\n                }\n\n\n            * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.\n            * If min_value is None, then max_value is treated as an upper bound\n            * If max_value is None, then min_value is treated as a lower bound\n\n        '
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)
            if max_value:
                max_value = parse(max_value)
        column_max = self.get_column_max(column, parse_strings_as_datetimes)
        if (column_max is None):
            success = False
        else:
            if (min_value is not None):
                if isinstance(column_max, datetime):
                    try:
                        min_value = parse(min_value)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Something went wrong when parsing 'min_value': {e}")
                if strict_min:
                    above_min = (column_max > min_value)
                else:
                    above_min = (column_max >= min_value)
            else:
                above_min = True
            if (max_value is not None):
                if isinstance(column_max, datetime):
                    try:
                        max_value = parse(max_value)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Something went wrong when parsing 'max_value': {e}")
                if strict_max:
                    below_max = (column_max < max_value)
                else:
                    below_max = (column_max <= max_value)
            else:
                below_max = True
            success = (above_min and below_max)
        if parse_strings_as_datetimes:
            if output_strftime_format:
                column_max = datetime.strftime(column_max, output_strftime_format)
            else:
                column_max = str(column_max)
        return {'success': success, 'result': {'observed_value': column_max}}

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_chisquare_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, tail_weight_holdout=0, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to be distributed similarly to the provided categorical partition. \n        This expectation compares categorical distributions using a Chi-squared test.         It returns `success=True` if values in the column match the distribution of the provided partition.\n\n\n        expect_column_chisquare_test_p_value_to_be_greater_than is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            partition_object (dict):                 The expected partition object (see :ref:`partition_object`).\n            p (float):                 The p-value threshold for rejecting the null hypothesis of the Chi-Squared test.                For values below the specified threshold, the expectation will return `success=False`,                rejecting the null hypothesis that the distributions are the same.                Defaults to 0.05.\n\n        Keyword Args:\n            tail_weight_holdout (float between 0 and 1 or None):                 The amount of weight to split uniformly between values observed in the data but not present in the                 provided partition. tail_weight_holdout provides a mechanism to make the test less strict by                 assigning positive weights to unknown values observed in the data that are not present in the                 partition.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The true p-value of the Chi-squared test\n                    "details": {\n                        "observed_partition" (dict):\n                            The partition observed in the data.\n                        "expected_partition" (dict):\n                            The partition expected from the data, after including tail_weight_holdout\n                    }\n                }\n        '
        if (not is_valid_categorical_partition_object(partition_object)):
            raise ValueError('Invalid partition object.')
        element_count = self.get_column_nonnull_count(column)
        observed_frequencies = self.get_column_value_counts(column)
        expected_column = (pd.Series(partition_object['weights'], index=partition_object['values'], name='expected') * element_count)
        test_df = pd.concat([expected_column, observed_frequencies], axis=1)
        na_counts = test_df.isnull().sum()
        test_df['count'] = test_df['count'].fillna(0)
        if (na_counts['expected'] > 0):
            test_df['expected'] *= (1 - tail_weight_holdout)
            test_df['expected'] = test_df['expected'].fillna((element_count * (tail_weight_holdout / na_counts['expected'])))
        test_result = stats.chisquare(test_df['count'], test_df['expected'])[1]
        expected_weights = (test_df['expected'] / test_df['expected'].sum()).tolist()
        observed_weights = (test_df['count'] / test_df['count'].sum()).tolist()
        return {'success': (test_result > p), 'result': {'observed_value': test_result, 'details': {'observed_partition': {'values': test_df.index.tolist(), 'weights': observed_weights}, 'expected_partition': {'values': test_df.index.tolist(), 'weights': expected_weights}}}}

    def expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, bootstrap_samples=None, bootstrap_sample_size=None, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect column values to be distributed similarly to the provided continuous partition. This expectation         compares continuous distributions using a bootstrapped Kolmogorov-Smirnov test. It returns `success=True` if         values in the column match the distribution of the provided partition.\n\n        The expected cumulative density function (CDF) is constructed as a linear interpolation between the bins,         using the provided weights. Consequently the test expects a piecewise uniform distribution using the bins from         the provided partition object.\n\n        ``expect_column_bootstrapped_ks_test_p_value_to_be_greater_than`` is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            partition_object (dict):                 The expected partition object (see :ref:`partition_object`).\n            p (float):                 The p-value threshold for the Kolmogorov-Smirnov test.\n                For values below the specified threshold the expectation will return `success=False`, rejecting the                 null hypothesis that the distributions are the same.                 Defaults to 0.05.\n\n        Keyword Args:\n            bootstrap_samples (int):                 The number bootstrap rounds. Defaults to 1000.\n            bootstrap_sample_size (int):                 The number of samples to take from the column for each bootstrap. A larger sample will increase the                 specificity of the test. Defaults to 2 * len(partition_object[\'weights\'])\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                    "observed_value": (float) The true p-value of the KS test\n                    "details": {\n                        "bootstrap_samples": The number of bootstrap rounds used\n                        "bootstrap_sample_size": The number of samples taken from\n                            the column in each bootstrap round\n                        "observed_cdf": The cumulative density function observed\n                            in the data, a dict containing \'x\' values and cdf_values\n                            (suitable for plotting)\n                        "expected_cdf" (dict):\n                            The cumulative density function expected based on the\n                            partition object, a dict containing \'x\' values and\n                            cdf_values (suitable for plotting)\n                        "observed_partition" (dict):\n                            The partition observed on the data, using the provided\n                            bins but also expanding from min(column) to max(column)\n                        "expected_partition" (dict):\n                            The partition expected from the data. For KS test,\n                            this will always be the partition_object parameter\n                    }\n                }\n\n        '
        raise NotImplementedError

    @DocInherit
    @MetaDataset.column_aggregate_expectation
    def expect_column_kl_divergence_to_be_less_than(self, column, partition_object=None, threshold=None, tail_weight_holdout=0, internal_weight_holdout=0, bucketize_data=True, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the         partition object to be lower than the provided threshold.\n\n        KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger the         difference between the two distributions. A relative entropy of zero indicates that the data are         distributed identically, `when binned according to the provided partition`.\n\n        In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.\n\n        This expectation works on both categorical and continuous partitions. See notes below for details.\n\n        ``expect_column_kl_divergence_to_be_less_than`` is a         :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n        Args:\n            column (str):                 The column name.\n            partition_object (dict):                 The expected partition object (see :ref:`partition_object`).\n            threshold (float):                 The maximum KL divergence to for which to return `success=True`. If KL divergence is larger than the                provided threshold, the test will return `success=False`.\n\n        Keyword Args:\n            internal_weight_holdout (float between 0 and 1 or None):                 The amount of weight to split uniformly among zero-weighted partition bins. internal_weight_holdout                 provides a mechanisms to make the test less strict by assigning positive weights to values observed in                 the data for which the partition explicitly expected zero weight. With no internal_weight_holdout,                 any value observed in such a region will cause KL divergence to rise to +Infinity.                Defaults to 0.\n            tail_weight_holdout (float between 0 and 1 or None):                 The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly between                (-Infinity, min(partition_object[\'bins\'])) and (max(partition_object[\'bins\']), +Infinity).                 tail_weight_holdout provides a mechanism to make the test less strict by assigning positive weights to                 values observed in the data that are not present in the partition. With no tail_weight_holdout,                 any value observed outside the provided partition_object will cause KL divergence to rise to +Infinity.                Defaults to 0.\n            bucketize_data (boolean): If True, then continuous data will be bucketized before evaluation. Setting\n                this parameter to false allows evaluation of KL divergence with a None partition object for profiling\n                against discrete data.\n\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        Notes:\n            These fields in the result object are customized for this expectation:\n            ::\n\n                {\n                  "observed_value": (float) The true KL divergence (relative entropy) or None if the value is                   calculated as infinity, -infinity, or NaN\n                  "details": {\n                    "observed_partition": (dict) The partition observed in the data\n                    "expected_partition": (dict) The partition against which the data were compared,\n                                            after applying specified weight holdouts.\n                  }\n                }\n\n            If the partition_object is categorical, this expectation will expect the values in column to also be             categorical.\n\n                * If the column includes values that are not present in the partition, the tail_weight_holdout will be                 equally split among those values, providing a mechanism to weaken the strictness of the expectation                 (otherwise, relative entropy would immediately go to infinity).\n                * If the partition includes values that are not present in the column, the test will simply include                 zero weight for that value.\n\n            If the partition_object is continuous, this expectation will discretize the values in the column according             to the bins specified in the partition_object, and apply the test to the resulting distribution.\n\n                * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the                 expectation, since an expected weight of zero would drive relative entropy to be infinite if any data                 are observed in that interval.\n                * If internal_weight_holdout is specified, that value will be distributed equally among any intervals                 with weight zero in the partition_object.\n                * If tail_weight_holdout is specified, that value will be appended to the tails of the bins                 ((-Infinity, min(bins)) and (max(bins), Infinity).\n\n          If relative entropy/kl divergence goes to infinity for any of the reasons mentioned above, the observed value          will be set to None. This is because inf, -inf, Nan, are not json serializable and cause some json parsers to          crash when encountered. The python None token will be serialized to null in json.\n\n        See also:\n            :func:`expect_column_chisquare_test_p_value_to_be_greater_than             <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`\n\n            :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than             <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`\n\n        '
        if (partition_object is None):
            if bucketize_data:
                partition_object = build_continuous_partition_object(dataset=self, column=column)
            else:
                partition_object = build_categorical_partition_object(dataset=self, column=column)
        if (not is_valid_partition_object(partition_object)):
            raise ValueError('Invalid partition object.')
        if ((threshold is not None) and ((not isinstance(threshold, (int, float))) or (threshold < 0))):
            raise ValueError('Threshold must be specified, greater than or equal to zero.')
        if ((not isinstance(tail_weight_holdout, (int, float))) or (tail_weight_holdout < 0) or (tail_weight_holdout > 1)):
            raise ValueError('tail_weight_holdout must be between zero and one.')
        if ((not isinstance(internal_weight_holdout, (int, float))) or (internal_weight_holdout < 0) or (internal_weight_holdout > 1)):
            raise ValueError('internal_weight_holdout must be between zero and one.')
        if ((tail_weight_holdout != 0) and ('tail_weights' in partition_object)):
            raise ValueError('tail_weight_holdout must be 0 when using tail_weights in partition object')
        if is_valid_categorical_partition_object(partition_object):
            if (internal_weight_holdout > 0):
                raise ValueError('Internal weight holdout cannot be used for discrete data.')
            observed_weights = (self.get_column_value_counts(column) / self.get_column_nonnull_count(column))
            expected_weights = pd.Series(partition_object['weights'], index=partition_object['values'], name='expected')
            test_df = pd.concat([expected_weights, observed_weights], axis=1)
            na_counts = test_df.isnull().sum()
            pk = test_df['count'].fillna(0)
            if (na_counts['expected'] > 0):
                test_df['expected'] *= (1 - tail_weight_holdout)
                qk = test_df['expected'].fillna((tail_weight_holdout / na_counts['expected']))
            else:
                qk = test_df['expected']
            kl_divergence = stats.entropy(pk, qk)
            if (np.isinf(kl_divergence) or np.isnan(kl_divergence)):
                observed_value = None
            else:
                observed_value = kl_divergence
            if (threshold is None):
                success = True
            else:
                success = (kl_divergence <= threshold)
            return_obj = {'success': success, 'result': {'observed_value': observed_value, 'details': {'observed_partition': {'values': test_df.index.tolist(), 'weights': pk.tolist()}, 'expected_partition': {'values': test_df.index.tolist(), 'weights': qk.tolist()}}}}
        else:
            if (bucketize_data is False):
                raise ValueError('KL Divergence cannot be computed with a continuous partition object and the bucketize_data parameter set to false.')
            hist = np.array(self.get_column_hist(column, tuple(partition_object['bins'])))
            bin_edges = partition_object['bins']
            below_partition = self.get_column_count_in_range(column, max_val=partition_object['bins'][0])
            above_partition = self.get_column_count_in_range(column, min_val=partition_object['bins'][(- 1)], strict_min=True)
            observed_weights = (np.array(hist) / self.get_column_nonnull_count(column))
            if ('tail_weights' in partition_object):
                partition_tail_weight_holdout = np.sum(partition_object['tail_weights'])
            else:
                partition_tail_weight_holdout = 0
            expected_weights = (np.array(partition_object['weights']) * ((1 - tail_weight_holdout) - internal_weight_holdout))
            if (internal_weight_holdout > 0):
                zero_count = (len(expected_weights) - np.count_nonzero(expected_weights))
                if (zero_count > 0):
                    for (index, value) in enumerate(expected_weights):
                        if (value == 0):
                            expected_weights[index] = (internal_weight_holdout / zero_count)
            if ((partition_object['bins'][0] == (- np.inf)) and (partition_object['bins'][(- 1)] == np.inf)):
                if (tail_weight_holdout > 0):
                    raise ValueError('tail_weight_holdout cannot be used for partitions with infinite endpoints.')
                if ('tail_weights' in partition_object):
                    raise ValueError('There can be no tail weights for partitions with one or both endpoints at infinity')
                expected_bins = partition_object['bins'][1:(- 1)]
                comb_expected_weights = expected_weights
                expected_tail_weights = np.concatenate(([expected_weights[0]], [expected_weights[(- 1)]]))
                expected_weights = expected_weights[1:(- 1)]
                comb_observed_weights = observed_weights
                observed_tail_weights = np.concatenate(([observed_weights[0]], [observed_weights[(- 1)]]))
                observed_weights = observed_weights[1:(- 1)]
            elif (partition_object['bins'][0] == (- np.inf)):
                if ('tail_weights' in partition_object):
                    raise ValueError('There can be no tail weights for partitions with one or both endpoints at infinity')
                expected_bins = partition_object['bins'][1:]
                comb_expected_weights = np.concatenate((expected_weights, [tail_weight_holdout]))
                expected_tail_weights = np.concatenate(([expected_weights[0]], [tail_weight_holdout]))
                expected_weights = expected_weights[1:]
                comb_observed_weights = np.concatenate((observed_weights, [(above_partition / self.get_column_nonnull_count(column))]))
                observed_tail_weights = np.concatenate(([observed_weights[0]], [(above_partition / self.get_column_nonnull_count(column))]))
                observed_weights = observed_weights[1:]
            elif (partition_object['bins'][(- 1)] == np.inf):
                if ('tail_weights' in partition_object):
                    raise ValueError('There can be no tail weights for partitions with one or both endpoints at infinity')
                expected_bins = partition_object['bins'][:(- 1)]
                comb_expected_weights = np.concatenate(([tail_weight_holdout], expected_weights))
                expected_tail_weights = np.concatenate(([tail_weight_holdout], [expected_weights[(- 1)]]))
                expected_weights = expected_weights[:(- 1)]
                comb_observed_weights = np.concatenate(([(below_partition / self.get_column_nonnull_count(column))], observed_weights))
                observed_tail_weights = np.concatenate(([(below_partition / self.get_column_nonnull_count(column))], [observed_weights[(- 1)]]))
                observed_weights = observed_weights[:(- 1)]
            else:
                expected_bins = partition_object['bins']
                if ('tail_weights' in partition_object):
                    tail_weights = partition_object['tail_weights']
                    comb_expected_weights = np.concatenate(([tail_weights[0]], expected_weights, [tail_weights[1]]))
                    expected_tail_weights = np.array(tail_weights)
                else:
                    comb_expected_weights = np.concatenate(([(tail_weight_holdout / 2)], expected_weights, [(tail_weight_holdout / 2)]))
                    expected_tail_weights = np.concatenate(([(tail_weight_holdout / 2)], [(tail_weight_holdout / 2)]))
                comb_observed_weights = np.concatenate(([(below_partition / self.get_column_nonnull_count(column))], observed_weights, [(above_partition / self.get_column_nonnull_count(column))]))
                observed_tail_weights = (np.concatenate(([below_partition], [above_partition])) / self.get_column_nonnull_count(column))
            kl_divergence = stats.entropy(comb_observed_weights, comb_expected_weights)
            if (np.isinf(kl_divergence) or np.isnan(kl_divergence)):
                observed_value = None
            else:
                observed_value = kl_divergence
            if (threshold is None):
                success = True
            else:
                success = (kl_divergence <= threshold)
            return_obj = {'success': success, 'result': {'observed_value': observed_value, 'details': {'observed_partition': {'bins': expected_bins, 'weights': observed_weights.tolist(), 'tail_weights': observed_tail_weights.tolist()}, 'expected_partition': {'bins': expected_bins, 'weights': expected_weights.tolist(), 'tail_weights': expected_tail_weights.tolist()}}}}
        return return_obj

    @MetaDataset.column_aggregate_expectation
    def expect_column_pair_cramers_phi_value_to_be_less_than(self, column_A, column_B, bins_A=None, bins_B=None, n_bins_A=None, n_bins_B=None, threshold=0.1, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect the values in column_A to be independent of those in column_B.\n\n        Args:\n            column_A (str): The first column name\n            column_B (str): The second column name\n            threshold (float): Maximum allowed value of cramers V for expectation to pass.\n\n        Keyword Args:\n            bins_A (list of float): Bins for column_A.\n            bins_B (list of float): Bins for column_B.\n            n_bins_A (int): Number of bins for column_A. Ignored if bins_A is not None.\n            n_bins_B (int): Number of bins for column_B. Ignored if bins_B is not None.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        crosstab = self.get_crosstab(column_A, column_B, bins_A, bins_B, n_bins_A, n_bins_B)
        chi2_result = stats.chi2_contingency(crosstab)
        cramers_V = max(min(np.sqrt(((chi2_result[0] / self.get_row_count()) / (min(crosstab.shape) - 1))), 1), 0)
        return_obj = {'success': (cramers_V <= threshold), 'result': {'observed_value': cramers_V, 'unexpected_list': crosstab, 'details': {'crosstab': crosstab}}}
        return return_obj

    def expect_column_pair_values_to_be_equal(self, column_A, column_B, ignore_row_if='both_values_are_missing', result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect the values in column A to be the same as column B.\n\n        Args:\n            column_A (str): The first column name\n            column_B (str): The second column name\n\n        Keyword Args:\n            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_column_pair_values_A_to_be_greater_than_B(self, column_A, column_B, or_equal=None, parse_strings_as_datetimes=False, allow_cross_type_comparisons=None, ignore_row_if='both_values_are_missing', result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect values in column A to be greater than column B.\n\n        Args:\n            column_A (str): The first column name\n            column_B (str): The second column name\n            or_equal (boolean or None): If True, then values can be equal, not strictly greater\n\n        Keyword Args:\n            allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and                string). Otherwise, attempting such comparisons will raise an exception.\n\n        Keyword Args:\n            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_column_pair_values_to_be_in_set(self, column_A, column_B, value_pairs_set, ignore_row_if='both_values_are_missing', result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect paired values from columns A and B to belong to a set of valid pairs.\n\n        Args:\n            column_A (str): The first column name\n            column_B (str): The second column name\n            value_pairs_set (list of tuples): All the valid pairs to be matched\n\n        Keyword Args:\n            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_multicolumn_values_to_be_unique(self, column_list, ignore_row_if='all_values_are_missing', result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        NOTE: This method is deprecated. Please use expect_select_column_values_to_be_unique_within_record instead\n        Expect the values for each record to be unique across the columns listed.\n        Note that records can be duplicated.\n\n        For example::\n\n            A B C\n            1 1 2 Fail\n            1 2 3 Pass\n            8 2 7 Pass\n            1 2 3 Pass\n            4 4 4 Fail\n\n        Args:\n            column_list (tuple or list): The column names to evaluate\n\n        Keyword Args:\n            ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_select_column_values_to_be_unique_within_record(self, column_list, ignore_row_if='all_values_are_missing', result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect the values for each record to be unique across the columns listed.\n        Note that records can be duplicated.\n\n        For example::\n\n            A B C\n            1 1 2 Fail\n            1 2 3 Pass\n            8 2 7 Pass\n            1 2 3 Pass\n            4 4 4 Fail\n\n        Args:\n            column_list (tuple or list): The column names to evaluate\n\n        Keyword Args:\n            ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        raise NotImplementedError

    def expect_compound_columns_to_be_unique(self, column_list, ignore_row_if='all_values_are_missing', result_format=None, row_condition=None, condition_parser=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect that the columns are unique together, e.g. a multi-column primary key\n        Note that all instances of any duplicates are considered failed\n\n        For example::\n\n            A B C\n            1 1 2 Fail\n            1 2 3 Pass\n            1 1 2 Fail\n            2 2 2 Pass\n            3 2 3 Pass\n\n        Args:\n            column_list (tuple or list): The column names to evaluate\n\n        Keyword Args:\n            ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n            An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n        '
        raise NotImplementedError

    def expect_multicolumn_sum_to_equal(self, column_list, sum_total, result_format=None, include_config=True, catch_exceptions=None, meta=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        ' Multi-Column Map Expectation\n\n        Expects that the sum of row values is the same for each row, summing only values in columns specified in\n        column_list, and equal to the specific value, sum_total.\n\n        For example (with column_list=["B", "C"] and sum_total=5)::\n\n            A B C\n            1 3 2\n            1 5 0\n            1 1 4\n\n            Pass\n\n            A B C\n            1 3 2\n            1 5 1\n            1 1 4\n\n            Fail on row 2\n\n        Args:\n            column_list (List[str]):                 Set of columns to be checked\n            sum_total (int):                 expected sum of columns\n        '
        raise NotImplementedError

    @staticmethod
    def _parse_value_set(value_set):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        parsed_value_set = [(parse(value) if isinstance(value, str) else value) for value in value_set]
        return parsed_value_set

    def attempt_allowing_relative_error(self) -> Union[(bool, float)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Subclasses can override this method if the respective data source (e.g., Redshift) supports "approximate" mode.\n        In certain cases (e.g., for SparkDFDataset), a fraction between 0 and 1 (i.e., not only a boolean) is allowed.\n        '
        return False
