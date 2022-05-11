
import logging
import warnings
from typing import Any, List
import numpy as np
import pandas as pd
from scipy import stats
logger = logging.getLogger(__name__)
try:
    import sqlalchemy
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql.elements import WithinGroup
except ImportError:
    logger.debug('Unable to load SqlAlchemy or one of its subclasses.')
    DefaultDialect = None
    WithinGroup = None

def is_valid_partition_object(partition_object):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Tests whether a given object is a valid continuous or categorical partition object.\n    :param partition_object: The partition_object to evaluate\n    :return: Boolean\n    '
    return (is_valid_continuous_partition_object(partition_object) or is_valid_categorical_partition_object(partition_object))

def is_valid_categorical_partition_object(partition_object):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Tests whether a given object is a valid categorical partition object.\n    :param partition_object: The partition_object to evaluate\n    :return: Boolean\n    '
    if ((partition_object is None) or ('weights' not in partition_object) or ('values' not in partition_object)):
        return False
    return ((len(partition_object['values']) == len(partition_object['weights'])) and np.allclose(np.sum(partition_object['weights']), 1))

def is_valid_continuous_partition_object(partition_object):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Tests whether a given object is a valid continuous partition object. See :ref:`partition_object`.\n\n    :param partition_object: The partition_object to evaluate\n    :return: Boolean\n    '
    if ((partition_object is None) or ('weights' not in partition_object) or ('bins' not in partition_object)):
        return False
    if ('tail_weights' in partition_object):
        if (len(partition_object['tail_weights']) != 2):
            return False
        comb_weights = (partition_object['tail_weights'] + partition_object['weights'])
    else:
        comb_weights = partition_object['weights']
    return ((len(partition_object['bins']) == (len(partition_object['weights']) + 1)) and np.all((np.diff(partition_object['bins']) > 0)) and np.allclose(np.sum(comb_weights), 1.0))

def categorical_partition_data(data):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for creating weights from categorical data.\n\n    Args:\n        data (list-like): The data from which to construct the estimate.\n\n    Returns:\n        A new partition object::\n\n            {\n                "values": (list) The categorical values present in the data\n                "weights": (list) The weights of the values in the partition.\n            }\n\n        See :ref:`partition_object`.\n    '
    series = pd.Series(data)
    value_counts = series.value_counts(dropna=True)
    null_indexes = series.isnull()
    nonnull_count = (null_indexes == False).sum()
    weights = (value_counts.values / nonnull_count)
    return {'values': value_counts.index.tolist(), 'weights': weights}

def kde_partition_data(data, estimate_tails=True):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for building a partition and weights using a gaussian Kernel Density Estimate and default bandwidth.\n\n    Args:\n        data (list-like): The data from which to construct the estimate\n        estimate_tails (bool): Whether to estimate the tails of the distribution to keep the partition object finite\n\n    Returns:\n        A new partition_object::\n\n        {\n            "bins": (list) The endpoints of the partial partition of reals,\n            "weights": (list) The densities of the bins implied by the partition.\n        }\n\n        See :ref:`partition_object`.\n    '
    kde = stats.kde.gaussian_kde(data)
    evaluation_bins = np.linspace(start=(np.min(data) - (kde.covariance_factor() / 2)), stop=(np.max(data) + (kde.covariance_factor() / 2)), num=np.floor((((np.max(data) - np.min(data)) / kde.covariance_factor()) + 1)).astype(int))
    cdf_vals = [kde.integrate_box_1d((- np.inf), x) for x in evaluation_bins]
    evaluation_weights = np.diff(cdf_vals)
    if estimate_tails:
        bins = np.concatenate(([(np.min(data) - (1.5 * kde.covariance_factor()))], evaluation_bins, [(np.max(data) + (1.5 * kde.covariance_factor()))]))
    else:
        bins = np.concatenate(([(- np.inf)], evaluation_bins, [np.inf]))
    weights = np.concatenate(([cdf_vals[0]], evaluation_weights, [(1 - cdf_vals[(- 1)])]))
    return {'bins': bins, 'weights': weights}

def partition_data(data, bins='auto', n_bins=10):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    warnings.warn('partition_data is deprecated as of v0.10.10 and will be removed is a future release. Use either continuous_partition_data or                     categorical_partition_data instead.', DeprecationWarning)
    return continuous_partition_data(data, bins, n_bins)

def continuous_partition_data(data, bins='auto', n_bins=10, **kwargs):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for building a partition object on continuous data\n\n    Args:\n        data (list-like): The data from which to construct the estimate.\n        bins (string): One of \'uniform\' (for uniformly spaced bins), \'ntile\' (for percentile-spaced bins), or \'auto\'\n            (for automatically spaced bins)\n        n_bins (int): Ignored if bins is auto.\n        kwargs (mapping): Additional keyword arguments to be passed to numpy histogram\n\n    Returns:\n        A new partition_object::\n\n        {\n            "bins": (list) The endpoints of the partial partition of reals,\n            "weights": (list) The densities of the bins implied by the partition.\n        }\n        See :ref:`partition_object`.\n    '
    if (bins == 'uniform'):
        bins = np.linspace(start=np.min(data), stop=np.max(data), num=(n_bins + 1))
    elif (bins == 'ntile'):
        bins = np.percentile(data, np.linspace(start=0, stop=100, num=(n_bins + 1)))
    elif (bins != 'auto'):
        raise ValueError('Invalid parameter for bins argument')
    try:
        (hist, bin_edges) = np.histogram(data, bins, density=False, **kwargs)
    except ValueError as e:
        raise ValueError(('Unable to compute histogram. Did you know you can pass additional kwargs to numpy histogram,such as a range? Numpy error was: ' + str(e)))
    except TypeError as e:
        raise TypeError(f'Unable to compute histogram. numpy histogram raised error: {str(e)}')
    return {'bins': bin_edges, 'weights': (hist / len(data))}

def build_continuous_partition_object(dataset, column, bins='auto', n_bins=10, allow_relative_error=False):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for building a partition object on continuous data from a dataset and column\n\n    Args:\n        dataset (GE Dataset): the dataset for which to compute the partition\n        column (string): The name of the column for which to construct the estimate.\n        bins (string): One of \'uniform\' (for uniformly spaced bins), \'ntile\' (for percentile-spaced bins), or \'auto\'\n            (for automatically spaced bins)\n        n_bins (int): Ignored if bins is auto.\n        allow_relative_error: passed to get_column_quantiles, set to False for only precise\n            values, True to allow approximate values on systems with only binary choice (e.g. Redshift), and to a\n            value between zero and one for systems that allow specification of relative error (e.g.\n            SparkDFDataset).\n\n    Returns:\n\n        A new partition_object::\n\n            {\n                "bins": (list) The endpoints of the partial partition of reals,\n                "weights": (list) The densities of the bins implied by the partition.\n            }\n\n            See :ref:`partition_object`.\n    '
    bins = dataset.get_column_partition(column, bins, n_bins, allow_relative_error)
    if isinstance(bins, np.ndarray):
        bins = bins.tolist()
    else:
        bins = list(bins)
    weights = list((np.array(dataset.get_column_hist(column, tuple(bins))) / dataset.get_column_nonnull_count(column)))
    tail_weights = ((1 - sum(weights)) / 2)
    partition_object = {'bins': bins, 'weights': weights, 'tail_weights': [tail_weights, tail_weights]}
    return partition_object

def build_categorical_partition_object(dataset, column, sort='value'):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for building a partition object on categorical data from a dataset and column\n\n    Args:\n        dataset (GE Dataset): the dataset for which to compute the partition\n        column (string): The name of the column for which to construct the estimate.\n        sort (string): must be one of "value", "count", or "none".\n            - if "value" then values in the resulting partition object will be sorted lexigraphically\n            - if "count" then values will be sorted according to descending count (frequency)\n            - if "none" then values will not be sorted\n\n    Returns:\n        A new partition_object::\n\n        {\n            "values": (list) the categorical values for which each weight applies,\n            "weights": (list) The densities of the values implied by the partition.\n        }\n        See :ref:`partition_object`.\n    '
    counts = dataset.get_column_value_counts(column, sort)
    return {'values': list(counts.index), 'weights': list((np.array(counts) / dataset.get_column_nonnull_count(column)))}

def infer_distribution_parameters(data, distribution, params=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Convenience method for determining the shape parameters of a given distribution\n\n    Args:\n        data (list-like): The data to build shape parameters from.\n        distribution (string): Scipy distribution, determines which parameters to build.\n        params (dict or None): The known parameters. Parameters given here will not be altered.                                Keep as None to infer all necessary parameters from the data data.\n\n    Returns:\n        A dictionary of named parameters::\n\n        {\n            "mean": (float),\n            "std_dev": (float),\n            "loc": (float),\n            "scale": (float),\n            "alpha": (float),\n            "beta": (float),\n            "min": (float),\n            "max": (float),\n            "df": (float)\n        }\n\n        See: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kstest.html#scipy.stats.kstest\n    '
    if (params is None):
        params = {}
    elif (not isinstance(params, dict)):
        raise TypeError('params must be a dictionary object, see great_expectations documentation')
    if ('mean' not in params.keys()):
        params['mean'] = data.mean()
    if ('std_dev' not in params.keys()):
        params['std_dev'] = data.std()
    if (distribution == 'beta'):
        if ('alpha' not in params.keys()):
            params['alpha'] = ((params['mean'] ** 2) * (((1 - params['mean']) / (params['std_dev'] ** 2)) - (1 / params['mean'])))
        if ('beta' not in params.keys()):
            params['beta'] = (params['alpha'] * ((1 / params['mean']) - 1))
    elif (distribution == 'gamma'):
        if ('alpha' not in params.keys()):
            params['alpha'] = (params['mean'] / params.get('scale', 1))
    elif (distribution == 'uniform'):
        if ('min' not in params.keys()):
            if ('loc' in params.keys()):
                params['min'] = params['loc']
            else:
                params['min'] = min(data)
        if ('max' not in params.keys()):
            if ('scale' in params.keys()):
                params['max'] = params['scale']
            else:
                params['max'] = (max(data) - params['min'])
    elif (distribution == 'chi2'):
        if ('df' not in params.keys()):
            params['df'] = params['mean']
    elif (distribution != 'norm'):
        raise AttributeError('Unsupported distribution type. Please refer to Great Expectations Documentation')
    params['loc'] = params.get('loc', 0)
    params['scale'] = params.get('scale', 1)
    return params

def _scipy_distribution_positional_args_from_dict(distribution, params):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Helper function that returns positional arguments for a scipy distribution using a dict of parameters.\n\n       See the `cdf()` function here https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.beta.html#Methods       to see an example of scipy's positional arguments. This function returns the arguments specified by the        scipy.stat.distribution.cdf() for that distribution.\n\n       Args:\n           distribution (string):                The scipy distribution name.\n           params (dict):                A dict of named parameters.\n\n       Raises:\n           AttributeError:                If an unsupported distribution is provided.\n    "
    params['loc'] = params.get('loc', 0)
    if ('scale' not in params):
        params['scale'] = 1
    if (distribution == 'norm'):
        return (params['mean'], params['std_dev'])
    elif (distribution == 'beta'):
        return (params['alpha'], params['beta'], params['loc'], params['scale'])
    elif (distribution == 'gamma'):
        return (params['alpha'], params['loc'], params['scale'])
    elif (distribution == 'uniform'):
        return (params['min'], params['max'])
    elif (distribution == 'chi2'):
        return (params['df'], params['loc'], params['scale'])
    elif (distribution == 'expon'):
        return (params['loc'], params['scale'])

def validate_distribution_parameters(distribution, params):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "Ensures that necessary parameters for a distribution are present and that all parameters are sensical.\n\n       If parameters necessary to construct a distribution are missing or invalid, this function raises ValueError       with an informative description. Note that 'loc' and 'scale' are optional arguments, and that 'scale'       must be positive.\n\n       Args:\n           distribution (string):                The scipy distribution name, e.g. normal distribution is 'norm'.\n           params (dict or list):                The distribution shape parameters in a named dictionary or positional list form following the scipy                cdf argument scheme.\n\n               params={'mean': 40, 'std_dev': 5} or params=[40, 5]\n\n       Exceptions:\n           ValueError:                With an informative description, usually when necessary parameters are omitted or are invalid.\n\n    "
    norm_msg = "norm distributions require 0 parameters and optionally 'mean', 'std_dev'."
    beta_msg = "beta distributions require 2 positive parameters 'alpha', 'beta' and optionally 'loc', 'scale'."
    gamma_msg = "gamma distributions require 1 positive parameter 'alpha' and optionally 'loc','scale'."
    uniform_msg = "uniform distributions require 0 parameters and optionally 'loc', 'scale'."
    chi2_msg = "chi2 distributions require 1 positive parameter 'df' and optionally 'loc', 'scale'."
    expon_msg = "expon distributions require 0 parameters and optionally 'loc', 'scale'."
    if (distribution not in ['norm', 'beta', 'gamma', 'poisson', 'uniform', 'chi2', 'expon']):
        raise AttributeError(f'Unsupported  distribution provided: {distribution}')
    if isinstance(params, dict):
        if ((params.get('std_dev', 1) <= 0) or (params.get('scale', 1) <= 0)):
            raise ValueError('std_dev and scale must be positive.')
        if ((distribution == 'beta') and ((params.get('alpha', (- 1)) <= 0) or (params.get('beta', (- 1)) <= 0))):
            raise ValueError(f'Invalid parameters: {beta_msg}')
        elif ((distribution == 'gamma') and (params.get('alpha', (- 1)) <= 0)):
            raise ValueError(f'Invalid parameters: {gamma_msg}')
        elif ((distribution == 'chi2') and (params.get('df', (- 1)) <= 0)):
            raise ValueError(f'Invalid parameters: {chi2_msg}:')
    elif (isinstance(params, tuple) or isinstance(params, list)):
        scale = None
        if (distribution == 'beta'):
            if (len(params) < 2):
                raise ValueError(f'Missing required parameters: {beta_msg}')
            if ((params[0] <= 0) or (params[1] <= 0)):
                raise ValueError(f'Invalid parameters: {beta_msg}')
            if (len(params) == 4):
                scale = params[3]
            elif (len(params) > 4):
                raise ValueError(f'Too many parameters provided: {beta_msg}')
        elif (distribution == 'norm'):
            if (len(params) > 2):
                raise ValueError(f'Too many parameters provided: {norm_msg}')
            if (len(params) == 2):
                scale = params[1]
        elif (distribution == 'gamma'):
            if (len(params) < 1):
                raise ValueError(f'Missing required parameters: {gamma_msg}')
            if (len(params) == 3):
                scale = params[2]
            if (len(params) > 3):
                raise ValueError(f'Too many parameters provided: {gamma_msg}')
            elif (params[0] <= 0):
                raise ValueError(f'Invalid parameters: {gamma_msg}')
        elif (distribution == 'uniform'):
            if (len(params) == 2):
                scale = params[1]
            if (len(params) > 2):
                raise ValueError(f'Too many arguments provided: {uniform_msg}')
        elif (distribution == 'chi2'):
            if (len(params) < 1):
                raise ValueError(f'Missing required parameters: {chi2_msg}')
            elif (len(params) == 3):
                scale = params[2]
            elif (len(params) > 3):
                raise ValueError(f'Too many arguments provided: {chi2_msg}')
            if (params[0] <= 0):
                raise ValueError(f'Invalid parameters: {chi2_msg}')
        elif (distribution == 'expon'):
            if (len(params) == 2):
                scale = params[1]
            if (len(params) > 2):
                raise ValueError(f'Too many arguments provided: {expon_msg}')
        if ((scale is not None) and (scale <= 0)):
            raise ValueError('std_dev and scale must be positive.')
    else:
        raise ValueError('params must be a dict or list, or use ge.dataset.util.infer_distribution_parameters(data, distribution)')
    return

def create_multiple_expectations(df, columns, expectation_type, *args, **kwargs):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Creates an identical expectation for each of the given columns with the specified arguments, if any.\n\n    Args:\n        df (great_expectations.dataset): A great expectations dataset object.\n        columns (list): A list of column names represented as strings.\n        expectation_type (string): The expectation type.\n\n    Raises:\n        KeyError if the provided column does not exist.\n        AttributeError if the provided expectation type does not exist or df is not a valid great expectations dataset.\n\n    Returns:\n        A list of expectation results.\n\n\n    '
    expectation = getattr(df, expectation_type)
    results = list()
    for column in columns:
        results.append(expectation(column, *args, **kwargs))
    return results

def get_approximate_percentile_disc_sql(selects: List, sql_engine_dialect: Any) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return ', '.join([('approximate ' + str(stmt.compile(dialect=sql_engine_dialect, compile_kwargs={'literal_binds': True}))) for stmt in selects])

def check_sql_engine_dialect(actual_sql_engine_dialect: Any, candidate_sql_engine_dialect: Any) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    try:
        return isinstance(actual_sql_engine_dialect, candidate_sql_engine_dialect)
    except (AttributeError, TypeError):
        return False
