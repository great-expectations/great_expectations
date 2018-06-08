# Utility methods for dealing with Dataset objects

from __future__ import division

import decimal

from six import string_types, integer_types

import numpy as np
from scipy import stats
import pandas as pd
import numpy as np
import warnings
import sys
import copy
import datetime

from functools import wraps


def parse_result_format(result_format):
    """This is a simple helper utility that can be used to parse a string result_format into the dict format used
    internally by great_expectations. It is not necessary but allows shorthand for result_format in cases where
    there is no need to specify a custom partial_unexpected_count."""
    if isinstance(result_format, string_types):
        result_format = {
            'result_format': result_format,
            'partial_unexpected_count': 20
        }
    else:
        if 'partial_unexpected_count' not in result_format:
            result_format['partial_unexpected_count'] = 20

    return result_format

class DotDict(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, attr):
        return self.get(attr)

    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__

    def __dir__(self):
        return self.keys()

    #Cargo-cultishly copied from: https://github.com/spindlelabs/pyes/commit/d2076b385c38d6d00cebfe0df7b0d1ba8df934bc
    def __deepcopy__(self, memo):
        return DotDict([(copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items()])


"""Docstring inheriting descriptor. Note that this is not a docstring so that this is not added to @DocInherit-\
decorated functions' hybrid docstrings.

Usage::

    class Foo(object):
        def foo(self):
            "Frobber"
            pass

    class Bar(Foo):
        @DocInherit
        def foo(self):
            pass

    Now, Bar.foo.__doc__ == Bar().foo.__doc__ == Foo.foo.__doc__ == "Frobber"

    Original implementation cribbed from:
    https://stackoverflow.com/questions/2025562/inherit-docstrings-in-python-class-inheritance,
    following a discussion on comp.lang.python that resulted in:
    http://code.activestate.com/recipes/576862/. Unfortunately, the
    original authors did not anticipate deep inheritance hierarchies, and
    we ran into a recursion issue when implementing custom subclasses of
    PandasDataset:
    https://github.com/great-expectations/great_expectations/issues/177.

    Our new homegrown implementation directly searches the MRO, instead
    of relying on super, and concatenates documentation together.
"""
class DocInherit(object):

    def __init__(self, mthd):
        self.mthd = mthd
        self.name = mthd.__name__
        self.mthd_doc = mthd.__doc__

    def __get__(self, obj, cls):
        doc = self.mthd_doc if self.mthd_doc is not None else ''

        for parent in cls.mro():
            if self.name not in parent.__dict__:
                continue
            if parent.__dict__[self.name].__doc__ is not None:
                doc = doc + '\n' + parent.__dict__[self.name].__doc__

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            return self.mthd(obj, *args, **kwargs)

        f.__doc__ = doc
        return f


def recursively_convert_to_json_serializable(test_obj):
    """
    Helper function to convert a dict object to one that is serializable

    Args:
        test_obj: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    # Validate that all aruguments are of approved types, coerce if it's easy, else exception
    # print(type(test_obj), test_obj)
    #Note: Not 100% sure I've resolved this correctly...
    try:
        if not isinstance(test_obj, list) and np.isnan(test_obj):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(test_obj, (string_types, integer_types, float, bool)):
        # No problem to encode json
        return test_obj

    elif isinstance(test_obj, dict):
        new_dict = {}
        for key in test_obj:
            new_dict[key] = recursively_convert_to_json_serializable(test_obj[key])

        return new_dict

    elif isinstance(test_obj, (list, tuple, set)):
        new_list = []
        for val in test_obj:
            new_list.append(recursively_convert_to_json_serializable(val))

        return new_list

    elif isinstance(test_obj, (np.ndarray, pd.Index)):
        #test_obj[key] = test_obj[key].tolist()
        ## If we have an array or index, convert it first to a list--causing coercion to float--and then round
        ## to the number of digits for which the string representation will equal the float representation
        return [recursively_convert_to_json_serializable(x) for x in test_obj.tolist()]

    #Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif test_obj == None:
        # No problem to encode json
        return test_obj

    elif isinstance(test_obj, (datetime.datetime, datetime.date)):
        return str(test_obj)

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(test_obj), np.bool_):
        return bool(test_obj)

    elif np.issubdtype(type(test_obj), np.integer) or np.issubdtype(type(test_obj), np.uint):
        return int(test_obj)

    elif np.issubdtype(type(test_obj), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(test_obj, sys.float_info.dig))

    # elif np.issubdtype(type(test_obj), np.complexfloating):
        # Note: Use np.complexfloating to avoid Future Warning from numpy
        # Complex numbers consist of two floating point numbers
        # return complex(
        #     float(round(test_obj.real, sys.float_info.dig)),
        #     float(round(test_obj.imag, sys.float_info.dig)))

    elif isinstance(test_obj, decimal.Decimal):
        return float(test_obj)

    else:
        raise TypeError('%s is of type %s which cannot be serialized.' % (str(test_obj), type(test_obj).__name__))


def is_valid_partition_object(partition_object):
    """Tests whether a given object is a valid continuous or categorical partition object.
    :param partition_object: The partition_object to evaluate
    :return: Boolean
    """
    if is_valid_continuous_partition_object(partition_object) or is_valid_categorical_partition_object(partition_object):
        return True
    return False


def is_valid_categorical_partition_object(partition_object):
    """Tests whether a given object is a valid categorical partition object.
    :param partition_object: The partition_object to evaluate
    :return: Boolean
    """
    if partition_object is None or ("weights" not in partition_object) or ("values" not in partition_object):
        return False
    # Expect the same number of values as weights; weights should sum to one
    if len(partition_object['values']) == len(partition_object['weights']) and \
            np.allclose(np.sum(partition_object['weights']), 1):
        return True
    return False


def is_valid_continuous_partition_object(partition_object):
    """Tests whether a given object is a valid continuous partition object.
    :param partition_object: The partition_object to evaluate
    :return: Boolean
    """
    if (partition_object is None) or ("weights" not in partition_object) or ("bins" not in partition_object):
        return False
    # Expect one more bin edge than weight; all bin edges should be monotonically increasing; weights should sum to one
    if (len(partition_object['bins']) == (len(partition_object['weights']) + 1)) and \
            np.all(np.diff(partition_object['bins']) > 0) and \
            np.allclose(np.sum(partition_object['weights']), 1):
        return True
    return False


def categorical_partition_data(data):
    """Convenience method for creating weights from categorical data.

    Args:
        data (list-like): The data from which to construct the estimate.

    Returns:
        A new partition object::

            {
                "partition": (list) The categorical values present in the data
                "weights": (list) The weights of the values in the partition.
            }
    """

    # Make dropna explicit (even though it defaults to true)
    series = pd.Series(data)
    value_counts = series.value_counts(dropna=True)

    # Compute weights using denominator only of nonnull values
    null_indexes = series.isnull()
    nonnull_count = (null_indexes == False).sum()

    weights = value_counts.values / nonnull_count
    return {
        "values": value_counts.index.tolist(),
        "weights": weights
    }


def kde_partition_data(data, estimate_tails=True):
    """Convenience method for building a partition and weights using a gaussian Kernel Density Estimate and default bandwidth.

    Args:
        data (list-like): The data from which to construct the estimate
        estimate_tails (bool): Whether to estimate the tails of the distribution to keep the partition object finite

    Returns:
        A new partition_object::

        {
            "partition": (list) The endpoints of the partial partition of reals,
            "weights": (list) The densities of the bins implied by the partition.
        }
    """
    kde = stats.kde.gaussian_kde(data)
    evaluation_bins = np.linspace(start=np.min(data) - (kde.covariance_factor() / 2),
                                  stop=np.max(data) + (kde.covariance_factor() / 2),
                                  num=np.floor(((np.max(data) - np.min(data)) / kde.covariance_factor()) + 1 ).astype(int))
    cdf_vals = [kde.integrate_box_1d(-np.inf, x) for x in evaluation_bins]
    evaluation_weights = np.diff(cdf_vals)

    if estimate_tails:
        bins = np.concatenate(([np.min(data) - (1.5 * kde.covariance_factor())],
                               evaluation_bins,
                               [np.max(data) + (1.5 * kde.covariance_factor())]))
    else:
        bins = np.concatenate(([-np.inf], evaluation_bins, [np.inf]))

    weights = np.concatenate(([cdf_vals[0]], evaluation_weights, [1 - cdf_vals[-1]]))

    return {
        "bins": bins,
        "weights": weights
    }


def partition_data(data, bins='auto', n_bins=10):
    warnings.warn("partition_data is deprecated and will be removed. Use either continuous_partition_data or \
                    categorical_partition_data instead.", DeprecationWarning)
    return continuous_partition_data(data, bins, n_bins)


def continuous_partition_data(data, bins='auto', n_bins=10):
    """Convenience method for building a partition object on continuous data

    Args:
        data (list-like): The data from which to construct the estimate.
        bins (string): One of 'uniform' (for uniformly spaced bins), 'ntile' (for percentile-spaced bins), or 'auto' (for automatically spaced bins)
        n_bins (int): Ignored if bins is auto.

    Returns:
        A new partition_object::

        {
            "bins": (list) The endpoints of the partial partition of reals,
            "weights": (list) The densities of the bins implied by the partition.
        }
    """
    if bins == 'uniform':
        bins = np.linspace(start=np.min(data), stop=np.max(data), num = n_bins+1)
    elif bins =='ntile':
        bins = np.percentile(data, np.linspace(start=0, stop=100, num = n_bins+1))
    elif bins != 'auto':
        raise ValueError("Invalid parameter for bins argument")

    hist, bin_edges = np.histogram(data, bins, density=False)

    return {
        "bins": bin_edges,
        "weights": hist / len(data)
    }


def infer_distribution_parameters(data, distribution, params=None):
    """Convenience method for determining the shape parameters of a given distribution

    Args:
        data (list-like): The data to build shape parameters from.
        distribution (string): Scipy distribution, determines which parameters to build.
        params (dict or None): The known parameters. Parameters given here will not be altered. \
                               Keep as None to infer all necessary parameters from the data data.

    Returns:
        A dictionary of named parameters::

        {
            "mean": (float),
            "std_dev": (float),
            "loc": (float),
            "scale": (float),
            "alpha": (float),
            "beta": (float),
            "min": (float),
            "max": (float),
            "df": (float)
        }

        See: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kstest.html#scipy.stats.kstest
    """

    if params is None:
        params = dict()
    elif not isinstance(params, dict):
        raise TypeError("params must be a dictionary object, see great_expectations documentation")

    if 'mean' not in params.keys():
        params['mean'] = data.mean()

    if 'std_dev' not in params.keys():
        params['std_dev'] = data.std()

    if distribution == "beta":
        # scipy cdf(x, a, b, loc=0, scale=1)
        if 'alpha' not in params.keys():
            # from https://stats.stackexchange.com/questions/12232/calculating-the-parameters-of-a-beta-distribution-using-the-mean-and-variance
            params['alpha'] = (params['mean'] ** 2) * (
                        ((1 - params['mean']) / params['std_dev'] ** 2) - (1 / params['mean']))
        if 'beta' not in params.keys():
            params['beta'] = params['alpha'] * ((1 / params['mean']) - 1)

    elif distribution == 'gamma':
        # scipy cdf(x, a, loc=0, scale=1)
        if 'alpha' not in params.keys():
            # Using https://en.wikipedia.org/wiki/Gamma_distribution
            params['alpha'] = (params['mean'] / params.get('scale', 1))


    #elif distribution == 'poisson':
    #    if 'lambda' not in params.keys():
    #       params['lambda'] = params['mean']

    elif distribution == 'uniform':
        # scipy cdf(x, loc=0, scale=1)
        if 'min' not in params.keys():
            if 'loc' in params.keys():
                params['min'] = params['loc']
            else:
                params['min'] = min(data)
        if 'max' not in params.keys():
            if 'scale' in params.keys():
                params['max'] = params['scale']
            else:
                params['max'] = max(data) - params['min']

    elif distribution == 'chi2':
        # scipy cdf(x, df, loc=0, scale=1)
        if 'df' not in params.keys():
            # from https://en.wikipedia.org/wiki/Chi-squared_distribution
            params['df'] = params['mean']

    #  Expon only uses loc and scale, use default
    #elif distribution == 'expon':
        # scipy cdf(x, loc=0, scale=1)
    #    if 'lambda' in params.keys():
            # Lambda is optional
    #        params['scale'] = 1 / params['lambda']
    elif distribution is not 'norm':
        raise AttributeError("Unsupported distribution type. Please refer to Great Expectations Documentation")

    params['loc'] = params.get('loc', 0)
    params['scale'] = params.get('scale', 1)

    return params

def _scipy_distribution_positional_args_from_dict(distribution, params):
    """Helper function that returns positional arguments for a scipy distribution using a dict of parameters.

       See the `cdf()` function here https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.beta.html#Methods\
       to see an example of scipy's positional arguments. This function returns the arguments specified by the \
       scipy.stat.distribution.cdf() for tha distribution.

       Args:
           distribution (string): \
               The scipy distribution name.
           params (dict): \
               A dict of named parameters.

       Raises:
           AttributeError: \
               If an unsupported distribution is provided.
    """

    params['loc'] = params.get('loc', 0)
    if 'scale' not in params:
        params['scale'] = 1

    if distribution == 'norm':
        return params['mean'], params['std_dev']
    elif distribution == 'beta':
        return params['alpha'], params['beta'], params['loc'], params['scale']
    elif distribution == 'gamma':
        return params['alpha'], params['loc'], params['scale']
    #elif distribution == 'poisson':
    #    return params['lambda'], params['loc']
    elif distribution == 'uniform':
        return params['min'], params['max']
    elif distribution == 'chi2':
        return params['df'], params['loc'], params['scale']
    elif distribution == 'expon':
        return params['loc'], params['scale']


def validate_distribution_parameters(distribution, params):
    """Ensures that necessary parameters for a distribution are present and that all parameters are sensical.

       If parameters necessary to construct a distribution are missing or invalid, this function raises ValueError\
       with an informative description. Note that 'loc' and 'scale' are optional arguments, and that 'scale'\
       must be positive.

       Args:
           distribution (string): \
               The scipy distribution name, e.g. normal distribution is 'norm'.
           params (dict or list): \
               The distribution shape parameters in a named dictionary or positional list form following the scipy \
               cdf argument scheme.

               params={'mean': 40, 'std_dev': 5} or params=[40, 5]

       Exceptions:
           ValueError: \
               With an informative description, usually when necessary parameters are omitted or are invalid.

    """

    norm_msg = "norm distributions require 0 parameters and optionally 'mean', 'std_dev'."
    beta_msg = "beta distributions require 2 positive parameters 'alpha', 'beta' and optionally 'loc', 'scale'."
    gamma_msg = "gamma distributions require 1 positive parameter 'alpha' and optionally 'loc','scale'."
    # poisson_msg = "poisson distributions require 1 positive parameter 'lambda' and optionally 'loc'."
    uniform_msg = "uniform distributions require 0 parameters and optionally 'loc', 'scale'."
    chi2_msg = "chi2 distributions require 1 positive parameter 'df' and optionally 'loc', 'scale'."
    expon_msg = "expon distributions require 0 parameters and optionally 'loc', 'scale'."

    if (distribution not in ['norm', 'beta', 'gamma', 'poisson', 'uniform', 'chi2', 'expon']):
        raise AttributeError("Unsupported  distribution provided: %s" % distribution)

    if isinstance(params, dict):
        # `params` is a dictionary
        if params.get("std_dev", 1) <= 0 or params.get('scale', 1) <= 0:
            raise ValueError("std_dev and scale must be positive.")

        # alpha and beta are required and positive
        if distribution == 'beta' and (params.get('alpha', -1) <= 0 or params.get('beta', -1) <= 0):
            raise ValueError("Invalid parameters: %s" %beta_msg)

        # alpha is required and positive
        elif distribution == 'gamma' and params.get('alpha', -1) <= 0:
            raise ValueError("Invalid parameters: %s" %gamma_msg)

        # lambda is a required and positive
        #elif distribution == 'poisson' and params.get('lambda', -1) <= 0:
        #    raise ValueError("Invalid parameters: %s" %poisson_msg)

        # df is necessary and required to be positve
        elif distribution == 'chi2' and params.get('df', -1) <= 0:
            raise ValueError("Invalid parameters: %s:" %chi2_msg)

    elif isinstance(params, tuple) or isinstance(params, list):
        scale = None

        # `params` is a tuple or a list
        if distribution == 'beta':
            if len(params) < 2:
                raise ValueError("Missing required parameters: %s" %beta_msg)
            if params[0] <= 0 or params[1] <= 0:
                raise ValueError("Invalid parameters: %s" %beta_msg)
            if len(params) == 4:
                scale = params[3]
            elif len(params) > 4:
                raise ValueError("Too many parameters provided: %s" %beta_msg)

        elif distribution == 'norm':
            if len(params) > 2:
                raise ValueError("Too many parameters provided: %s" %norm_msg)
            if len(params) == 2:
                scale = params[1]

        elif distribution == 'gamma':
            if len(params) < 1:
                raise ValueError("Missing required parameters: %s" %gamma_msg)
            if len(params) == 3:
                scale = params[2]
            if len(params) > 3:
                raise ValueError("Too many parameters provided: %s" % gamma_msg)
            elif params[0] <= 0:
                raise ValueError("Invalid parameters: %s" %gamma_msg)

        #elif distribution == 'poisson':
        #    if len(params) < 1:
        #        raise ValueError("Missing required parameters: %s" %poisson_msg)
        #   if len(params) > 2:
        #        raise ValueError("Too many parameters provided: %s" %poisson_msg)
        #    elif params[0] <= 0:
        #        raise ValueError("Invalid parameters: %s" %poisson_msg)

        elif distribution == 'uniform':
            if len(params) == 2:
                scale = params[1]
            if len(params) > 2:
                raise ValueError("Too many arguments provided: %s" %uniform_msg)

        elif distribution == 'chi2':
            if len(params) < 1:
                raise ValueError("Missing required parameters: %s" %chi2_msg)
            elif len(params) == 3:
                scale = params[2]
            elif len(params) > 3:
                raise ValueError("Too many arguments provided: %s" %chi2_msg)
            if params[0] <= 0:
                raise ValueError("Invalid parameters: %s" %chi2_msg)

        elif distribution == 'expon':

            if len(params) == 2:
                scale = params[1]
            if len(params) > 2:
                raise ValueError("Too many arguments provided: %s" %expon_msg)

        if scale is not None and scale <= 0:
            raise ValueError("std_dev and scale must be positive.")

    else:
        raise ValueError(
                "params must be a dict or list, or use ge.dataset.util.infer_distribution_parameters(data, distribution)")

    return


def create_multiple_expectations(df, columns, expectation_type, *args, **kwargs):
    """Creates an identical expectation for each of the given columns with the specified arguments, if any.

    Args:
        df (great_expectations.dataset): A great expectations dataset object.
        columns (list): A list of column names represented as strings.
        expectation_type (string): The expectation type.

    Raises:
        KeyError if the provided column does not exist.
        AttributeError if the provided expectation type does not exist or df is not a valid great expectations dataset.

    Returns:
        A list of expectation results.


    """
    expectation = getattr(df, expectation_type)
    results = list()

    for column in columns:
        results.append(expectation(column, *args,  **kwargs))

    return results
