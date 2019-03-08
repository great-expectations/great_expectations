# Utility methods for dealing with Dataset objects

from __future__ import division

from scipy import stats
import pandas as pd
import numpy as np
import warnings


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
    if("tail_weights" in partition_object):
        if (len(partition_object["tail_weights"])!=2):
            return False
        comb_weights=partition_object["tail_weights"]+partition_object["weights"]
    else:
        comb_weights=partition_object["weights"]
    # Expect one more bin edge than weight; all bin edges should be monotonically increasing; weights should sum to one
    if (len(partition_object['bins']) == (len(partition_object['weights']) + 1)) and \
            np.all(np.diff(partition_object['bins']) > 0) and \
            np.allclose(np.sum(comb_weights), 1):
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
                                  stop=np.max(data) +
                                  (kde.covariance_factor() / 2),
                                  num=np.floor(((np.max(data) - np.min(data)) / kde.covariance_factor()) + 1).astype(int))
    cdf_vals = [kde.integrate_box_1d(-np.inf, x) for x in evaluation_bins]
    evaluation_weights = np.diff(cdf_vals)

    if estimate_tails:
        bins = np.concatenate(([np.min(data) - (1.5 * kde.covariance_factor())],
                               evaluation_bins,
                               [np.max(data) + (1.5 * kde.covariance_factor())]))
    else:
        bins = np.concatenate(([-np.inf], evaluation_bins, [np.inf]))

    weights = np.concatenate(
        ([cdf_vals[0]], evaluation_weights, [1 - cdf_vals[-1]]))

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
        bins = np.linspace(start=np.min(data), stop=np.max(data), num=n_bins+1)
    elif bins == 'ntile':
        bins = np.percentile(data, np.linspace(
            start=0, stop=100, num=n_bins+1))
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
        raise TypeError(
            "params must be a dictionary object, see great_expectations documentation")

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

    # elif distribution == 'poisson':
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
    # elif distribution == 'expon':
        # scipy cdf(x, loc=0, scale=1)
    #    if 'lambda' in params.keys():
            # Lambda is optional
    #        params['scale'] = 1 / params['lambda']
    elif distribution is not 'norm':
        raise AttributeError(
            "Unsupported distribution type. Please refer to Great Expectations Documentation")

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
    # elif distribution == 'poisson':
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
        raise AttributeError(
            "Unsupported  distribution provided: %s" % distribution)

    if isinstance(params, dict):
        # `params` is a dictionary
        if params.get("std_dev", 1) <= 0 or params.get('scale', 1) <= 0:
            raise ValueError("std_dev and scale must be positive.")

        # alpha and beta are required and positive
        if distribution == 'beta' and (params.get('alpha', -1) <= 0 or params.get('beta', -1) <= 0):
            raise ValueError("Invalid parameters: %s" % beta_msg)

        # alpha is required and positive
        elif distribution == 'gamma' and params.get('alpha', -1) <= 0:
            raise ValueError("Invalid parameters: %s" % gamma_msg)

        # lambda is a required and positive
        # elif distribution == 'poisson' and params.get('lambda', -1) <= 0:
        #    raise ValueError("Invalid parameters: %s" %poisson_msg)

        # df is necessary and required to be positve
        elif distribution == 'chi2' and params.get('df', -1) <= 0:
            raise ValueError("Invalid parameters: %s:" % chi2_msg)

    elif isinstance(params, tuple) or isinstance(params, list):
        scale = None

        # `params` is a tuple or a list
        if distribution == 'beta':
            if len(params) < 2:
                raise ValueError("Missing required parameters: %s" % beta_msg)
            if params[0] <= 0 or params[1] <= 0:
                raise ValueError("Invalid parameters: %s" % beta_msg)
            if len(params) == 4:
                scale = params[3]
            elif len(params) > 4:
                raise ValueError("Too many parameters provided: %s" % beta_msg)

        elif distribution == 'norm':
            if len(params) > 2:
                raise ValueError("Too many parameters provided: %s" % norm_msg)
            if len(params) == 2:
                scale = params[1]

        elif distribution == 'gamma':
            if len(params) < 1:
                raise ValueError("Missing required parameters: %s" % gamma_msg)
            if len(params) == 3:
                scale = params[2]
            if len(params) > 3:
                raise ValueError(
                    "Too many parameters provided: %s" % gamma_msg)
            elif params[0] <= 0:
                raise ValueError("Invalid parameters: %s" % gamma_msg)

        # elif distribution == 'poisson':
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
                raise ValueError(
                    "Too many arguments provided: %s" % uniform_msg)

        elif distribution == 'chi2':
            if len(params) < 1:
                raise ValueError("Missing required parameters: %s" % chi2_msg)
            elif len(params) == 3:
                scale = params[2]
            elif len(params) > 3:
                raise ValueError("Too many arguments provided: %s" % chi2_msg)
            if params[0] <= 0:
                raise ValueError("Invalid parameters: %s" % chi2_msg)

        elif distribution == 'expon':

            if len(params) == 2:
                scale = params[1]
            if len(params) > 2:
                raise ValueError("Too many arguments provided: %s" % expon_msg)

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
