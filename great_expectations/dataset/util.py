# Utility methods for dealing with DataSet objects

from __future__ import division
import numpy as np
from scipy import stats
import pandas as pd
import warnings
import sys

from functools import wraps

class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
    def __dir__(self):
        return self.keys()


class DocInherit(object):
    """
    Docstring inheriting method descriptor

    The class itself is also used as a decorator
    doc_inherit decorator

    Usage:

    class Foo(object):
        def foo(self):
            "Frobber"
            pass

    class Bar(Foo):
        @doc_inherit
        def foo(self):
            pass

    Now, Bar.foo.__doc__ == Bar().foo.__doc__ == Foo.foo.__doc__ == "Frobber"
    # From here: https://stackoverflow.com/questions/2025562/inherit-docstrings-in-python-class-inheritance

    """

    def __init__(self, mthd):
        self.mthd = mthd
        self.name = mthd.__name__

    def __get__(self, obj, cls):
        if obj is not None:
            return self.get_with_inst(obj, cls)
        else:
            return self.get_no_inst(cls)

    def get_with_inst(self, obj, cls):

        overridden = getattr(super(cls, obj), self.name, None)

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            return self.mthd(obj, *args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def get_no_inst(self, cls):

        for parent in cls.__mro__[1:]:
            overridden = getattr(parent, self.name, None)
            if overridden: break

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            return self.mthd(*args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def use_parent_doc(self, func, source):
        if source is None:
            raise NameError("Can't find '%s' in parents" % self.name)
        func.__doc__ = source.__doc__
        return func


def ensure_json_serializable(test_dict):
    """
    Helper function to convert a dict object to one that is serializable

    :param test_dict: the dictionary to attempt to convert to be serializable to json.
    :return: None. test_dict is converted in place
    """
    # Validate that all aruguments are of approved types, coerce if it's easy, else exception
    for key in test_dict:
        if isinstance(test_dict[key], (list, tuple, str, int, float, bool)):
            # No problem to encode json
            continue

        elif test_dict[key] is None:
            continue

        elif isinstance(test_dict[key], (np.ndarray, pd.Index)):
            #test_dict[key] = test_dict[key].tolist()
            ## If we have an array or index, convert it first to a list--causing coercion to float--and then round
            ## to the number of digits for which the string representation will equal the float representation
            test_dict[key] = map(
                lambda x: round(x, sys.float_info.dig),
                test_dict[key].tolist()
            )


        elif isinstance(test_dict[key], dict):
            test_dict[key] = ensure_json_serializable(test_dict[key])

        else:
            try:
                # In Python 2, unicode and long should still be valid.
                # This will break in Python 3 and throw the exception instead.
                if isinstance(test_dict[key], (long, unicode)):
                    # No problem to encode json
                    continue
            except:
                raise TypeError(key + ' is type ' + type(test_dict[key]).__name__ + ' which cannot be serialized.')

    return test_dict

def is_valid_partition_object(partition_object):
    """Convenience method for determing whether a given partition object is a valid weighted partition of the real number line.
    """
    if (partition_object is None) or ("partition" not in partition_object) or ("weights" not in partition_object):
        return False
    if (len(partition_object['partition']) != (len(partition_object['weights']) + 1)):
        if (len(partition_object['partition']) != len(partition_object['weights'])):
            return False
    # TODO: Evaluate desired tolerance for weights
    if (abs(np.sum(partition_object['weights']) - 1) > 1e-4):
        return False
    return True

def is_valid_categorical_partition_object(partition_object):
    return is_valid_partition_object(partition_object) and (len(partition_object['partition']) == len(partition_object['weights']))

def is_valid_continuous_partition_object(partition_object):
    if is_valid_partition_object(partition_object) and (len(partition_object['partition']) == (len(partition_object['weights']) + 1)):
        if partition_object['partition'][0] == -np.inf and partition_object['partition'][-1] == np.inf:
            return True
    return False


def categorical_partition_data(data):
    """Convenience method for creating weights from categorical data.
    Args:
        data (list-like): The data from which to construct the estimate.
    Returns:
        dict:
            {
                "partition": (list) The categorical values present in the data
                "weights": (list) The weights of the values in the partition.
            }
    """
    s = pd.Series(data).value_counts()
    weights = s.values / (1. * len(data))
    return {
        "partition": s.index.tolist(),
        "weights":  weights.tolist()
    }

def kde_partition_data(data, internal_weight_holdout=1e-5, tail_weight_holdout=1e-5):
    """Convenience method for building a partition and weights using a gaussian Kernel Density Estimate and default bandwidth.

    Note that unlike continuous_partition_data, kde_smooth_data defaults to holding out internal weight.

    :param data (list-like): The data from which to construct the estimate
    :param internal_weight_holdout: the amount of weight to split uniformly and add to any partitions whose weight is zero in the source data
    :param tail_weight_holdout: the amount of weight to split uniformly and add to the tails of the histogram (the area between -Infinity and the data's min value and between the data's max value and Infinity)
    :return: A new partition_object:
        dict:
            {
                "partition": (list) The endpoints of the partial partition of reals,
                "weights": (list) The densities of the bins implied by the partition.
            }
    """
    if internal_weight_holdout < 0 or internal_weight_holdout > 1:
        raise ValueError("Internal weight holdout must be between zero and one")

    if tail_weight_holdout < 0 or tail_weight_holdout > 1:
        raise ValueError("Tail weight holdout must be between zero and one")

    kde = stats.kde.gaussian_kde(data)
    evaluation_partition = np.linspace(start = np.min(data) - (kde.covariance_factor() / 2),
                            stop = np.max(data) + (kde.covariance_factor() / 2),
                            num = np.floor(((np.max(data) - np.min(data)) / kde.covariance_factor()) + 1 ).astype(int))
    cdf_vals = [kde.integrate_box_1d(-np.inf, x) for x in evaluation_partition]
    evaluation_weights = np.diff(cdf_vals)

    # We need to account for weight outside the explicit partition at this point since we have smoothed.
    # No longer use lower_bound and upper_bound because of extending the interval from -np.inf to np.inf
    # lower_bound = np.min(data) - (1.5 * kde.covariance_factor())
    # upper_bound = np.max(data) + (1.5 * kde.covariance_factor())
    partition = [ -np.inf ] + evaluation_partition.tolist() + [ np.inf ]

    if internal_weight_holdout > 0:
        internal_zero_count = len(evaluation_weights) - np.count_nonzero(evaluation_weights)
        weights = [(internal_weight_holdout / (1. * internal_zero_count)) if weight == 0
                        else (weight * (1. - (internal_weight_holdout + tail_weight_holdout)))
                        for weight in evaluation_weights.tolist()]
    else:
        weights = [(weight * (1. - (internal_weight_holdout + tail_weight_holdout)))
                    for weight in evaluation_weights.tolist()]

    weights = [ (tail_weight_holdout / 2.) + cdf_vals[0] ] + weights + [1 - cdf_vals[-1] + (tail_weight_holdout / 2.)]

    return {
        "partition": partition,
        "weights": weights
    }

def partition_data(data, bins='auto', n_bins=10):
    warnings.warn("partition_data is deprecated and will be removed. Use either continuous_partition_data or \
                    categorical_partition_data instead.", DeprecationWarning)
    return continuous_partition_data(data, bins, n_bins)


def continuous_partition_data(data, bins='auto', n_bins=10, internal_weight_holdout=0.0, tail_weight_holdout=1e-5):
    """Convenience method for building a partition object on continuous data

    :param data (list-like): The data from which to construct the estimate.
    :param bins (string): One of 'uniform' (for uniformly spaced bins), 'ntile' (for percentile-spaced bins), or 'auto' (for automatically spaced bins)
    :param n_bins (int): Ignored if bins is auto.
    :param internal_weight_holdout: the amount of weight to split uniformly and add to any partitions whose weight is zero in the source data
    :param tail_weight_holdout: the amount of weight to split uniformly and add to the tails of the histogram (the area between -Infinity and the data's min value and between the data's max value and Infinity)
    :return: A new partition_object:
        dict:
            {
                "partition": (list) The endpoints of the partial partition of reals,
                "weights": (list) The densities of the bins implied by the partition.
            }
    """
    if bins == 'uniform':
        bins = np.linspace(start=np.min(data), stop=np.max(data), num = n_bins+1)
    elif bins =='ntile':
        bins = np.percentile(data, np.linspace(start=0, stop=100, num = n_bins+1))
    elif bins != 'auto':
        raise ValueError("Invalid parameter for bins argument")

    if internal_weight_holdout < 0 or internal_weight_holdout > 1:
        raise ValueError("Internal weight holdout must be between zero and one")

    if tail_weight_holdout < 0 or tail_weight_holdout > 1:
        raise ValueError("Tail weight holdout must be between zero and one")

    hist, bin_edges = np.histogram(data, bins, density=False)

    ## When np.histogram constructs its bins, it uses closed lower bounds and open upper bounds for all bins
    ## EXCEPT the largest bin, which is closed for both its lower and upper bound.

    ## Since we are about to add an additional bin to the top, we want to ensure that any values
    ## EXACTLY equal to the max value in the dataset used to construct the bins are still in the second-to-largest bin

    bin_edges[-1] = bin_edges[-1] + 1e-15
    bin_edges = [ -np.inf ] + bin_edges.tolist() + [ np.inf ]

    # Apply internal weight holdout to any zero-weight partitions, if applicable
    data_size = 1. * len(data)

    if internal_weight_holdout > 0:
        internal_zero_count = len(hist) - np.count_nonzero(hist)
        weights = [(internal_weight_holdout / (1. * internal_zero_count)) if count == 0
                        else (count * (1. - (internal_weight_holdout + tail_weight_holdout))) / data_size
                        for count in hist.tolist()]
    else:
        weights = [(count * (1. - (internal_weight_holdout + tail_weight_holdout))) / data_size
                    for count in hist.tolist()]

    # Apply tail weight holdout
    weights = [ tail_weight_holdout / 2. ] + weights + [ tail_weight_holdout / 2. ]

    return {
        "partition": bin_edges,
        "weights": weights
    }