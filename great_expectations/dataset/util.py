#!! This is a second copy of this file. Inelegant.

import numpy as np
from scipy import stats
import pandas as pd


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
    def __dir__(self):
        return self.keys()

def ensure_json_serializable(test_dict):
    # Validate that all aruguments are of approved types, coerce if it's easy, else exception
    # TODO: ensure in-place iteration like this is okay
    for key in test_dict:
        ## TODO: Brittle if non python3 (unicode, long type?)
        if isinstance(test_dict[key], (list, tuple, str, unicode, int, float, bool)):
            # No problem to encode json
            continue

        elif test_dict[key] is None:
            continue

        elif isinstance(test_dict[key], (np.ndarray, pd.Index)):
            #TODO: Evaluate risk of precision loss in this call and weigh options for performance
            test_dict[key] = test_dict[key].tolist()

        elif isinstance(test_dict[key], dict):
            test_dict[key] = ensure_json_serializable(test_dict[key])

        else:
            raise TypeError(key + ' is type ' + type(test_dict[key]).__name__ + ' which cannot be serialized.')

    return test_dict

def is_valid_partition_object(partition_object):
    """Convenience method for determing whether a given partition object is a valid weighted partition of the real number line.
    """
    if ("partition" not in partition_object) or ("weights" not in partition_object):
        return False
    if (len(partition_object['partition']) != (len(partition_object['weights']) + 1)):
        return False
    # TODO: Evaluate desired tolerance for weights
    if (abs(np.sum(partition_object['weights']) - 1) > 1e-10):
        return False
    return True

def cumulative_densities(partition, data):
    """Convenience method for evaluating cumulative densities given a partition and dataset.
    """
    return [1. * np.sum(data < x) / (1. * len(data)) for x in partition]

def weights(partition, data):
    """Convenience method for evaluating partition densities given a partition and dataset.
    """
    cumulative = cumulative_densities(partition, data)
    return np.diff(cumulative)
    #return [cumulative[k+1] - cumulative[k] for k in range(len(partition)-1)]

def even_partition(data, n):
    """Convenience method for creating an even partition of a provided data sample.
    """
    return np.linspace(start=np.min(data), stop=np.max(data), num = (n+1))

def ntile_partition(data, n):
    """Convenience method for creating a percentile-based partition of a provided data sample.
    """
    return np.percentile(data, np.linspace(start=0, stop=100, num = (n+1)))

def categorical_partition(data):
    """Convenience method for creating densities from categorical data.
    """
    s = pd.Series(data).value_counts()
    return (s.index, (s.values / 1. * len(data)))

def kde_smooth_data(data):
    """Convenience method for building a partition and weights using a gaussian Kernel Density Estimate and default bandwidth.
    Args:
        data (list-like): The data from which to construct the estimate.
    Returns:
        dict:
            {
                "partition": (list) The edges of the partial partition of reals implied by the data and covariance_factor,
                "weights": (list) The densities of the bins implied by the partition.
            }
    """
    kde = stats.kde.gaussian_kde(data)
    partition = np.linspace(start = np.min(data) - (kde.covariance_factor() / 2),
                            stop = np.max(data) + (kde.covariance_factor() / 2),
                            num = ((np.max(data) - np.min(data)) / kde.covariance_factor()) + 1 )
    densities = weights(partition, data)
    return { "partition": partition, "weights": densities }

def partition_data(data, bins='auto', n_bins='10'):
    """Convenience method for building a partition and weights using simple options.
    Args:
        data (list-like): The data from which to construct the estimate.
        bins (string): One of 'even' (for evenly spaced bins), 'ntile' (for percentile-spaced bins), or 'auto' (for automatically spaced bins)
        n_bins (int): Ignored if bins is auto.
    Returns:
        dict:
            {
                "partition": (list) The edges of the partial partition of reals implied by the data and covariance_factor,
                "weights": (list) The densities of the bins implied by the partition.
            }
    """
    if bins == 'even':
        partition = even_partition(data, n_bins)
    elif bins =='ntile':
        partition = ntile_partition(data, n_bins)
    elif bins == 'auto':
        hist, bin_edges = np.histogram(data, bins='auto', density=False)
        return { "partition": bin_edges, "weights": hist / (1.*len(data)) }

    return {
      "partition": partition,
      "weights": weights(partition, data)
    }
