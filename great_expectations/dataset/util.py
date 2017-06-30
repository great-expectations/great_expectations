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
        if isinstance(test_dict[key], (list, tuple, str, int, float, bool)):
            # No problem to encode json
            continue

        elif test_dict[key] is None:
            continue

        elif isinstance(test_dict[key], (np.ndarray, pd.Index)):
            test_dict[key] = test_dict[key].tolist()

        elif isinstance(test_dict[key], dict):
            test_dict[key] = ensure_json_serializable(test_dict[key])

        else:
            raise TypeError(key + ' is type ' + type(test_dict[key]).__name__ + ' which cannot be serialized.')

    return test_dict

def kde_compress_data(data):
    kde = stats.kde.gaussian_kde(data)
    partition = np.arange(start=np.min(data), stop= np.max(data), step=kde.covariance_factor())
    #density = kde.evaluate(partition)
    #return np.stack((partition,density))
    cdf_vals = [kde.integrate_box_1d(-np.inf, x) for x in partition]
    return (partition, cdf_vals)


def empirical_cdf(partition, data):
    return [np.sum(data < x) / len(data) for x in partition]

def naive_partition(data, n):
    if n > 1:
        return np.arange(start=np.min(data), stop=np.max(data), step=(np.max(data) - np.min(data)) / n )
    return np.min(data)

def ntile_partition(data, n):
    if n > 1:
        return np.percentile(data, np.arange(start=0, stop=100, step=100/n))
    return np.min(data)


def categorical_model(data):
    s = pd.Series(data).value_counts()
    return (s.index, s.values)
