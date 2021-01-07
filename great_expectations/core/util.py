import datetime
import decimal
import logging
import sys
from collections.abc import Mapping

import numpy as np
import pandas as pd
from IPython import get_ipython

from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.types import SerializableDictDot

# Updated from the stack overflow version below to concatenate lists
# https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth


logger = logging.getLogger(__name__)

try:
    import pyspark
except ImportError:
    pyspark = None
    logger.debug(
        "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes"
    )


def nested_update(d, u):
    """update d with items from u, recursively and joining elements"""
    for k, v in u.items():
        if isinstance(v, Mapping):
            d[k] = nested_update(d.get(k, {}), v)
        elif isinstance(v, set) or (k in d and isinstance(d[k], set)):
            s1 = d.get(k, set())
            s2 = v or set()
            d[k] = s1 | s2
        elif isinstance(v, list) or (k in d and isinstance(d[k], list)):
            l1 = d.get(k, [])
            l2 = v or []
            d[k] = l1 + l2
        else:
            d[k] = v
    return d


def in_jupyter_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def convert_to_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable
    Args:
        data: an object to attempt to convert a corresponding json-serializable object
    Returns:
        (dict) A converted test_object
    Warning:
        test_obj may also be converted in place.
    """

    # If it's one of our types, we use our own conversion; this can move to full schema
    # once nesting goes all the way down
    if isinstance(data, SerializableDictDot):
        return data.to_json_dict()

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return data

    elif isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    elif isinstance(data, (list, tuple, set)):
        new_list = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [convert_to_json_serializable(x) for x in data.tolist()]

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return data

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return bool(data)

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [
            {
                index_name: convert_to_json_serializable(idx),
                value_name: convert_to_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]

    elif isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient="records"))

    elif pyspark and isinstance(data, pyspark.sql.DataFrame):
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return convert_to_json_serializable(
            dict(zip(data.schema.names, zip(*data.collect())))
        )

    elif isinstance(data, decimal.Decimal):
        if requires_lossy_conversion(data):
            logger.warning(
                f"Using lossy conversion for decimal {data} to float object to support serialization."
            )
        return float(data)

    elif isinstance(data, RunIdentifier):
        return data.to_json_dict()

    else:
        raise TypeError(
            "%s is of type %s which cannot be serialized."
            % (str(data), type(data).__name__)
        )


def ensure_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable
    Args:
        data: an object to attempt to convert a corresponding json-serializable object
    Returns:
        (dict) A converted test_object
    Warning:
        test_obj may also be converted in place.
    """

    if isinstance(data, SerializableDictDot):
        return

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, ((str,), (int,), float, bool)):
        # No problem to encode json
        return

    elif isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    elif isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        _ = [ensure_json_serializable(x) for x in data.tolist()]
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        _ = [
            {
                index_name: ensure_json_serializable(idx),
                value_name: ensure_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]
        return

    elif pyspark and isinstance(data, pyspark.sql.DataFrame):
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return ensure_json_serializable(
            dict(zip(data.schema.names, zip(*data.collect())))
        )

    elif isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient="records"))

    elif isinstance(data, decimal.Decimal):
        return

    elif isinstance(data, RunIdentifier):
        return

    else:
        raise InvalidExpectationConfigurationError(
            "%s is of type %s which cannot be serialized to json"
            % (str(data), type(data).__name__)
        )


def requires_lossy_conversion(d):
    return d - decimal.Context(prec=sys.float_info.dig).create_decimal(d) != 0
