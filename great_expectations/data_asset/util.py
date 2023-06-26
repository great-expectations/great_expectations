# Utility methods for dealing with DataAsset objects

import datetime
import decimal
import sys
from functools import wraps

import numpy as np
import pandas as pd

from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.types import SerializableDictDot, SerializableDotDict


def parse_result_format(result_format):
    """This is a simple helper utility that can be used to parse a string result_format into the dict format used
    internally by great_expectations. It is not necessary but allows shorthand for result_format in cases where
    there is no need to specify a custom partial_unexpected_count."""
    if isinstance(result_format, str):
        result_format = {"result_format": result_format, "partial_unexpected_count": 20}
    else:
        if "partial_unexpected_count" not in result_format:  # noqa: PLR5501
            result_format["partial_unexpected_count"] = 20

    return result_format


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


class DocInherit:
    def __init__(self, mthd) -> None:
        self.mthd = mthd
        self.name = mthd.__name__
        self.mthd_doc = mthd.__doc__

    def __get__(self, obj, cls):
        doc = self.mthd_doc if self.mthd_doc is not None else ""

        for parent in cls.mro():
            if self.name not in parent.__dict__:
                continue
            if parent.__dict__[self.name].__doc__ is not None:
                doc = f"{doc}\n{parent.__dict__[self.name].__doc__}"

        @wraps(self.mthd, assigned=("__name__", "__module__"))
        def f(*args, **kwargs):
            return self.mthd(obj, *args, **kwargs)

        f.__doc__ = doc
        return f


def recursively_convert_to_json_serializable(test_obj):  # noqa: C901, PLR0911, PLR0912
    """
    Helper function to convert a dict object to one that is serializable

    Args:
        test_obj: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    # If it's one of our types, we pass
    if isinstance(test_obj, (SerializableDictDot, SerializableDotDict)):
        return test_obj

    # Validate that all aruguments are of approved types, coerce if it's easy, else exception
    # print(type(test_obj), test_obj)
    # Note: Not 100% sure I've resolved this correctly...
    try:
        if not isinstance(test_obj, list) and np.isnan(test_obj):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list)`
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(test_obj, (str, int, float, bool)):
        # No problem to encode json
        return test_obj

    elif isinstance(test_obj, dict):
        new_dict = {}
        for key in test_obj:
            if key == "row_condition" and test_obj[key] is not None:
                ensure_row_condition_is_correct(test_obj[key])
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = recursively_convert_to_json_serializable(test_obj[key])

        return new_dict

    elif isinstance(test_obj, (list, tuple, set)):
        new_list = []
        for val in test_obj:
            new_list.append(recursively_convert_to_json_serializable(val))

        return new_list

    elif isinstance(test_obj, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [recursively_convert_to_json_serializable(x) for x in test_obj.tolist()]

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif test_obj is None:
        # No problem to encode json
        return test_obj

    elif isinstance(test_obj, (datetime.datetime, datetime.date)):
        return str(test_obj)

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(test_obj), np.bool_):
        return bool(test_obj)

    elif np.issubdtype(type(test_obj), np.integer) or np.issubdtype(
        type(test_obj), np.uint
    ):
        return int(test_obj)

    elif np.issubdtype(type(test_obj), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(test_obj, sys.float_info.dig))

    elif isinstance(test_obj, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = test_obj.index.name or "index"
        value_name = test_obj.name or "value"
        return [
            {
                index_name: recursively_convert_to_json_serializable(idx),
                value_name: recursively_convert_to_json_serializable(val),
            }
            for idx, val in test_obj.items()
        ]

    elif isinstance(test_obj, pd.DataFrame):
        return recursively_convert_to_json_serializable(
            test_obj.to_dict(orient="records")
        )

    # elif np.issubdtype(type(test_obj), np.complexfloating):
    # Note: Use np.complexfloating to avoid Future Warning from numpy
    # Complex numbers consist of two floating point numbers
    # return complex(
    #     float(round(test_obj.real, sys.float_info.dig)),
    #     float(round(test_obj.imag, sys.float_info.dig)))

    elif isinstance(test_obj, decimal.Decimal):
        return float(test_obj)

    else:
        raise TypeError(
            f"{str(test_obj)} is of type {type(test_obj).__name__} which cannot be serialized."
        )


def ensure_row_condition_is_correct(row_condition_string) -> None:
    """Ensure no quote nor \\\\n are introduced in row_condition string.

    Otherwise it may cause an issue at the reload of the expectation.
    An error is raised at the declaration of the expectations to ensure
    the user is not doing a mistake. He can use double quotes for example.

    Parameters
    ----------
    row_condition_string : str
        the pandas query string
    """
    if "'" in row_condition_string:
        raise InvalidExpectationConfigurationError(
            f"{row_condition_string} cannot be serialized to json. "
            "Do not introduce simple quotes in configuration."
            "Use double quotes instead."
        )
    if "\n" in row_condition_string:
        raise InvalidExpectationConfigurationError(
            f"{repr(row_condition_string)} cannot be serialized to json. Do not introduce \\n in configuration."
        )
