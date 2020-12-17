import datetime
import decimal
import json
import platform
import sys
from functools import wraps

import numpy as np
import pytest

import great_expectations as ge
from great_expectations.core.expectation_suite import ExpectationSuiteSchema
from tests.test_utils import expectationSuiteSchema


def test_recursively_convert_to_json_serializable():
    asset = ge.dataset.PandasDataset(
        {
            "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    asset.expect_column_values_to_be_in_set(
        "x", [1, 2, 3, 4, 5, 6, 7, 8, 9], mostly=0.8
    )

    part = ge.dataset.util.partition_data(asset.x)
    asset.expect_column_kl_divergence_to_be_less_than("x", part, 0.6)

    # Dumping this JSON object verifies that everything is serializable
    json.dumps(expectationSuiteSchema.dump(asset.get_expectation_suite()), indent=2)

    x = {
        "w": ["aaaa", "bbbb", 1.3, 5, 6, 7],
        "x": np.array([1, 2, 3]),
        "y": {"alpha": None, "beta": np.nan, "delta": np.inf, "gamma": -np.inf},
        "z": {1, 2, 3, 4, 5},
        "zz": (1, 2, 3),
        "zzz": [
            datetime.datetime(2017, 1, 1),
            datetime.date(2017, 5, 1),
        ],
        "np.bool": np.bool_([True, False, True]),
        "np.int_": np.int_([5, 3, 2]),
        "np.int8": np.int8([5, 3, 2]),
        "np.int16": np.int16([10, 6, 4]),
        "np.int32": np.int32([20, 12, 8]),
        "np.uint": np.uint([20, 5, 6]),
        "np.uint8": np.uint8([40, 10, 12]),
        "np.uint64": np.uint64([80, 20, 24]),
        "np.float_": np.float_([3.2, 5.6, 7.8]),
        "np.float32": np.float32([5.999999999, 5.6]),
        "np.float64": np.float64([5.9999999999999999999, 10.2]),
        # 'np.complex64': np.complex64([10.9999999 + 4.9999999j, 11.2+7.3j]),
        # 'np.complex128': np.complex128([20.999999999978335216827+10.99999999j, 22.4+14.6j]),
        # 'np.complex256': np.complex256([40.99999999 + 20.99999999j, 44.8+29.2j]),
        "np.str": np.unicode_(["hello"]),
        "yyy": decimal.Decimal(123.456),
    }
    if platform.system() != "Windows":
        x["np.float128"] = np.float128([5.999999999998786324399999999, 20.4])

    x = ge.data_asset.util.recursively_convert_to_json_serializable(x)
    assert isinstance(x["x"], list)

    assert isinstance(x["np.bool"][0], bool)
    assert isinstance(x["np.int_"][0], int)
    assert isinstance(x["np.int8"][0], int)
    assert isinstance(x["np.int16"][0], int)
    assert isinstance(x["np.int32"][0], int)

    assert isinstance(x["np.uint"][0], int)
    assert isinstance(x["np.uint8"][0], int)
    assert isinstance(x["np.uint64"][0], int)

    assert isinstance(x["np.float32"][0], float)
    assert isinstance(x["np.float64"][0], float)
    if platform.system() != "Windows":
        assert isinstance(x["np.float128"][0], float)
    # self.assertEqual(type(x['np.complex64'][0]), complex)
    # self.assertEqual(type(x['np.complex128'][0]), complex)
    # self.assertEqual(type(x['np.complex256'][0]), complex)
    assert isinstance(x["np.float_"][0], float)

    # Make sure nothing is going wrong with precision rounding
    if platform.system() != "Windows":
        assert np.allclose(
            x["np.float128"][0],
            5.999999999998786324399999999,
            atol=10 ** (-sys.float_info.dig),
        )

    # TypeError when non-serializable numpy object is in dataset.
    with pytest.raises(TypeError):
        y = {"p": np.DataSource()}
        ge.data_asset.util.recursively_convert_to_json_serializable(y)

    try:
        x = unicode("abcdefg")
        x = ge.data_asset.util.recursively_convert_to_json_serializable(x)
        assert isinstance(x, unicode)
    except NameError:
        pass


"""
The following Parent and Child classes are used for testing documentation inheritance.
"""


class Parent:
    """Parent class docstring"""

    @classmethod
    def expectation(cls, func):
        """Manages configuration and running of expectation objects."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            # wrapper logic
            func(*args, **kwargs)

        return wrapper

    def override_me(self):
        """Parent method docstring
        Returns:
            Unattainable abiding satisfaction.
        """
        raise NotImplementedError


class Child(Parent):
    """
    Child class docstring
    """

    @ge.data_asset.util.DocInherit
    @Parent.expectation
    def override_me(self):
        """Child method docstring
        Returns:
            Real, instantiable, abiding satisfaction.
        """


def test_doc_inheritance():
    c = Child()

    assert (
        c.__getattribute__("override_me").__doc__
        == """Child method docstring
        Returns:
            Real, instantiable, abiding satisfaction.
        """
        + "\n"
        """Parent method docstring
        Returns:
            Unattainable abiding satisfaction.
        """
    )
