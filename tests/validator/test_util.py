import datetime
import decimal
import platform
import sys

import numpy as np
import pytest
from numpy.lib.npyio import DataSource

from great_expectations import validator


@pytest.mark.big
def test_recursively_convert_to_json_serializable(tmp_path):
    x = {
        "w": ["aaaa", "bbbb", 1.3, 5, 6, 7],
        "x": np.array([1, 2, 3]),
        "y": {"alpha": None, "beta": np.nan, "delta": np.inf, "gamma": -np.inf},
        "z": {1, 2, 3, 4, 5},
        "zz": (1, 2, 3),
        "zzz": [
            datetime.datetime(2017, 1, 1),  # noqa: DTZ001
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
        "np.float_": np.float64([3.2, 5.6, 7.8]),
        "np.float32": np.float32([5.999999999, 5.6]),
        "np.float64": np.float64([5.9999999999999999999, 10.2]),
        # 'np.complex64': np.complex64([10.9999999 + 4.9999999j, 11.2+7.3j]),
        # 'np.complex128': np.complex128([20.999999999978335216827+10.99999999j, 22.4+14.6j]),
        # 'np.complex256': np.complex256([40.99999999 + 20.99999999j, 44.8+29.2j]),
        "np.str": np.str_(["hello"]),
        "yyy": decimal.Decimal(123.456),
    }
    if hasattr(np, "float128") and platform.system() != "Windows":
        x["np.float128"] = np.float128([5.999999999998786324399999999, 20.4])

    x = validator.util.recursively_convert_to_json_serializable(x)
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
    if hasattr(np, "float128") and platform.system() != "Windows":
        assert isinstance(x["np.float128"][0], float)
    # self.assertEqual(type(x['np.complex64'][0]), complex)
    # self.assertEqual(type(x['np.complex128'][0]), complex)
    # self.assertEqual(type(x['np.complex256'][0]), complex)
    assert isinstance(x["np.float_"][0], float)

    # Make sure nothing is going wrong with precision rounding
    if hasattr(np, "float128") and platform.system() != "Windows":
        assert np.allclose(
            x["np.float128"][0],
            5.999999999998786324399999999,
            atol=10 ** (-sys.float_info.dig),
        )

    # TypeError when non-serializable numpy object is in dataset.
    with pytest.raises(TypeError):
        y = {"p": DataSource(tmp_path)}
        validator.util.recursively_convert_to_json_serializable(y)
