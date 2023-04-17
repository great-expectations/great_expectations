from __future__ import annotations

import sys
from typing import Optional, Union

import numpy as np
from numpy import typing as npt
from packaging import version


def numpy_quantile(
    a: npt.NDArray, q: float, method: str, axis: Optional[int] = None
) -> Union[np.float64, npt.NDArray]:
    """
    As of NumPy 1.21.0, the 'interpolation' arg in quantile() has been renamed to `method`.
    Source: https://numpy.org/doc/stable/reference/generated/numpy.quantile.html
    """
    quantile: npt.NDArray
    if sys.version_info >= (3, 8):
        if version.parse(np.__version__) >= version.parse("1.22.0"):
            quantile = np.quantile(
                a=a,
                q=q,
                axis=axis,
                method=method,
            )
        else:
            quantile = np.quantile(
                a=a,
                q=q,
                axis=axis,
                interpolation=method,
            )
    else:
        # Handle type checking differently in python 3.7
        if version.parse(np.__version__) >= version.parse("1.22.0"):
            quantile = np.quantile(  # type: ignore[call-arg]
                a=a,
                q=q,
                axis=axis,
                method=method,
            )
        else:
            quantile = np.quantile(
                a=a,
                q=q,
                axis=axis,
                interpolation=method,
            )

    return quantile
