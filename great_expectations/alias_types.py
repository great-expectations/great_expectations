from __future__ import annotations

"""This module contains shared TypeAliases"""

import pathlib
from typing import Union

from typing_extensions import TypeAlias

PathStr: TypeAlias = Union[str, pathlib.Path]
JSONValues: TypeAlias = Union[
    dict[str, "JSONValues"], list["JSONValues"], str, int, float, bool, None
]
