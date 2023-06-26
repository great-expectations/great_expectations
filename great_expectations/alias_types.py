"""This module contains shared TypeAliases"""

import pathlib
from typing import Dict, List, Union

from typing_extensions import TypeAlias

PathStr: TypeAlias = Union[str, pathlib.Path]
JSONValues: TypeAlias = Union[
    Dict[str, "JSONValues"], List["JSONValues"], str, int, float, bool, None
]
