"""This module contains shared TypeAliases"""

import pathlib
from typing import Union

from typing_extensions import TypeAlias

PathStr: TypeAlias = Union[str, pathlib.Path]
