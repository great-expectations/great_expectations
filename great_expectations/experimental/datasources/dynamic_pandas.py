import inspect
from pprint import pformat as pf
from typing import Callable, List, Tuple

import pandas as pd
from typing_extensions import reveal_type


def _public_dir(obj: object) -> List[str]:
    return [x for x in dir(obj) if not x.startswith("_")]


def _extract_io_methods() -> List[Tuple[str, Callable[..., pd.DataFrame]]]:
    member_functions = inspect.getmembers(pd.io.api, predicate=inspect.isfunction)
    return [t for t in member_functions if t[0].startswith("read_")]


if __name__ == "__main__":
    print(f"  IO Methods\n{pf(_extract_io_methods())}")
