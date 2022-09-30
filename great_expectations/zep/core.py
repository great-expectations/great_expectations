import dataclasses as dc
import functools
import re
from typing import Optional, Union

import pandas as pd

from typing_extensions import Protocol


@dc.dataclass(frozen=True)
class Pandas:
    name: str


@dc.dataclass(frozen=True)
class SQL:
    name: str


class Datasource(Protocol):
    ...
    # def add_asset(self):
    #     ...


class PandasDatasource:
    ...
    # def add_asset(self):
    #     pass


class SQLDatasource:
    ...
    # def add_asset(self):
    #     pass


@functools.singledispatch
def create_source(config=None) -> Datasource:
    raise TypeError(
        f"No registered `create_source()` handler for {type(config)} - {config}"
    )

@create_source.register(Pandas)
def create_pandas(type_: Pandas) -> PandasDatasource:
    source = PandasDatasource()
    print(f"creating {source.__class__.__name__} ...")
    return source

@create_source.register(type(pd.DataFrame))
def create_pandas2(type_: pd.DataFrame) -> PandasDatasource:
    source = PandasDatasource()
    print(f"creating {source.__class__.__name__} from {type_} ...")
    return source


@create_source.register(SQL)
def create_sql(type_: SQL) -> SQLDatasource:
    source = SQLDatasource()
    print(f"creating {source.__class__.__name__} ...")
    return source


if __name__ == "__main__":
    create_source(SQL("taxi"))
    create_source(Pandas("taxi"))
    create_source(pd.DataFrame)
    # create_source({"foo": "bar"})
