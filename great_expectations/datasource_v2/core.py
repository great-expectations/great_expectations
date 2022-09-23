import dataclasses as dc
import functools
from typing import Union

from typing_extensions import Protocol


@dc.dataclass
class PandasSourceConfig:
    name: str
    base_dir: str


class Source(Protocol):
    pass


class PandasSource:
    pass


@functools.singledispatch
def create_source(config=None) -> Source:
    raise TypeError(
        f"No registered `create_source()` handler for {type(config)} - {config}"
    )


@create_source.register(PandasSourceConfig)
def pandas_source(config: PandasSourceConfig) -> PandasSource:
    source = PandasSource()
    print(f"creating {source.__class__.__name__} ...")
    return source


if __name__ == "__main__":
    create_source(PandasSourceConfig(name="taxi", base_dir="."))
    create_source({"foo": "bar"})
