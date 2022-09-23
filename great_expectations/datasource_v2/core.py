import dataclasses as dc
import functools
from typing import Optional

from typing_extensions import Protocol


@dc.dataclass
class PandasSourceConfig:
    name: str
    base_dir: str


@dc.dataclass
class SQLSourceConfig:
    name: str
    connection_string: str
    database_file: Optional[str] = None


class Source(Protocol):
    pass


class PandasSource:
    pass


class SQLSource:
    pass


@functools.singledispatch
def create_source(config=None) -> Source:
    raise TypeError(
        f"No registered `create_source()` handler for {type(config)} - {config}"
    )


# @create_source.register(dict)
# def type_coercion(config: dict) -> Source:
#     # TODO: pull from all registerd types?
#     registed_types = [PandasSourceConfig, SQLSourceConfig]
#     for source_config in registed_types:
#         try:
#             coerced_type = source_config(**config)
#             return create_source(coerced_type)

#         except TypeError:
#             break

#     raise TypeError(f"Failed to coerce {config}")


@create_source.register(PandasSourceConfig)
def pandas_source(config: PandasSourceConfig) -> PandasSource:
    source = PandasSource()
    print(f"creating {source.__class__.__name__} ...")
    return source


@create_source.register(SQLSourceConfig)
def sql_source(config: SQLSourceConfig) -> SQLSource:
    source = SQLSource()
    print(f"creating {source.__class__.__name__} ...")
    return source


if __name__ == "__main__":
    create_source(PandasSourceConfig(name="taxi", base_dir="."))
    create_source(SQLSourceConfig(name="taxi", connection_string="taxi.db"))
    create_source({"name": "taxi", "base_dir": "."})
    create_source({"foo": "bar"})
