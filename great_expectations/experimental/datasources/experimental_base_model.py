from __future__ import annotations

import json
import logging
import pathlib

# from copy import deepcopy
from io import StringIO
from pprint import pformat as pf
from typing import Type, TypeVar, Union, overload

import pydantic
from ruamel.yaml import YAML

LOGGER = logging.getLogger(__name__)

yaml = YAML(typ="safe")
# NOTE (kilo59): the following settings appear to be what we use in existing codebase
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

# TODO (kilo59): replace this with `typing_extensions.Self` once mypy supports it
# Taken from this SO answer https://stackoverflow.com/a/72182814/6304433
_Self = TypeVar("_Self", bound="ExperimentalBaseModel")


DEEP_COPY_CALLS = 0


class ExperimentalBaseModel(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.forbid
        # copy_on_model_validation = "deep"

    @classmethod
    def parse_yaml(cls: Type[_Self], f: Union[pathlib.Path, str]) -> _Self:
        loaded = yaml.load(f)
        LOGGER.debug(f"loaded from yaml ->\n{pf(loaded, depth=3)}\n")
        config = cls(**loaded)
        return config

    @overload
    def yaml(self, stream_or_path: Union[StringIO, None] = None, **yaml_kwargs) -> str:
        ...

    @overload
    def yaml(self, stream_or_path: pathlib.Path, **yaml_kwargs) -> pathlib.Path:
        ...

    def yaml(
        self, stream_or_path: Union[StringIO, pathlib.Path, None] = None, **yaml_kwargs
    ) -> Union[str, pathlib.Path]:
        """
        Serialize the config object as yaml.

        Writes to a file if a `pathlib.Path` is provided.
        Else it writes to a stream and returns a yaml string.
        """
        if stream_or_path is None:
            stream_or_path = StringIO()

        # pydantic json encoder has support for many more types
        # TODO: can we dump json string directly to yaml.dump?
        intermediate_json = json.loads(self.json())
        yaml.dump(intermediate_json, stream=stream_or_path, **yaml_kwargs)

        if isinstance(stream_or_path, pathlib.Path):
            return stream_or_path
        return stream_or_path.getvalue()

    def __str__(self):
        return self.yaml()

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        global DEEP_COPY_CALLS
        DEEP_COPY_CALLS += 1
        if DEEP_COPY_CALLS < 40:
            print(
                f"{DEEP_COPY_CALLS} __deepcopy__ {self.__class__.__name__}  \tmemo:{len(memo)}"
            )
            # print(list(memo.keys()))
            for i, (k, v) in enumerate(memo.items()):
                print(
                    "\t",
                    i,
                    k,
                    type(v),
                    # v,
                )

        copy = self.copy(deep=True)
        memo[id(copy)] = copy

        print(f" return __deepcopy__ {self.__class__.__name__}")
        return copy
