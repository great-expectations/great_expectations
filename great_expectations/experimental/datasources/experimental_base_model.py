from __future__ import annotations

import json
import logging
import pathlib
from io import StringIO
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Mapping,
    Type,
    TypeVar,
    Union,
    overload,
)

import pydantic
from ruamel.yaml import YAML

from great_expectations.experimental.datasources.constants import _FIELDS_ALWAYS_SET

if TYPE_CHECKING:
    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]

logger = logging.getLogger(__name__)

yaml = YAML(typ="safe")
# NOTE (kilo59): the following settings appear to be what we use in existing codebase
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

# TODO (kilo59): replace this with `typing_extensions.Self` once mypy supports it
# Taken from this SO answer https://stackoverflow.com/a/72182814/6304433
_Self = TypeVar("_Self", bound="ExperimentalBaseModel")


class ExperimentalBaseModel(pydantic.BaseModel):
    """
    Base model for most ZEP pydantic models.

    Adds yaml dumping and parsing methods.

    Extra fields are not allowed.

    Serialization methods default to `exclude_unset = True` to prevent serializing
    configs full of mostly unset default values.
    Also prevents passing along unset kwargs to BatchSpec.
    https://docs.pydantic.dev/usage/exporting_models/
    """

    class Config:
        extra = pydantic.Extra.forbid

    @classmethod
    def parse_yaml(cls: Type[_Self], f: Union[pathlib.Path, str]) -> _Self:
        loaded = yaml.load(f)
        logger.debug(f"loaded from yaml ->\n{pf(loaded, depth=3)}\n")
        # noinspection PyArgumentList
        config = cls(**loaded)
        return config

    @overload
    def yaml(
        self,
        stream_or_path: Union[StringIO, None] = None,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        by_alias: bool = ...,
        exclude_unset: bool = ...,
        exclude_defaults: bool = ...,
        exclude_none: bool = ...,
        encoder: Union[Callable[[Any], Any], None] = ...,
        models_as_dict: bool = ...,
        **yaml_kwargs,
    ) -> str:
        ...

    @overload
    def yaml(
        self,
        stream_or_path: pathlib.Path,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        by_alias: bool = ...,
        exclude_unset: bool = ...,
        exclude_defaults: bool = ...,
        exclude_none: bool = ...,
        encoder: Union[Callable[[Any], Any], None] = ...,
        models_as_dict: bool = ...,
        **yaml_kwargs,
    ) -> pathlib.Path:
        ...

    def yaml(
        self,
        stream_or_path: Union[StringIO, pathlib.Path, None] = None,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = None,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = True,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Union[Callable[[Any], Any], None] = None,
        models_as_dict: bool = True,
        **yaml_kwargs,
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
        intermediate_json = self._json_dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
        )
        yaml.dump(intermediate_json, stream=stream_or_path, **yaml_kwargs)

        if isinstance(stream_or_path, pathlib.Path):
            return stream_or_path
        return stream_or_path.getvalue()

    def json(
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        by_alias: bool = False,
        # deprecated - use exclude_unset instead
        skip_defaults: bool | None = None,
        # Default to True to prevent serializing long configs full of unset default values
        exclude_unset: bool = True,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Callable[[Any], Any] | None = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Generate a JSON representation of the model, `include` and `exclude` arguments
        as per `dict()`.

        `encoder` is an optional function to supply as `default` to json.dumps(), other
        arguments as per `json.dumps()`.

        Deviates from pydantic `exclude_unset` `True` by default instead of `False` by
        default.
        """
        self.__fields_set__.update(_FIELDS_ALWAYS_SET)
        return super().json(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
            **dumps_kwargs,
        )

    def _json_dict(
        self,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = None,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = True,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Union[Callable[[Any], Any], None] = None,
        models_as_dict: bool = True,
        **dumps_kwargs,
    ) -> dict:
        """
        JSON compatible dictionary. All complex types removed.
        Prefer `.dict()` or `.json()`
        """
        return json.loads(
            self.json(
                include=include,
                exclude=exclude,
                by_alias=by_alias,
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
                encoder=encoder,
                models_as_dict=models_as_dict,
                **dumps_kwargs,
            )
        )

    def dict(
        self,
        *,
        include: AbstractSetIntStr | MappingIntStrAny | None = None,
        exclude: AbstractSetIntStr | MappingIntStrAny | None = None,
        by_alias: bool = False,
        # Default to True to prevent serializing long configs full of unset default values
        exclude_unset: bool = True,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        # deprecated - use exclude_unset instead
        skip_defaults: bool | None = None,
    ) -> dict[str, Any]:
        """
        Generate a dictionary representation of the model, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic `exclude_unset` `True` by default instead of `False` by
        default.
        """
        self.__fields_set__.update(_FIELDS_ALWAYS_SET)
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            skip_defaults=skip_defaults,
        )

    def __str__(self):
        return self.yaml()
