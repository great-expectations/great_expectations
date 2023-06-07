from __future__ import annotations

import json
import logging
import pathlib
from collections.abc import MutableMapping, MutableSequence
from io import StringIO
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Mapping,
    Type,
    TypeVar,
    Union,
    overload,
)

import pydantic
from ruamel.yaml import YAML

from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.constants import (
    _ASSETS_KEY,
    _FIELDS_ALWAYS_SET,
)

if TYPE_CHECKING:
    MappingIntStrAny = Mapping[Union[int, str], Any]
    AbstractSetIntStr = AbstractSet[Union[int, str]]
    from great_expectations.core.config_provider import _ConfigurationProvider

logger = logging.getLogger(__name__)

yaml = YAML(typ="safe")
# NOTE (kilo59): the following settings appear to be what we use in existing codebase
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

# TODO (kilo59): replace this with `typing_extensions.Self` once mypy supports it
# Taken from this SO answer https://stackoverflow.com/a/72182814/6304433
_Self = TypeVar("_Self", bound="FluentBaseModel")


class FluentBaseModel(pydantic.BaseModel):
    """
    Base model for most fluent datasource related pydantic models.

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
    def yaml(  # noqa: PLR0913
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
    def yaml(  # noqa: PLR0913
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

    def yaml(  # noqa: PLR0913
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

    def json(  # noqa: PLR0913
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
        _update__fields_set__on_truthyness(self, _ASSETS_KEY)

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

    def _json_dict(  # noqa: PLR0913
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

    def dict(  # noqa: PLR0913
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
        # custom
        config_provider: _ConfigurationProvider | None = None,
    ) -> dict[str, Any]:
        """
        Generate a dictionary representation of the model, optionally specifying which
        fields to include or exclude.

        Deviates from pydantic `exclude_unset` `True` by default instead of `False` by
        default.
        """
        self.__fields_set__.update(_FIELDS_ALWAYS_SET)
        _update__fields_set__on_truthyness(self, _ASSETS_KEY)

        result = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            skip_defaults=skip_defaults,
        )
        if config_provider:
            logger.info(
                f"{self.__class__.__name__}.dict() - substituting config values"
            )
            _recursively_set_config_value(result, config_provider)
        return result

    @staticmethod
    def _include_exclude_to_dict(
        include_exclude: AbstractSetIntStr | MappingIntStrAny | None,
    ) -> Dict[int | str, Any]:
        """
        Takes the mapping or abstract set passed to pydantic model export include or exclude and makes it a
        mutable dictionary that can be altered for nested include/exclude operations.

        See: https://docs.pydantic.dev/usage/exporting_models/#advanced-include-and-exclude

        Args:
            include_exclude: The include or exclude key passed to pydantic model export methods.

        Returns: A mutable dictionary that can be used for nested include/exclude.
        """
        if isinstance(include_exclude, Mapping):
            include_exclude_dict = dict(include_exclude)
        elif isinstance(include_exclude, AbstractSet):
            include_exclude_dict = {field: True for field in include_exclude}
        else:
            include_exclude_dict = {}
        return include_exclude_dict

    def __str__(self):
        return self.yaml()


def _recursively_set_config_value(
    data: MutableMapping | MutableSequence, config_provider: _ConfigurationProvider
) -> None:
    if isinstance(data, MutableMapping):
        for k, v in data.items():
            if isinstance(v, ConfigStr):
                data[k] = v.get_config_value(config_provider)
            elif isinstance(v, (MutableMapping, MutableSequence)):
                _recursively_set_config_value(v, config_provider)
    elif isinstance(data, MutableSequence):
        for i, v in enumerate(data):
            if isinstance(v, ConfigStr):
                data[i] = v.get_config_value(config_provider)
            elif isinstance(v, (MutableMapping, MutableSequence)):
                _recursively_set_config_value(v, config_provider)


def _update__fields_set__on_truthyness(model: FluentBaseModel, field_name: str) -> None:
    """
    This method updates the special `__fields__set__` attribute if the provided field is
    present and the value truthy. Otherwise it removes the entry from `__fields_set__`.

    For background `__fields_set__` is what determines whether or not a field is
    serialized when `exclude_unset` is used with `.dict()`/`.json()`/`.yaml()`.

    This is set automatically in most cases, but if a field was set with a `pre`
    validator then this will not have been updated and so if we want it to be dumped
    when `exclude_unset` is used we need to update `__fields_set__`.

    https://docs.pydantic.dev/usage/validators/#pre-and-per-item-validators
    https://docs.pydantic.dev/usage/exporting_models/#modeldict
    """
    if getattr(model, field_name, None):
        model.__fields_set__.add(field_name)
        logger.debug(f"{model.__class__.__name__}.__fields_set__ {field_name} added")
    else:
        model.__fields_set__.discard(field_name)
        logger.debug(
            f"{model.__class__.__name__}.__fields_set__ {field_name} discarded"
        )
