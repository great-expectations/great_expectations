"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from io import StringIO
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Final,
    List,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from pydantic import Extra, Field, validator
from ruamel.yaml import YAML

from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.datasource.fluent.constants import (
    _DATA_ASSET_NAME_KEY,
    _DATASOURCE_NAME_KEY,
    _FLUENT_DATASOURCES_KEY,
)
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.interfaces import (
    Datasource,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    _SourceFactories,
)

if TYPE_CHECKING:
    from pydantic.error_wrappers import ErrorDict as PydanticErrorDict

    from great_expectations.datasource.fluent.fluent_base_model import (
        AbstractSetIntStr,
        MappingIntStrAny,
    )


logger = logging.getLogger(__name__)


yaml = YAML(typ="safe")
# NOTE (kilo59): the following settings appear to be what we use in existing codebase
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


_FLUENT_STYLE_DESCRIPTION: Final[str] = "Fluent Datasources"

_MISSING_FLUENT_DATASOURCES_ERRORS: Final[List[PydanticErrorDict]] = [
    {
        "loc": (_FLUENT_DATASOURCES_KEY,),
        "msg": "field required",
        "type": "value_error.missing",
    }
]

# sentinel value to know if parameter was passed
_MISSING: Final = object()

JSON_ENCODERS: dict[Type, Callable] = {}
if TextClause:
    JSON_ENCODERS[TextClause] = lambda v: str(v)

T = TypeVar("T")


class GxConfig(FluentBaseModel):
    """Represents the full fluent configuration file."""

    fluent_datasources: List[Datasource] = Field(
        ..., description=_FLUENT_STYLE_DESCRIPTION
    )

    _EXCLUDE_FROM_DATASOURCE_SERIALIZATION: ClassVar[Set[str]] = {
        _DATASOURCE_NAME_KEY,  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }

    _EXCLUDE_FROM_DATA_ASSET_SERIALIZATION: ClassVar[Set[str]] = {
        _DATA_ASSET_NAME_KEY,  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }

    class Config:
        extra = Extra.ignore  # ignore any old style config keys
        json_encoders = JSON_ENCODERS

    @property
    def datasources(self) -> List[Datasource]:
        """Returns available Fluent Datasources as list."""
        return self.fluent_datasources

    def get_datasources_as_dict(self) -> Dict[str, Datasource]:
        """Returns available Datasource objects as dictionary, with corresponding name as key.

        Returns:
            Dictionary of "Datasource" objects with "name" attribute serving as key.
        """
        datasource: Datasource
        datasources_as_dict: Dict[str, Datasource] = {
            datasource.name: datasource for datasource in self.fluent_datasources
        }

        return datasources_as_dict

    def get_datasource_names(self) -> Set[str]:
        """Returns the set of available Datasource names.

        Returns:
            Set of available Datasource names.
        """
        datasource: Datasource
        return {datasource.name for datasource in self.datasources}

    def get_datasource(self, datasource_name: str) -> Datasource:
        """Returns the Datasource referred to by datasource_name

        Args:
            datasource_name: name of Datasource sought.

        Returns:
            Datasource -- if named "Datasource" objects exists; otherwise, exception is raised.
        """
        try:
            datasource: Datasource
            return list(
                filter(
                    lambda datasource: datasource.name == datasource_name,
                    self.datasources,
                )
            )[0]
        except IndexError as exc:
            raise LookupError(
                f"'{datasource_name}' not found. Available datasources are {self.get_datasource_names()}"
            ) from exc

    def update_datasources(self, datasources: Dict[str, Datasource]) -> None:
        """
        Updates internal list of datasources using supplied datasources dictionary.

        Args:
            datasources: Dictionary of datasources to use to update internal datasources.
        """
        datasources_as_dict: Dict[str, Datasource] = self.get_datasources_as_dict()
        datasources_as_dict.update(datasources)
        self.fluent_datasources = list(datasources_as_dict.values())

    def pop(self, datasource_name: str, default: T = _MISSING) -> Datasource | T:  # type: ignore[assignment] # sentinel value is never returned
        """
        Returns and deletes the Datasource referred to by datasource_name

        Args:
            datasource_name: name of Datasource sought.

        Returns:
            Datasource -- if named "Datasource" objects exists or the provided default;
                otherwise, exception is raised.
        """
        ds_dicts = self.get_datasources_as_dict()
        result: T | Datasource
        if default is _MISSING:
            result = ds_dicts.pop(datasource_name)
        else:
            result = ds_dicts.pop(datasource_name, default)

        self.fluent_datasources = list(ds_dicts.values())
        return result

    # noinspection PyNestedDecorators
    @validator(_FLUENT_DATASOURCES_KEY, pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: List[dict]):
        logger.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: List[Datasource] = []

        for config in v:
            ds_type_name: str = config.get("type", "")
            ds_name: str = config[_DATASOURCE_NAME_KEY]
            if not ds_type_name:
                # TODO: (kilo59 122222) ideally this would be raised by `Datasource` validation
                # https://github.com/pydantic/pydantic/issues/734
                raise ValueError(f"'{ds_name}' is missing a 'type' entry")

            try:
                ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
                logger.debug(f"Instantiating '{ds_name}' as {ds_type}")
            except KeyError as type_lookup_err:
                raise ValueError(
                    f"'{ds_name}' has unsupported 'type' - {type_lookup_err}"
                ) from type_lookup_err

            if "assets" not in config:
                config["assets"] = []

            datasource = ds_type(**config)

            # the ephemeral asset should never be serialized
            if DEFAULT_PANDAS_DATA_ASSET_NAME in datasource.get_assets_as_dict():
                datasource.delete_asset(asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME)

            # if the default pandas datasource has no assets, it should not be serialized
            if (
                datasource.name != DEFAULT_PANDAS_DATASOURCE_NAME
                or len(datasource.assets) > 0
            ):
                loaded_datasources.append(datasource)

                # TODO: move this to a different 'validator' method
                # attach the datasource to the nested assets, avoiding recursion errors
                for asset in datasource.assets:
                    asset._datasource = datasource

        logger.debug(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")

        if v and not loaded_datasources:
            logger.info(f"Of {len(v)} entries, no 'datasources' could be loaded")

        return loaded_datasources

    @classmethod
    def parse_yaml(
        cls: Type[GxConfig], f: Union[pathlib.Path, str], _allow_empty: bool = False
    ) -> GxConfig:
        """
        Overriding base method to allow an empty/missing `fluent_datasources` field.
        In addition, converts datasource and assets configuration sections from dictionary style to list style.
        Other validation errors will still result in an error.

        TODO (kilo59) 122822: remove this as soon as it's no longer needed. Such as when
        we use a new `config_version` instead of `fluent_datasources` key.
        """
        loaded = yaml.load(f)
        logger.debug(f"loaded from yaml ->\n{pf(loaded, depth=3)}\n")
        loaded = _convert_fluent_datasources_loaded_from_yaml_to_internal_object_representation(
            config=loaded, _allow_empty=_allow_empty
        )
        if _FLUENT_DATASOURCES_KEY not in loaded:
            return cls(fluent_datasources=[])

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

        intermediate_json_dict = self._json_dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            models_as_dict=models_as_dict,
        )
        intermediate_json_dict = self._exclude_name_fields_from_fluent_datasources(
            config=intermediate_json_dict
        )
        yaml.dump(intermediate_json_dict, stream=stream_or_path, **yaml_kwargs)

        if isinstance(stream_or_path, pathlib.Path):
            return stream_or_path

        return stream_or_path.getvalue()

    def _exclude_name_fields_from_fluent_datasources(
        self, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        if _FLUENT_DATASOURCES_KEY in config:
            fluent_datasources_config_as_dict = {}

            fluent_datasources: List[dict] = config[_FLUENT_DATASOURCES_KEY]

            datasource_name: str
            datasource_config: dict
            for datasource_config in fluent_datasources:
                datasource_name = datasource_config[_DATASOURCE_NAME_KEY]
                datasource_config = _exclude_fields_from_serialization(  # noqa: PLW2901
                    source_dict=datasource_config,
                    exclusions=self._EXCLUDE_FROM_DATASOURCE_SERIALIZATION,
                )
                if "assets" in datasource_config:
                    data_assets: List[dict] = datasource_config["assets"]
                    data_asset_config: dict
                    data_assets_config_as_dict = {
                        data_asset_config[
                            _DATA_ASSET_NAME_KEY
                        ]: _exclude_fields_from_serialization(
                            source_dict=data_asset_config,
                            exclusions=self._EXCLUDE_FROM_DATA_ASSET_SERIALIZATION,
                        )
                        for data_asset_config in data_assets
                    }
                    datasource_config["assets"] = data_assets_config_as_dict

                fluent_datasources_config_as_dict[datasource_name] = datasource_config

            config[_FLUENT_DATASOURCES_KEY] = fluent_datasources_config_as_dict

        return config


def _exclude_fields_from_serialization(
    source_dict: Dict[str, Any], exclusions: Set[str]
) -> Dict[str, Any]:
    element: Tuple[str, Any]
    # noinspection PyTypeChecker
    return dict(
        filter(
            lambda element: element[0] not in exclusions,
            source_dict.items(),
        )
    )


def _convert_fluent_datasources_loaded_from_yaml_to_internal_object_representation(
    config: Dict[str, Any], _allow_empty: bool = False
) -> Dict[str, Any]:
    if _FLUENT_DATASOURCES_KEY in config:
        fluent_datasources: dict = config[_FLUENT_DATASOURCES_KEY] or {}

        datasource_name: str
        datasource_config: dict
        for datasource_name, datasource_config in fluent_datasources.items():
            datasource_config[_DATASOURCE_NAME_KEY] = datasource_name
            if "assets" in datasource_config:
                data_assets: dict = datasource_config["assets"]
                data_asset_name: str
                data_asset_config: dict
                for data_asset_name, data_asset_config in data_assets.items():
                    data_asset_config[_DATA_ASSET_NAME_KEY] = data_asset_name

                datasource_config["assets"] = list(data_assets.values())

            fluent_datasources[datasource_name] = datasource_config

        config[_FLUENT_DATASOURCES_KEY] = list(fluent_datasources.values())

    return config
