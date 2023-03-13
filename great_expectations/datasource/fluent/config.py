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
    List,
    Set,
    Tuple,
    Type,
    Union,
    overload,
)

from pydantic import Extra, Field, ValidationError, validator
from ruamel.yaml import YAML
from typing_extensions import Final

from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.interfaces import (
    Datasource,  # noqa: TCH001
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
        "loc": ("fluent_datasources",),
        "msg": "field required",
        "type": "value_error.missing",
    }
]


class GxConfig(FluentBaseModel):
    """Represents the full fluent configuration file."""

    fluent_datasources: Dict[str, Datasource] = Field(
        ..., description=_FLUENT_STYLE_DESCRIPTION
    )

    _EXCLUDE_FROM_DATASOURCE_SERIALIZATION: ClassVar[Set[str]] = {
        "name",  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }

    _EXCLUDE_FROM_DATA_ASSET_SERIALIZATION: ClassVar[Set[str]] = {
        "name",  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }

    @property
    def datasources(self) -> Dict[str, Datasource]:
        return self.fluent_datasources

    class Config:
        extra = Extra.ignore  # ignore any old style config keys

    # noinspection PyNestedDecorators
    @validator("fluent_datasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        logger.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: Dict[str, Datasource] = {}

        for ds_key, config in v.items():
            ds_type_name: str = config.get("type", "")
            if not ds_type_name:
                # TODO: (kilo59 122222) ideally this would be raised by `Datasource` validation
                # https://github.com/pydantic/pydantic/issues/734
                raise ValueError(f"'{ds_key}' is missing a 'type' entry")

            try:
                ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
                logger.debug(f"Instantiating '{ds_key}' as {ds_type}")
            except KeyError as type_lookup_err:
                raise ValueError(
                    f"'{ds_key}' has unsupported 'type' - {type_lookup_err}"
                ) from type_lookup_err

            if "name" in config:
                ds_name: str = config["name"]
                if ds_name != ds_key:
                    raise ValueError(
                        f'Datasource key "{ds_key}" is different from name "{ds_name}" in its configuration.'
                    )
            else:
                config["name"] = ds_key

            if "assets" not in config:
                config["assets"] = {}

            for asset_key, asset_config in config["assets"].items():
                if "name" in asset_config:
                    asset_name: str = asset_config["name"]
                    if asset_name != asset_key:
                        raise ValueError(
                            f'DataAsset key "{asset_key}" is different from name "{asset_name}" in its configuration.'
                        )
                else:
                    asset_config["name"] = asset_key

            datasource = ds_type(**config)

            # the ephemeral asset should never be serialized
            if DEFAULT_PANDAS_DATA_ASSET_NAME in datasource.assets:
                datasource.assets.pop(DEFAULT_PANDAS_DATA_ASSET_NAME)

            # if the default pandas datasource has no assets, it should not be serialized
            if (
                datasource.name != DEFAULT_PANDAS_DATASOURCE_NAME
                or len(datasource.assets) > 0
            ):
                loaded_datasources[datasource.name] = datasource

                # TODO: move this to a different 'validator' method
                # attach the datasource to the nested assets, avoiding recursion errors
                for asset in datasource.assets.values():
                    asset._datasource = datasource

        logger.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")

        if v and not loaded_datasources:
            logger.info(f"Of {len(v)} entries, no 'datasources' could be loaded")

        return loaded_datasources

    @classmethod
    def parse_yaml(
        cls: Type[GxConfig], f: Union[pathlib.Path, str], _allow_empty: bool = False
    ) -> GxConfig:
        """
        Overriding base method to allow an empty/missing `fluent_datasources` field.
        Other validation errors will still result in an error.

        TODO (kilo59) 122822: remove this as soon as it's no longer needed. Such as when
        we use a new `config_version` instead of `fluent_datasources` key.
        """
        if _allow_empty:
            try:
                super().parse_yaml(f)
            except ValidationError as validation_err:
                errors_list: List[PydanticErrorDict] = validation_err.errors()
                logger.info(
                    f"{cls.__name__}.parse_yaml() failed with errors - {errors_list}"
                )
                if errors_list == _MISSING_FLUENT_DATASOURCES_ERRORS:
                    logger.info(
                        f"{cls.__name__}.parse_yaml() returning empty `fluent_datasources`"
                    )
                    return cls(fluent_datasources={})
                else:
                    logger.warning(
                        "`_allow_empty` does not prevent unrelated validation errors"
                    )
                    raise

        # noinspection PyTypeChecker
        return super().parse_yaml(f)

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
        if "fluent_datasources" in config:
            fluent_datasources: dict = config["fluent_datasources"]

            datasource_name: str
            datasource_config: dict
            for datasource_name, datasource_config in fluent_datasources.items():
                datasource_config = _exclude_fields_from_serialization(
                    source_dict=datasource_config,
                    exclusions=self._EXCLUDE_FROM_DATASOURCE_SERIALIZATION,
                )
                if "assets" in datasource_config:
                    data_assets: dict = datasource_config["assets"]
                    data_asset_name: str
                    data_asset_config: dict
                    data_assets = {
                        data_asset_name: _exclude_fields_from_serialization(
                            source_dict=data_asset_config,
                            exclusions=self._EXCLUDE_FROM_DATA_ASSET_SERIALIZATION,
                        )
                        for data_asset_name, data_asset_config in data_assets.items()
                    }
                    datasource_config["assets"] = data_assets

                fluent_datasources[datasource_name] = datasource_config

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
