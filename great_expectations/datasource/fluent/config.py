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

from pydantic import Extra, Field, validator
from ruamel.yaml import YAML
from typing_extensions import Final

from great_expectations.datasource.fluent.constants import (
    _DATA_ASSET_NAME_KEY,
    _DATASOURCE_NAME_KEY,
    _FLUENT_DATASOURCES_KEY,
)
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
        "loc": (_FLUENT_DATASOURCES_KEY,),
        "msg": "field required",
        "type": "value_error.missing",
    }
]


class GxConfig(FluentBaseModel):
    """Represents the full fluent configuration file."""

    fluent_datasources: List[Datasource] = Field(
        ..., description=_FLUENT_STYLE_DESCRIPTION
    )

    # TODO: <Alex>ALEX</Alex>
    _EXCLUDE_FROM_DATASOURCE_SERIALIZATION: ClassVar[Set[str]] = {
        _DATASOURCE_NAME_KEY,  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }

    _EXCLUDE_FROM_DATA_ASSET_SERIALIZATION: ClassVar[Set[str]] = {
        _DATA_ASSET_NAME_KEY,  # The "name" field is set in validation upon deserialization from configuration key; hence, it should not be serialized.
    }
    # TODO: <Alex>ALEX</Alex>

    class Config:
        extra = Extra.ignore  # ignore any old style config keys

    @property
    def datasources(self) -> List[Datasource]:
        return self.fluent_datasources

    # TODO: <Alex>ALEX</Alex>
    def get_datasources(self) -> Dict[str, Datasource]:
        datasource: Datasource
        return {datasource.name: datasource for datasource in self.fluent_datasources}

    # TODO: <Alex>ALEX</Alex>

    # TODO: <Alex>ALEX</Alex>
    def update_datasources(self, datasources: Dict[str, Datasource]) -> None:
        """
        # TODO: <Alex>ADD_DOCSTRING.</Alex>

        Args:
            datasources:

        """
        # TODO: <Alex>ALEX-FIND_SUPPLIED_AND_EXISTING_DATASOURCES;UPDATE_LIST FROM BOTH.</Alex>
        current_datasources: Dict[str, Datasource] = self.get_datasources()
        current_datasource_names: Set[str] = set(current_datasources.keys())
        updating_datasource_names: Set[str] = set(datasources.keys())
        unaltered_current_datasource_names: Set[str] = (
            current_datasource_names - updating_datasource_names
        )
        common_datasource_names: Set[str] = (
            updating_datasource_names & current_datasource_names
        )
        updating_new_datasource_names: Set[str] = (
            updating_datasource_names - current_datasource_names
        )
        modified_datasource_names: Set[str] = (
            common_datasource_names | updating_new_datasource_names
        )

        datasource: Datasource
        datasource_name: str
        self.fluent_datasources = list(
            filter(
                lambda datasource: datasource.name
                in unaltered_current_datasource_names,
                self.datasources,
            )
        ) + [
            datasources[datasource_name]
            for datasource_name in modified_datasource_names
        ]

    # TODO: <Alex>ALEX</Alex>

    # noinspection PyNestedDecorators
    @validator(_FLUENT_DATASOURCES_KEY, pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: List[dict]):
        print(
            f"\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] THE_V:\n{v} ; TYPE: {str(type(v))}"
        )
        logger.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: List[Datasource] = []

        for config in v:
            print(
                f"\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] DATASOURCE_CONFIG-0:\n{config} ; TYPE: {str(type(config))}"
            )
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
                # TODO: <Alex>ALEX</Alex>
                # config["assets"] = {}
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                config["assets"] = []
                # TODO: <Alex>ALEX</Alex>

            print(
                f"\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] DATASOURCE_TYPE:\n{ds_type} ; TYPE: {str(type(ds_type))}"
            )
            print(
                f"\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] DATASOURCE_CONFIG-1:\n{config} ; TYPE: {str(type(config))}"
            )
            print(
                f'\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] ASSETS-1:\n{config["assets"]} ; TYPE: {str(type(config["assets"]))}'
            )
            datasource = ds_type(**config)
            print(
                f"\n[ALEX_TEST] [GxConfig._load_datasource_subtype()] DATASOURCE:\n{datasource} ; TYPE: {str(type(datasource))}"
            )

            # the ephemeral asset should never be serialized
            # TODO: <Alex>ALEX</Alex>
            if DEFAULT_PANDAS_DATA_ASSET_NAME in datasource.get_assets():
                # TODO: <Alex>ALEX</Alex>
                # datasource.assets.pop(DEFAULT_PANDAS_DATA_ASSET_NAME)
                # TODO: <Alex>ALEX</Alex>
                # TODO: <Alex>ALEX</Alex>
                datasource.delete_asset(asset_name=DEFAULT_PANDAS_DATA_ASSET_NAME)
                # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX</Alex>

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
        In addition, converts datasource and assets configuration sections from dictionary style to list style.
        Other validation errors will still result in an error.

        TODO (kilo59) 122822: remove this as soon as it's no longer needed. Such as when
        we use a new `config_version` instead of `fluent_datasources` key.
        """
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        loaded = yaml.load(f)
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # loaded = cls.load_yaml(f=f)
        # TODO: <Alex>ALEX</Alex>
        print(f"\n[ALEX_TEST] [GxConfig.parse_yaml()] CLS:; TYPE: {str(type(cls))}")
        print(
            f"\n[ALEX_TEST] [GxConfig.parse_yaml()] LOADED_AS_DICT:\n{loaded} ; TYPE: {str(type(loaded))}"
        )
        logger.debug(f"loaded from yaml ->\n{pf(loaded, depth=3)}\n")
        loaded = _convert_fluent_datasources_loaded_from_yaml_to_internal_object_representation(
            config=loaded, _allow_empty=_allow_empty
        )
        print(
            f"\n[ALEX_TEST] [GxConfig.parse_yaml()] LOADED_AS_LIST:\n{loaded} ; TYPE: {str(type(loaded))}"
        )
        if _FLUENT_DATASOURCES_KEY not in loaded:
            return cls(fluent_datasources=[])
        # TODO: <Alex>ALEX</Alex>
        # # noinspection PyArgumentList
        config = cls(**loaded)
        return config
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # config = cls(**loaded)
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # # noinspection PyArgumentList
        # config = super(**loaded)
        # TODO: <Alex>ALEX</Alex>
        # return config
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # if _allow_empty:
        #     try:
        #         super().parse_yaml(f)
        #     except ValidationError as validation_err:
        #         errors_list: List[PydanticErrorDict] = validation_err.errors()
        #         logger.info(
        #             f"{cls.__name__}.parse_yaml() failed with errors - {errors_list}"
        #         )
        #         if errors_list == _MISSING_FLUENT_DATASOURCES_ERRORS:
        #             logger.info(
        #                 f"{cls.__name__}.parse_yaml() returning empty `fluent_datasources`"
        #             )
        #             return cls(fluent_datasources=[])
        #         else:
        #             logger.warning(
        #                 "`_allow_empty` does not prevent unrelated validation errors"
        #             )
        #             raise
        #
        # # TODO: <Alex>ALEX</Alex>
        # a = super().parse_yaml(f)
        # print(f'\n[ALEX_TEST] [GxConfig.parse_yaml()] SUPER().PARSE_YAML(F):\n{a} ; TYPE: {str(type(a))}')
        # return a
        # # TODO: <Alex>ALEX</Alex>
        # # TODO: <Alex>ALEX</Alex>
        # # # noinspection PyTypeChecker
        # # return super().parse_yaml(f)
        # # TODO: <Alex>ALEX</Alex>

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
        print(
            f"\n[ALEX_TEST] [GxConfig.yaml()] INTERMEDIATE_JSON_DICT-0:\n{intermediate_json_dict} ; TYPE: {str(type(intermediate_json_dict))}"
        )
        intermediate_json_dict = self._exclude_name_fields_from_fluent_datasources(
            config=intermediate_json_dict
        )
        print(
            f"\n[ALEX_TEST] [GxConfig.yaml()] INTERMEDIATE_JSON_DICT-1:\n{intermediate_json_dict} ; TYPE: {str(type(intermediate_json_dict))}"
        )
        yaml.dump(intermediate_json_dict, stream=stream_or_path, **yaml_kwargs)

        if isinstance(stream_or_path, pathlib.Path):
            return stream_or_path

        return stream_or_path.getvalue()

    # TODO: <Alex>ALEX</Alex>
    def _exclude_name_fields_from_fluent_datasources(
        self, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        if _FLUENT_DATASOURCES_KEY in config:
            fluent_datasources_config_as_dict = {}

            fluent_datasources: List[dict] = config[_FLUENT_DATASOURCES_KEY]
            print(
                f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] FLUENT_DATASOURCES:\n{fluent_datasources} ; TYPE: {str(type(fluent_datasources))}"
            )

            datasource_name: str
            datasource_config: dict
            for datasource_config in fluent_datasources:
                print(
                    f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] DATASOURCE_CONFIG-0:\n{datasource_config} ; TYPE: {str(type(datasource_config))}"
                )
                datasource_name = datasource_config[_DATASOURCE_NAME_KEY]
                datasource_config = _exclude_fields_from_serialization(
                    source_dict=datasource_config,
                    exclusions=self._EXCLUDE_FROM_DATASOURCE_SERIALIZATION,
                )
                print(
                    f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] DATASOURCE_CONFIG-1:\n{datasource_config} ; TYPE: {str(type(datasource_config))}"
                )
                if "assets" in datasource_config:
                    data_assets: List[dict] = datasource_config["assets"]
                    print(
                        f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] DATA_ASSETS-0:\n{data_assets} ; TYPE: {str(type(data_assets))}"
                    )
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
                    print(
                        f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] DATA_ASSETS-1:\n{data_assets_config_as_dict} ; TYPE: {str(type(data_assets_config_as_dict))}"
                    )
                    datasource_config["assets"] = data_assets_config_as_dict
                    print(
                        f"\n[ALEX_TEST] [GxConfig._exclude_name_fields_from_fluent_datasources()] DATASOURCE_CONFIG-2:\n{datasource_config} ; TYPE: {str(type(datasource_config))}"
                    )

                fluent_datasources_config_as_dict[datasource_name] = datasource_config

            config[_FLUENT_DATASOURCES_KEY] = fluent_datasources_config_as_dict

        return config

    # TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX</Alex>
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


# TODO: <Alex>ALEX</Alex>

# TODO: <Alex>ALEX</Alex>
def _convert_fluent_datasources_loaded_from_yaml_to_internal_object_representation(
    config: Dict[str, Any], _allow_empty: bool = False
) -> Dict[str, Any]:
    if _FLUENT_DATASOURCES_KEY in config:
        fluent_datasources: dict = config[_FLUENT_DATASOURCES_KEY]

        datasource_name: str
        datasource_config: dict
        print(
            f"\n[ALEX_TEST] [GxConfig._convert_fluent_datasources_loaded_from_yaml_to_internal_object_representation()] FLUENT_DATASOURCES:\n{fluent_datasources} ; TYPE: {str(type(fluent_datasources))}"
        )
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


# TODO: <Alex>ALEX</Alex>
