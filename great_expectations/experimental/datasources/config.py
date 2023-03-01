"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Type, Union

from pydantic import Extra, Field, PrivateAttr, ValidationError, validator
from ruamel.yaml import YAML
from typing_extensions import Final

from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.interfaces import (
    Datasource,  # noqa: TCH001
)
from great_expectations.experimental.datasources.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    _SourceFactories,
)

if TYPE_CHECKING:
    from pydantic.error_wrappers import ErrorDict as PydanticErrorDict

    from great_expectations.core.config_provider import _ConfigurationProvider

logger = logging.getLogger(__name__)


yaml = YAML(typ="safe")
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

_ZEP_STYLE_DESCRIPTION: Final[str] = "ZEP Experimental Datasources"

_MISSING_XDATASOURCES_ERRORS: Final[List[PydanticErrorDict]] = [
    {
        "loc": ("xdatasources",),
        "msg": "field required",
        "type": "value_error.missing",
    }
]


class _ConfigsTuple(NamedTuple):
    # the full great_expectations.yml config with config values substituted
    substituted: Dict[str, Any]
    # only the `xdatasources` portion of the config with no config substitutions
    xdatasources_raw: Dict[str, Any]


class GxConfig(ExperimentalBaseModel):
    """Represents the full new-style/experimental configuration file."""

    xdatasources: Dict[str, Datasource] = Field(..., description=_ZEP_STYLE_DESCRIPTION)

    # private non-field attributes
    _xdatasources_raw_config: Dict[str, Any] = PrivateAttr(default_factory=dict)

    @property
    def datasources(self) -> Dict[str, Datasource]:
        return self.xdatasources

    class Config:
        extra = Extra.ignore  # ignore any old style config keys

    @validator("xdatasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        logger.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: Dict[str, Datasource] = {}

        for ds_name, config in v.items():
            ds_type_name: str = config.get("type", "")
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
        cls: Type[GxConfig],
        f: Union[pathlib.Path, str],
        _allow_empty: bool = False,
        _config_provider: _ConfigurationProvider | None = None,
    ) -> GxConfig:
        """
        Overriding base method to allow an empty/missing `xdatasources` field.
        Other validation errors will still result in an error.

        TODO (kilo59) 122822: remove this as soon as it's no longer needed. Such as when
        we use a new `config_version` instead of `xdatasources` key.
        """
        config: dict[str, Any]
        xdatasources_raw_config: dict[str, Any] | None = None
        if _config_provider:
            config_tuple = cls._load_yaml_with_config_substitutions(f, _config_provider)
            config = config_tuple.substituted
            xdatasources_raw_config = config_tuple.xdatasources_raw
        else:
            config = yaml.load(f)

        if _allow_empty:
            try:
                cls(**config)
            except ValidationError as validation_err:
                errors_list: List[PydanticErrorDict] = validation_err.errors()
                logger.info(
                    f"{cls.__name__}.parse_yaml() failed with errors - {errors_list}"
                )
                if errors_list == _MISSING_XDATASOURCES_ERRORS:
                    logger.info(
                        f"{cls.__name__}.parse_yaml() returning empty `xdatasources`"
                    )
                    return cls(xdatasources={})
                else:
                    logger.info(
                        "`_allow_empty` does not prevent unrelated validation errors"
                    )
                    raise
        instance = cls(**config)
        if xdatasources_raw_config:
            instance._xdatasources_raw_config = xdatasources_raw_config
        return instance

    @classmethod
    def _load_yaml_with_config_substitutions(
        cls: Type[GxConfig],
        f: Union[pathlib.Path, str],
        _config_provider: _ConfigurationProvider,
    ) -> _ConfigsTuple:
        raw_config: dict = yaml.load(f)
        logger.debug(f"Substituting config with {_config_provider.__class__.__name__}")
        return _ConfigsTuple(
            _config_provider.substitute_config(raw_config),
            xdatasources_raw=raw_config.get("xdatasources", {}),
        )
