"""POC for loading config."""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import Dict, Type

from pydantic import Extra, Field, validator

from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)


_ZEP_STYLE_DESCRIPTION = "ZEP Experimental Datasources"


class GxConfig(ExperimentalBaseModel):
    """Represents the full new-style/experimental configuration file."""

    datasources: Dict[str, Datasource] = Field(..., description=_ZEP_STYLE_DESCRIPTION)

    class Config:
        extra = Extra.ignore  # ignore any old style config keys

    @validator("datasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        LOGGER.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: Dict[str, Datasource] = {}

        for ds_name, config in v.items():
            ds_type_name: str = config.get("type", "")
            if not ds_type_name:
                # TODO: (kilo59 122222) ideally this would be raised by `Datasource` validation
                # https://github.com/pydantic/pydantic/issues/734
                raise ValueError(f"'{ds_name}' is missing a 'type' entry")

            try:
                ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
                LOGGER.debug(f"Instantiating '{ds_name}' as {ds_type}")
            except KeyError as type_lookup_err:
                raise ValueError(
                    f"'{ds_name}' has unsupported 'type' - {type_lookup_err}"
                ) from type_lookup_err

            datasource = ds_type(**config)
            loaded_datasources[datasource.name] = datasource

            # TODO: move this to a different 'validator' method
            # attach the datasource to the nested assets, avoiding recursion errors
            for asset in datasource.assets.values():
                asset._datasource = datasource

        LOGGER.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")

        if v and not loaded_datasources:
            raise ValueError(f"Of {len(v)} entries, no 'datasources' could be loaded")
        return loaded_datasources
