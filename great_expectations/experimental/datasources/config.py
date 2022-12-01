"""POC for loading config."""
from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import Dict, Type

from pydantic import validator

from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories

LOGGER = logging.getLogger(__name__)


class GxConfig(ExperimentalBaseModel):
    """Represents the full new-style/experimental configuration file."""

    datasources: Dict[str, Datasource]

    @validator("datasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        LOGGER.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: Dict[str, Datasource] = {}

        # TODO (kilo59): catch key errors
        for ds_name, config in v.items():
            ds_type_name: str = config["type"]
            ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
            LOGGER.debug(f"Instantiating '{ds_name}' as {ds_type}")

            datasource = ds_type(**config)
            loaded_datasources[datasource.name] = datasource

            # TODO: move this to a different 'validator' method
            # attach the datasource to the nested assets, avoiding recursion errors
            for asset in datasource.assets.values():
                asset._datasource = datasource

        LOGGER.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")
        return loaded_datasources
