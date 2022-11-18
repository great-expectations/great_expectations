"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Dict, Type, Union

from pydantic import BaseModel, validator
from ruamel.yaml import YAML

from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories

yaml = YAML(typ="safe")


LOGGER = logging.getLogger(__name__)


class GxConfig(BaseModel):
    datasources: Dict[str, Datasource]

    @classmethod
    def parse_yaml(cls, f: Union[pathlib.Path, str]) -> GxConfig:
        loaded = yaml.load(f)
        LOGGER.debug(f"loaded from yaml ->\n{pf(loaded, depth=3)}\n")
        config = cls(**loaded)
        return config

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
            for asset in datasource.assets.values():
                asset._datasource = datasource

        LOGGER.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")
        return loaded_datasources
