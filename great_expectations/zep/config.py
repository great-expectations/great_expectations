"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Dict, Type, Union

from pydantic import BaseModel, validator
from ruamel.yaml import YAML

from great_expectations.zep.interfaces import Datasource
from great_expectations.zep.sources import _SourceFactories

yaml = YAML(typ="safe")


LOGGER = logging.getLogger(__name__.lstrip("great_expectations."))


class GxConfig(BaseModel):
    datasources: Dict[str, Datasource]

    @classmethod
    def parse_yaml(cls, f: Union[pathlib.Path, str]) -> GxConfig:
        loaded = yaml.load(f)
        LOGGER.debug(f"loaded from yaml ->\n{pf(loaded)}\n")
        return cls(**loaded)

    @validator("datasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        LOGGER.info(f"Loading 'datasources' ->\n{pf(v, depth=3)}")
        loaded_datasources: Dict[str, Datasource] = {}

        # TODO (kilo59): catch key errors
        for ds_name, config in v.items():
            ds_type_name: str = config["type"]
            ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
            LOGGER.debug(f"Instantiating '{ds_name}' as {ds_type}")
            loaded_datasources[ds_type_name] = ds_type(**config)

        LOGGER.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")
        return loaded_datasources
