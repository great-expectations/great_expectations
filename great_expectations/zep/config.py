"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Dict, Union

from pydantic import BaseModel
from ruamel.yaml import YAML

from great_expectations.zep import interfaces

yaml = YAML(typ="safe")

Datasource = interfaces.Datasource.Datasource

LOGGER = logging.getLogger(__name__)


class GxConfig(BaseModel):
    datasources: Dict[str, Datasource]

    @classmethod
    def parse_yaml(cls, f: Union[pathlib.Path, str]) -> GxConfig:
        loaded = yaml.load(f)
        LOGGER.debug(f"loaded from yaml ->\n{pf(loaded)}\n")
        return cls(**loaded)
