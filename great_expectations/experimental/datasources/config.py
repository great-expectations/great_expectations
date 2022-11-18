"""POC for loading config."""
from __future__ import annotations

import json
import logging
import pathlib
from io import StringIO
from pprint import pformat as pf
from typing import Dict, Type, Union, overload

from pydantic import BaseModel, validator
from ruamel.yaml import YAML

from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories

yaml = YAML(typ="safe")
# NOTE (kilo59): the following settings appear to be what we use in existing codebase
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


LOGGER = logging.getLogger(__name__)

# TODO (kilo59): should we move yaml parsing/dumping to ExperimentalBase??


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
            # attach the datasource to the nested assets, avoiding recursion errors
            for asset in datasource.assets.values():
                asset._datasource = datasource

        LOGGER.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")
        return loaded_datasources

    @overload
    def yaml(self, stream_or_path: Union[StringIO, None] = None, **yaml_kwargs) -> str:
        ...

    @overload
    def yaml(self, stream_or_path: pathlib.Path, **yaml_kwargs) -> pathlib.Path:
        ...

    def yaml(
        self, stream_or_path: Union[StringIO, pathlib.Path, None] = None, **yaml_kwargs
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
        intermediate_json = json.loads(self.json())
        yaml.dump(intermediate_json, stream=stream_or_path, **yaml_kwargs)

        if isinstance(stream_or_path, pathlib.Path):
            return stream_or_path
        return stream_or_path.getvalue()
