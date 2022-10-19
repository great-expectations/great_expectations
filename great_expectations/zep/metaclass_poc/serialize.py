from __future__ import annotations

import pathlib
from pprint import pformat as pf
from pprint import pprint as pp
from typing import Dict, Union, Optional

from pydantic import BaseModel
from ruamel.yaml import YAML
from typing_extensions import Literal

yaml = YAML(typ="safe")


# Base Models


class Asset(BaseModel):
    type: str


class Datasource(BaseModel):
    engine: str
    version: float
    assets: Dict[str, Asset]


# End Base Models


class FileAsset(Asset):
    type: Literal["file"] = "file"
    base_directory: pathlib.Path
    seperators: str = ","
    has_headers: bool


class SQLAsset(Asset):
    type: Literal["sql"] = "sql"
    query: str


class TableAsset(Asset):
    type: Literal["table"] = "table"
    table_name: str
    limit: Optional[int] = None


AssetTypes = Union[FileAsset, SQLAsset, TableAsset]


class PandasDatasource(Datasource):
    engine: Literal["pandas"]
    version: float
    assets: Dict[str, AssetTypes]


class PostgresDatasource(Datasource):
    engine: Literal["postgres"]


DatasourceTypes = Union[PandasDatasource, PostgresDatasource]


class GXConfig(BaseModel):
    datasources: Dict[str, DatasourceTypes]

    @classmethod
    def parse_yaml(cls, f: Union[pathlib.Path, str]) -> GXConfig:
        loaded = yaml.load(f)
        print(f"loaded from yaml ->\n{pf(loaded)}\n")
        return cls.parse_obj(loaded)


if __name__ == "__main__":
    parent_dir = pathlib.Path(__file__).parent
    yaml_file = parent_dir / "config.yaml"
    assert yaml_file.exists(), yaml_file

    config = GXConfig.parse_yaml(yaml_file)
    print(f"{type(config)}\n{config}")
