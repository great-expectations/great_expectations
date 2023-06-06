from __future__ import annotations

import re
from typing import Final

# these fields must be added to `__fields_set__` before pydantic model serialization
# methods are called. Otherwise it could be excluded.
# https://docs.pydantic.dev/usage/exporting_models/#modeldict
_FIELDS_ALWAYS_SET: Final[set[str]] = {
    "type",
}

_FLUENT_DATASOURCES_KEY: Final[str] = "fluent_datasources"
_DATASOURCE_NAME_KEY: Final[str] = "name"
_ASSETS_KEY: Final[str] = "assets"
_DATA_ASSET_NAME_KEY: Final[str] = "name"

_DATA_CONNECTOR_NAME: Final[str] = "fluent"

MATCH_ALL_PATTERN: Final[re.Pattern] = re.compile(".*")

DEFAULT_PANDAS_DATASOURCE_NAME: Final[str] = "default_pandas_datasource"

DEFAULT_PANDAS_DATA_ASSET_NAME: Final[str] = "#ephemeral_pandas_asset"
