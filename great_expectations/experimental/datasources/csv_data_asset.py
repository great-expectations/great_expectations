from __future__ import annotations

import logging

from typing_extensions import Literal

from great_expectations.experimental.datasources.filesystem_data_asset import (
    FilesystemDataAsset,
)

LOGGER = logging.getLogger(__name__)


class CsvDataAsset(FilesystemDataAsset):
    # Overridden inherited instance fields
    type: Literal["csv"] = "csv"
