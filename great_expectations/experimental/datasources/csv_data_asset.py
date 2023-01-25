from __future__ import annotations

import logging
import pathlib
import re
from typing import Optional, Union

from typing_extensions import Literal

from great_expectations.experimental.datasources.filesystem_data_asset import (
    FilesystemDataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
)

LOGGER = logging.getLogger(__name__)


class CsvDataAsset(FilesystemDataAsset):
    # Overridden inherited instance fields
    type: Literal["csv"] = "csv"

    def __init__(
        self,
        name: str,
        base_directory: pathlib.Path,
        regex: Union[str, re.Pattern],
        order_by: Optional[BatchSortersDefinition] = None,
    ):
        """Constructs a "CSV" type filesystem "DataAsset" object.

        Args:
            name: The name of the present File Path data asset
            base_directory: base directory path, relative to which file paths will be collected
            regex: regex pattern that matches filenames and whose groups are used to label the Batch samples
            order_by: one of "asc" (ascending) or "desc" (descending) -- the method by which to sort "Asset" parts.
        """
        super().__init__(
            name=name,
            base_directory=base_directory,
            regex=regex,
            order_by=order_by,
        )
