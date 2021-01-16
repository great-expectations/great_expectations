import logging
from typing import List

logger = logging.getLogger(__name__)


class Asset:
    """
    A typed data asset class that maintains data asset specific properties (to override data connector level proprties with
    the same name and/or semantics, such as "partitioner_name", "base_directory", and "glob_directive").
    """

    def __init__(
        self,
        name: str,
        base_directory: str = None,
        glob_directive: str = None,
        pattern: str = None,
        group_names: List[str] = None,
        bucket: str = None,
        prefix: str = None,
        delimiter: str = None,
        max_keys: int = None,
        batch_spec_passthrough: dict = None,
    ):
        self._name = name
        self._base_directory = base_directory
        self._glob_directive = glob_directive
        self._pattern = pattern

        # Note: this may need to become a nested object to accomodate sorters
        self._group_names = group_names

        self._bucket = bucket
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_keys = max_keys
        self._batch_spec_passthrough = batch_spec_passthrough or {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def base_directory(self) -> str:
        return self._base_directory

    @property
    def glob_directive(self) -> str:
        return self._glob_directive

    @property
    def pattern(self) -> str:
        return self._pattern

    @property
    def group_names(self) -> List[str]:
        return self._group_names

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def delimiter(self) -> str:
        return self._delimiter

    @property
    def max_keys(self) -> int:
        return self._max_keys

    @property
    def batch_spec_passthrough(self) -> dict:
        return self._batch_spec_passthrough
