# -*- coding: utf-8 -*-

from typing import List
import logging

logger = logging.getLogger(__name__)


class Asset:
    """
    A typed data asset class that maintains data asset specific properties (to override data connector level proprties with
    the same name and/or semantics, such as "partitioner_name", "base_directory", and "glob_directive").
    """

    def __init__(
        self,
        name: str,
        partitioner_name: str = None,
        base_directory: str = None,
        glob_directive: str = None,
        pattern: str = None,
        group_names: List[str] = None,
    ):
        self._name = name
        self._base_directory = base_directory
        self._glob_directive = glob_directive
        self._pattern = pattern

        # Note: this may need to become a nested object to accomodate sorters
        self._group_names = group_names

    @property
    def name(self) -> str:
        return self._name

    @property
    def partitioner_name(self) -> str:
        return self._partitioner_name

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
    def group_names(self) -> str:
        return self._group_names
