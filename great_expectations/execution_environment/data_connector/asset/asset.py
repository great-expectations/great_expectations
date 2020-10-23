# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class Asset(object):
    """
    A typed data asset class that maintains data asset specific properties (to override data connector level proprties with
    the same name and/or semantics, such as "partitioner_name", "base_directory", and "glob_directive").
    """

    def __init__(
        self,
        name: str,
        partitioner_name: str = None,
        base_directory: str = None,
        glob_directive: str = None
    ):
        self._name = name
        self._partitioner_name = partitioner_name
        self._base_directory = base_directory
        self._glob_directive = glob_directive

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
