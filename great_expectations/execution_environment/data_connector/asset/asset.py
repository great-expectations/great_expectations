# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class Asset(object):
    def __init__(
        self,
        name: str,
        partitioner: str = None,
        base_directory: str = None,
        glob_directive: str = None
    ):
        self._name = name
        self._partitioner = partitioner
        self._base_directory = base_directory
        self._glob_directive = glob_directive

    @property
    def name(self) -> str:
        return self._name

    @property
    def partitioner(self) -> str:
        return self._partitioner

    @property
    def base_directory(self) -> str:
        return self._base_directory

    @property
    def glob_directive(self) -> str:
        return self._glob_directive
