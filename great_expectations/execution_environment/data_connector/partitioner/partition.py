# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class Partition(object):
    r"""
    Part help
    """

    # TODO: <Alex>Should we accept **kwargs and set attributes for most generic partition definition?</Alex>
    def __init__(self, name: str, definition: dict):
        self._name = name
        self._definition = definition

    @property
    def name(self) -> str:
        return self._name

    @property
    def definition(self) -> dict:
        return self._definition

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Partition):
            return self.name == other.name and self.definition == other.definition
        return False
