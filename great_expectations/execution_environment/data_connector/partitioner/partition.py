# -*- coding: utf-8 -*-

import logging

from typing import Any

logger = logging.getLogger(__name__)


class Partition(object):
    r"""
    Part help
    """

    # TODO: <Alex>Should we accept **kwargs and set attributes for most generic partition definition?</Alex>
    def __init__(self, name: str, definition: dict, source: Any):
        self._name = name
        self._definition = definition
        self._source = source

    @property
    def name(self) -> str:
        return self._name

    @property
    def definition(self) -> dict:
        return self._definition

    @property
    def source(self) -> Any:
        return self._source

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Partition):
            return self.name == other.name and self.definition == other.definition
        return False

    def __repr__(self):
        doc_fields_dict: dict = {
            "name": {self.name},
            "definition": self.definition,
            "source": self.source,
            "type": type(self).__name__
        }
        return str(doc_fields_dict)
