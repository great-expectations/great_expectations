# -*- coding: utf-8 -*-

import logging

from typing import Any

logger = logging.getLogger(__name__)


class Partition(object):
    r"""
    Part help
    """

    # TODO: <Alex>Should we accept **kwargs and set attributes for most generic partition definition?</Alex>
    def __init__(self, name: str, definition: dict, source: Any, data_asset_name: str = None):
        self._name = name
        self._definition = definition
        self._source = source
        self._data_asset_name = data_asset_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def definition(self) -> dict:
        return self._definition

    @property
    def source(self) -> Any:
        return self._source

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Partition):
            return self.name == other.name \
                and self.definition == other.definition \
                and self.data_asset_name == other.data_asset_name
        return False

    def __repr__(self):
        doc_fields_dict: dict = {
            "name": {self.name},
            "definition": self.definition,
            "source": self.source,
            "data_asset_name": self.data_asset_name,
            "type": type(self).__name__
        }
        return str(doc_fields_dict)
