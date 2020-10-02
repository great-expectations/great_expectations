# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class PartitionSpec(object):
    RECOGNIZED_PARTITION_SPECIFICATION_KEYS: set = {
        "name",
        "data_asset_name",
        "definition"
    }

    def __init__(self, name: str = None, data_asset_name: str = None, definition: dict = None):
        self._name = name
        self._data_asset_name = data_asset_name
        self._definition = definition

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def definition(self) -> dict:
        return self._definition

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, PartitionSpec):
            return self.name == other.name \
                and self.data_asset_name == other.data_asset_name \
                and self.definition == other.definition
        return False

    def __hash__(self) -> int:
        return (
            hash(self.name) ^
            hash(self.data_asset_name) ^
            hash(zip(self.definition.items()))
        )

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "data_asset_name": self.data_asset_name,
            "definition": self.definition
        }
        return str(doc_fields_dict)
