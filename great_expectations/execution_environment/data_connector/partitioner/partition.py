# -*- coding: utf-8 -*-

from typing import Any
import logging

from great_expectations.core.id_dict import PartitionDefinition

logger = logging.getLogger(__name__)


class Partition(object):
    def __init__(
        self,
        name: str = None,
        data_asset_name: str = None,
        definition: PartitionDefinition = None,
        data_reference: Any = None
    ):
        self._name = name
        self._data_asset_name = data_asset_name
        self._definition = definition
        self._data_reference = data_reference

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def definition(self) -> PartitionDefinition:
        return self._definition

    @property
    def data_reference(self) -> Any:
        return self._data_reference

    @data_reference.setter
    def data_reference(self, data_reference: Any):
        self._data_reference = data_reference

    def __eq__(self, other):
        """Overrides the default implementation"""
        return (
            isinstance(other, Partition) and
            self.name == other.name and
            self.data_asset_name == other.data_asset_name and
            self.definition == other.definition
        )

    def __hash__(self) -> int:
        """Overrides the default implementation"""
        _result_hash: int = hash(self.name) ^ hash(self.data_asset_name)
        if self.definition is not None:
            for key, value in self.definition.items():
                _result_hash = _result_hash ^ hash(key) ^ hash(str(value))
        return _result_hash

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "name": self.name,
            "data_asset_name": self.data_asset_name,
            "definition": self.definition,
            "data_reference": self.data_reference
        }
        return str(doc_fields_dict)
