# -*- coding: utf-8 -*-

import logging

from typing import List, Iterable
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)


class Partitioner(object):
    r"""
    Partitioners help
    """

    _batch_spec_type = BatchSpec  #TODO : is this really needed?
    recognized_batch_definition_keys = {
        "regex",
        "sorters"
    }

    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def get_available_partitions(self, **kwargs) -> Iterable[Partition]:
        raise NotImplementedError

    def get_available_partition_names(self, **kwargs) -> List[str]:
        raise NotImplementedError

    def get_partition(self, partition_name: str) -> Partition:
        raise NotImplementedError


