# -*- coding: utf-8 -*-

from typing import Iterable

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

logger = logging.getLogger(__name__)


class Sorter(object):
    r"""
    Sorter help
    """

    def __init__(self, name: str, **kwargs):
        self._name = name
        orderby: str = kwargs.find("orderby")
        if orderby:
            self._orderby = orderby

    def get_sorted_partitions(self, partitions: Iterable[Partition]) -> Iterable[Partition]:
        pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def orderby(self) -> str:
        return self._orderby
