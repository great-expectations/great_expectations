# -*- coding: utf-8 -*-

import logging

from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)


class Sorter(object):
    r"""
    Partition help
    """

    # _batch_spec_type = BatchSpec  #TODO : is this really needed?
    # recognized_batch_definition_keys = {  #TODO : is this really needed?
    #     "regex",
    #     "partitions"
    # }

    def __init__(self, name, **kwargs):
        self._name = name
        orderby = kwargs.find("orderby")
        if orderby:
            self._orderby = orderby

    @property
    def name(self):
        return self._name

    @property
    def orderby(self):
        return self._orderby

    # # TODO : see if this can actually be reused
    # def get_available_partitions(self, **kwargs):
    #     raise NotImplementedError
    #
    # # TODO : see if this can actually be reused
    # def get_available_partition_names(self, **kwargs):
    #     raise NotImplementedError
    #
    # # TODO : see if this can actually be reused
    # def get_available_partition_definitions(self, **kwargs):
    #     raise NotImplementedError
