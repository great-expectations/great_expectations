# -*- coding: utf-8 -*-

import logging

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
    def name(self):
        return self._name

    # TODO : see if this can actually be reused
    def get_available_partitions(self, **kwargs):
        raise NotImplementedError

    # TODO : see if this can actually be reused
    def get_available_partition_keys(self, **kwargs):
        raise NotImplementedError

    # TODO : see if this can actually be reused
    def get_available_partition_definitions(self, **kwargs):
        raise NotImplementedError


