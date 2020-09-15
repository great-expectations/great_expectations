# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


class Sorter(object):
    r"""
    Sorter help
    """

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
