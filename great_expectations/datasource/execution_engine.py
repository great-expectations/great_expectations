# -*- coding: utf-8 -*-

import copy
import logging
import warnings

from ruamel.yaml import YAML

from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.types import ClassConfig

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class ExecutionEngine(object):
    pass
