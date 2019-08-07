from .base import (
    ListOf,
    LooselyTypedDotDict,
)

class Expectation(LooselyTypedDotDict):
    _allowed_keys = set([
        "results",
        "meta",
    ])
    _required_keys = set([
        "results",
        "meta",
    ])
    _key_types = {}

class ExpectationSuite(LooselyTypedDotDict):
    _allowed_keys = set([
        "results",
        "meta",
    ])
    _required_keys = set([
        "results",
        "meta",
    ])
    _key_types = {}

class ValidationResult(LooselyTypedDotDict):
    _allowed_keys = set([
        "expectation_config",
    ])
    _required_keys = set([
    ])
    _key_types = {}

class ValidationResultSuite(LooselyTypedDotDict):
    _allowed_keys = set([
        "results",
        "meta",
        "success",
        "statistics", #TODO: Someday we should deprecate this in favor of renderers.
    ])
    _required_keys = set([
        "results",
        "meta",
        "success"
    ])
    _key_types = {
        "results" : ListOf(ValidationResult),
        "meta" : dict,
        "success" : bool,
    }