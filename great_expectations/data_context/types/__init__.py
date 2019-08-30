from ...types import AllowedKeysDotDict
from collections import namedtuple
from six import string_types

from .configurations import (
    DataContextConfig
)
from .resource_identifiers import (
    DataContextResourceIdentifier,
    DataAssetIdentifier,
    BatchIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)

# TODO: Deprecate this in favor of DataAssetIdentifier
NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])