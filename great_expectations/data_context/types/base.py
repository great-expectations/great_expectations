from collections import namedtuple
from six import string_types

from great_expectations.types import AllowedKeysDotDict

NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])