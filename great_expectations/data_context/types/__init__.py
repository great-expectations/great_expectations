from collections import namedtuple
from .base import (
    NormalizedDataAssetName,
)

from .metrics import (
    Metric,
    NamespaceAwareValidationMetric
)

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

# # TODO: Deprecate this in favor of DataAssetIdentifier
# NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
#     "datasource",
#     "generator",
#     "generator_asset"
# ])