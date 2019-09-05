from collections import namedtuple

# TODO: Deprecate this in favor of DataAssetIdentifier
NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])