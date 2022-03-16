import warnings

# noinspection PyUnresolvedReferences
from great_expectations.expectations.metrics.table_metric_provider import *

# deprecated-v0.13.25
warnings.warn(
    f"""The module "{__name__}" has been renamed to "{__name__}_provider" -- the alias "{__name__}" is deprecated \
as of v0.13.25 and will be removed in a future release.
""",
    DeprecationWarning,
    stacklevel=2,
)
