import warnings

# noinspection PyUnresolvedReferences
from great_expectations.expectations.metrics.table_metric_provider import *

# deprecated-v0.13.25
warnings.warn(
    f"""The module "{__name__}" has been renamed to "{__name__}_provider" -- the alias "{__name__}" will be deprecated \
in the future.
""",
    DeprecationWarning,
    stacklevel=2,
)
