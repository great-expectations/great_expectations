import warnings
from enum import Enum

import altair as alt

# Once the context manager exits, the warning filter is removed.
# Do not remove this context-manager.
# https://docs.python.org/3/library/warnings.html#temporarily-suppressing-warnings
# DeprecationWarning: jsonschema.RefResolver is deprecated as of v4.18.0
# https://github.com/altair-viz/altair/issues/3097
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=DeprecationWarning)

    class AltairDataTypes(Enum):
        # available data types: https://altair-viz.github.io/user_guide/encoding.html#encoding-data-types
        QUANTITATIVE = alt.StandardType("quantitative")
        ORDINAL = alt.StandardType("ordinal")
        NOMINAL = alt.StandardType("nominal")
        TEMPORAL = alt.StandardType("temporal")

    class AltairAggregates(Enum):
        # available aggregates: https://altair-viz.github.io/user_guide/encoding.html#encoding-channel-options
        MEAN = alt.Aggregate("mean")
        MEDIAN = alt.Aggregate("median")
        MIN = alt.Aggregate("min")
        MAX = alt.Aggregate("max")
        COUNT = alt.Aggregate("count")
        SUM = alt.Aggregate("sum")
