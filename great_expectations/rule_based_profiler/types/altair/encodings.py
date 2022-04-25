from enum import Enum

import altair as alt


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
