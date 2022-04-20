from enum import Enum

import altair as alt


class AltairDataTypes(Enum):
    QUANTITATIVE = alt.StandardType("quantitative")
    ORDINAL = alt.StandardType("ordinal")
    NOMINAL = alt.StandardType("nominal")
    TEMPORAL = alt.StandardType("temporal")
