from dataclasses import dataclass
from enum import Enum
from typing import List

import altair as alt


class PlotMode(Enum):
    DESCRIPTIVE = "descriptive"
    PRESCRIPTIVE = "prescriptive"
    DIAGNOSTIC = "diagnostic"


@dataclass(frozen=True)
class PlotResult:
    """Wrapper object around DataAssistantResult plotted Altair charts.

    Please note that contained within this object are the raw Altair charts generated
    by `DataAssistantResult.plot()`. They may have been concatenated or formatted for
    purposes of display in Jupyter Notebooks.

    Attributes:
        charts: The list of Altair charts rendered through `DataAssistantResult.plot()`
    """

    charts: List[alt.Chart]

    def __repr__(self):
        return ""
