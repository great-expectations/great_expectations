
from dataclasses import dataclass
from typing import List
import altair as alt

@dataclass(frozen=True)
class PlotResult():
    'Wrapper object around DataAssistantResult plotted Altair charts.\n\n    Please note that contained within this object are the raw Altair charts generated\n    by `DataAssistantResult.plot()`. They may have been concatenated or formatted for\n    purposes of display in Jupyter Notebooks.\n\n    Attributes:\n        charts: The list of Altair charts rendered through `DataAssistantResult.plot()`\n    '
    charts: List[alt.Chart]
