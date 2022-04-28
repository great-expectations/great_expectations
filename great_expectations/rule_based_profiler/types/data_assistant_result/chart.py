from dataclasses import dataclass
from typing import List

import altair as alt


@dataclass(frozen=True)
class Chart:
    """Wrapper object around DataAssistantResult plotted Altair charts.

    Attributes:
        table_domain_charts: A list of charts (each of which corresponds to a single table)
        column_domain_chart: A single vertically concatenated chart containing all columns.
    """

    table_domain_charts: List[alt.Chart]
    column_domain_chart: alt.VConcatChart
