from dataclasses import dataclass
from typing import List

import altair as alt


@dataclass
class Chart:
    table_domain_charts: List[alt.Chart]
    column_domain_chart: alt.VConcatChart
