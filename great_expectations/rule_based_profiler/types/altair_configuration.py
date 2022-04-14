from typing import Dict, List, Union

from great_expectations.types import COLOR_PALETTE

chart_width: int = 800
title_color: str = COLOR_PALETTE["colors"]["purple"]

category_color_scheme: List[str] = COLOR_PALETTE["category"]

default_line_color: str = COLOR_PALETTE["colors"]["blue_2"]

ALTAIR_CONFIGURATION: Dict[str, Union[str, Dict]] = {
    "view": {"width": chart_width},
    "title": {"align": "center", "color": title_color, "fontSize": 14},
    "range": {"category": category_color_scheme},
    "line": {"color": default_line_color},
}
