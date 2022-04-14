from typing import Dict, List, Union

from great_expectations.types import ColorPalettes, Colors

chart_width: int = 800
chart_height: int = 300
title_color: str = Colors.PURPLE.value
x_axis_label_color: str = Colors.PURPLE.value
y_axis_label_color: str = Colors.PURPLE.value
category_color_scheme: List[str] = ColorPalettes.CATEGORY.value

ALTAIR_CONFIGURATION: Dict[str, Union[str, Dict]] = {
    "view": {"width": chart_width},
    "title": {"align": "center", "color": title_color, "fontSize": 14},
    "axisX": {"labelColor": x_axis_label_color},
    "axisY": {"labelColor": y_axis_label_color},
    "range": {"category": category_color_scheme},
}
