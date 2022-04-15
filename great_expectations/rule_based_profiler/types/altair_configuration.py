from typing import Dict, List, Union

from great_expectations.types import ColorPalettes, Colors

chart_width: int = 800
chart_height: int = 300
title_align: str = "center"
title_font_size: int = 14
title_color: str = Colors.PURPLE.value
axis_title_color: str = Colors.PURPLE.value
axis_label_color: str = Colors.BLUE_1.value
category_color_scheme: List[str] = ColorPalettes.CATEGORY.value


ALTAIR_CONFIGURATION: Dict[str, Union[str, Dict]] = {
    "view": {"width": chart_width, "height": chart_height},
    "title": {"align": title_align, "color": title_color, "fontSize": title_font_size},
    "axis": {"titleColor": axis_title_color, "labelColor": axis_label_color},
    "axisX": {"labelAngle": 90},
    "range": {"category": category_color_scheme},
}
