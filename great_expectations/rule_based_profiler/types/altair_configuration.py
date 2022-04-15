from typing import Any, Dict, List

from great_expectations.types import ColorPalettes, Colors

# Size
chart_width: int = 800
chart_height: int = 275

#
# Chart Components
#

# Title
title_align: str = "center"
title_font_size: int = 16
title_color: str = Colors.PURPLE.value

# Both Axes
axis_title_color: str = Colors.PURPLE.value
axis_title_font_size: int = 14
axis_label_color: str = Colors.BLUE_1.value
axis_label_font_size: int = 12

# X-Axis Only
x_axis_label_angle: int = 0
x_axis_grid: bool = True

# Y-Axis Only

#
# Color Palettes
#

category_color_scheme: List[str] = ColorPalettes.CATEGORY.value
diverging_color_scheme: List[str] = ColorPalettes.DIVERGING.value
heatmap_color_scheme: List[str] = ColorPalettes.HEATMAP.value
ordinal_color_scheme: List[str] = ColorPalettes.ORDINAL.value

#
# Chart Types
#

# Line Chart
line_chart_tooltip: bool = True


ALTAIR_CONFIGURATION: Dict[str, Any] = {
    "view": {"width": chart_width, "height": chart_height},
    "title": {"align": title_align, "color": title_color, "fontSize": title_font_size},
    "axis": {
        "titleFontSize": axis_title_font_size,
        "titleColor": axis_title_color,
        "labelFontSize": axis_label_font_size,
        "labelColor": axis_label_color,
    },
    "axisX": {"labelAngle": x_axis_label_angle, "grid": x_axis_grid},
    "range": {
        "category": category_color_scheme,
        "diverging": diverging_color_scheme,
        "heatmap": heatmap_color_scheme,
        "ordinal": ordinal_color_scheme,
    },
    "line": {"tooltip": line_chart_tooltip},
}
