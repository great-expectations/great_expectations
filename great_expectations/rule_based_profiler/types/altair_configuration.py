from typing import Any, Dict, List

from great_expectations.types import ColorPalettes, Colors

# Size
chart_width: int = 800
chart_height: int = 275

# Font
font: str = "Verdana"

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
axis_label_padding: int = 10

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
# Marks
#

# Area
area_fill_color: str = Colors.BLUE_3.value
area_fill_opacity: float = 0.20

#
# Chart Types
#

# Line Chart
line_color: str = Colors.BLUE_1.value
line_stroke_width: int = 3
line_chart_tooltip: bool = True


ALTAIR_CONFIGURATION: Dict[str, Any] = {
    "view": {"width": chart_width, "height": chart_height},
    "font": font,
    "title": {"align": title_align, "color": title_color, "fontSize": title_font_size},
    "selection": {"interval": {"bind": "scales"}},
    "axis": {
        "titleFontSize": axis_title_font_size,
        "titleColor": axis_title_color,
        "labelFontSize": axis_label_font_size,
        "labelColor": axis_label_color,
        "labelPadding": axis_label_padding,
    },
    "axisX": {"labelAngle": x_axis_label_angle, "grid": x_axis_grid},
    "range": {
        "category": category_color_scheme,
        "diverging": diverging_color_scheme,
        "heatmap": heatmap_color_scheme,
        "ordinal": ordinal_color_scheme,
    },
    "mark": {"fill": area_fill_color, "fillOpacity": area_fill_opacity},
    "line": {"strokeWidth": line_stroke_width, "tooltip": line_chart_tooltip},
}
