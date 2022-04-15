from typing import Any, Dict, List

from great_expectations.types import ColorPalettes, Colors

# Size
chart_width: int = 800
chart_height: int = 250

# Font
font: str = "Verdana"

#
# Chart Components
#

# Title
title_align: str = "center"
title_font_size: int = 15
title_color: str = Colors.PURPLE.value

# Both Axes
axis_title_color: str = Colors.PURPLE.value
axis_title_font_size: int = 14
axis_title_padding: int = 10
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
# Marks
#

# Area
area_fill_color: str = ColorPalettes.HEATMAP.value[5]
area_fill_opacity: float = 0.9

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
    "axis": {
        "titleFontSize": axis_title_font_size,
        "titleColor": axis_title_color,
        "titlePadding": axis_title_padding,
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
    "mark": {"fill": area_fill_color, "fillOpacity": area_fill_opacity},
    "line": {"strokeWidth": line_stroke_width, "tooltip": line_chart_tooltip},
}
