from enum import Enum
from typing import List

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
title_color: str = Colors.GREEN.value
title_dy: int = -15

# Both Axes
axis_title_color: str = Colors.PURPLE.value
axis_title_font_size: int = 14
axis_title_padding: int = 10
axis_label_color: str = Colors.BLUE_1.value
axis_label_font_size: int = 12

# X-Axis Only
x_axis_label_angle: int = 0
x_axis_label_flush: bool = True
x_axis_grid: bool = True
# Known vega-lite bug: https://github.com/vega/vega-lite/issues/5732
# forces us to choose between features "interactive scaling" (below) and "tooltips"
# x_axis_selection_type: str = "interval"
# x_axis_selection_bind: str = "scales"

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

# Area
fill_opacity: float = 0.5
fill_color: str = ColorPalettes.HEATMAP.value[5]

# Line Chart
line_color: str = Colors.BLUE_1.value
line_stroke_width: int = 3
line_opacity: float = 0.9
# Known vega-lite bug: https://github.com/vega/vega-lite/issues/5732
# forces us to choose between features "interactive scaling" and "tooltips" (below)
line_tooltip_content: str = "data"

# Point
point_size: int = 70
point_color: str = Colors.GREEN.value
point_filled: bool = True
point_opacity: float = 1.0
point_tooltip_content: str = "data"


class AltairThemes(Enum):
    # https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration
    DEFAULT_THEME = {
        "view": {"width": chart_width, "height": chart_height},
        "font": font,
        "title": {
            "align": title_align,
            "color": title_color,
            "fontSize": title_font_size,
            "dy": title_dy,
        },
        "axis": {
            "titleFontSize": axis_title_font_size,
            "titleColor": axis_title_color,
            "titlePadding": axis_title_padding,
            "labelFontSize": axis_label_font_size,
            "labelColor": axis_label_color,
        },
        "axisX": {
            "labelAngle": x_axis_label_angle,
            "labelFlush": x_axis_label_flush,
            "grid": x_axis_grid,
        },
        "range": {
            "category": category_color_scheme,
            "diverging": diverging_color_scheme,
            "heatmap": heatmap_color_scheme,
            "ordinal": ordinal_color_scheme,
        },
        "area": {
            "color": fill_color,
            "fillOpacity": fill_opacity,
        },
        "line": {
            "color": line_color,
            "strokeWidth": line_stroke_width,
            "tooltip": {"content": line_tooltip_content},
        },
        "point": {
            "size": point_size,
            "color": point_color,
            "filled": point_filled,
            "opacity": point_opacity,
            "tooltip": {"content": point_tooltip_content},
        },
    }
