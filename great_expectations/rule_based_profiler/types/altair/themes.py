from enum import Enum
from typing import List

from great_expectations.types import ColorPalettes, Colors

chart_width: int = 800
chart_height: int = 250
chart_border_opacity: float = 0
font: str = "Verdana"
title_align: str = "center"
title_font_size: int = 15
title_color: str = Colors.GREEN.value
title_dy: int = -10
subtitle_color: str = Colors.PURPLE.value
subtitle_font: str = font
subtitle_font_size: int = 12
subtitle_font_weight: str = "bold"
subtitle_padding: int = 5
axis_title_color: str = Colors.PURPLE.value
axis_title_font_size: int = 14
axis_title_padding: int = 10
axis_label_color: str = Colors.BLUE_1.value
axis_label_font_size: int = 12
x_axis_label_angle: int = 0
x_axis_label_flush: bool = True
x_axis_grid: bool = True
legend_title_color: str = Colors.PURPLE.value
legend_title_font_size: str = 12
category_color_scheme: List[str] = ColorPalettes.CATEGORY_5.value
diverging_color_scheme: List[str] = ColorPalettes.DIVERGING_7.value
heatmap_color_scheme: List[str] = ColorPalettes.HEATMAP_6.value
ordinal_color_scheme: List[str] = ColorPalettes.ORDINAL_7.value
fill_opacity: float = 0.5
fill_color: str = ColorPalettes.HEATMAP_6.value[5]
line_color: str = Colors.BLUE_2.value
line_stroke_width: int = 3
line_opacity: float = 0.9
line_tooltip_content: str = "data"
point_size: int = 70
point_color: str = Colors.GREEN.value
point_filled: bool = True
point_opacity: float = 1.0
point_tooltip_content: str = "data"


class AltairThemes(Enum):
    DEFAULT_THEME = {
        "view": {
            "width": chart_width,
            "height": chart_height,
            "strokeOpacity": chart_border_opacity,
        },
        "font": font,
        "title": {
            "align": title_align,
            "color": title_color,
            "fontSize": title_font_size,
            "dy": title_dy,
            "subtitleFont": subtitle_font,
            "subtitleFontSize": subtitle_font_size,
            "subtitleColor": subtitle_color,
            "subtitleFontWeight": subtitle_font_weight,
            "subtitlePadding": subtitle_padding,
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
        "legend": {
            "titleColor": legend_title_color,
            "titleFontSize": legend_title_font_size,
        },
        "range": {
            "category": category_color_scheme,
            "diverging": diverging_color_scheme,
            "heatmap": heatmap_color_scheme,
            "ordinal": ordinal_color_scheme,
        },
        "area": {"color": fill_color, "fillOpacity": fill_opacity},
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
