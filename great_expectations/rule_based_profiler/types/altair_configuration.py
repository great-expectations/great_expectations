from typing import Any, Dict, List

from great_expectations.types import ColorPalettes, Colors

# Size
chart_width: int = 800
chart_height: int = 275

# Title
title_align: str = "center"
title_font_size: int = 16
title_color: str = Colors.PURPLE.value

# Both Axes
axis_title_color: str = Colors.PURPLE.value
axis_title_font_size: int = 12
axis_label_color: str = Colors.BLUE_1.value

# X-Axis
x_axis_label_angle: int = 90

# Color Palettes
category_color_scheme: List[str] = ColorPalettes.CATEGORY.value
diverging_color_scheme: List[str] = ColorPalettes.DIVERGING.value
heatmap_color_scheme: List[str] = ColorPalettes.HEATMAP.value
ordinal_color_scheme: List[str] = ColorPalettes.ORDINAL.value


ALTAIR_CONFIGURATION: Dict[str, Any] = {
    "view": {"width": chart_width, "height": chart_height},
    "title": {"align": title_align, "color": title_color, "fontSize": title_font_size},
    "axis": {
        "titleFontSize": axis_title_font_size,
        "titleColor": axis_title_color,
        "labelColor": axis_label_color,
    },
    "axisX": {"labelAngle": x_axis_label_angle},
    "range": {
        "category": category_color_scheme,
        "diverging": diverging_color_scheme,
        "heatmap": heatmap_color_scheme,
        "ordinal": ordinal_color_scheme,
    },
}
