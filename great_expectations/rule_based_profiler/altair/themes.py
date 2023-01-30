from enum import Enum

from great_expectations.types import (
    ColorPalettes,
    FontFamily,
    PrimaryColors,
    SecondaryColors,
    TintsAndShades,
)

# Size
chart_width: int = 800
chart_height: int = 250

# View
chart_border_opacity: float = 0

# Font
font_family = FontFamily.MONTSERRAT
font = font_family.value

#
# Chart Components
#

# Title
title_align = "center"
title_color = PrimaryColors.ORANGE
title_font_size = 15
title_font_weight = 600
title_dy = -10
subtitle_color = PrimaryColors.COAL_GRAY
subtitle_font = font
subtitle_font_size = 14
subtitle_font_weight = 500

# Both Axes
axis_title_color = PrimaryColors.COAL_GRAY
axis_title_font_size = 14
axis_title_font_weight = 500
axis_title_padding = 10
axis_label_color = PrimaryColors.COAL_GRAY
axis_label_font_size = 12
axis_label_font_weight = 500
axis_label_flush = True
axis_label_overlap_reduction = True

# X-Axis Only
x_axis_title_y = 25
x_axis_label_angle = 0
x_axis_label_flush = True
x_axis_grid = True

# Y-Axis Only
y_axis_title_x = -55

# Legend
legend_title_color = PrimaryColors.COAL_GRAY
legend_title_font_size = 12
legend_title_font_weight = 500

# Scale
scale_continuous_padding = 33
scale_band_padding_outer = 1.0

#
# Color Palettes
#

category_color_scheme = ColorPalettes.CATEGORY_5.value
diverging_color_scheme = ColorPalettes.DIVERGING_7.value
heatmap_color_scheme = ColorPalettes.HEATMAP_6.value
ordinal_color_scheme = ColorPalettes.ORDINAL_5.value

#
# Chart Types
#

# Area
fill_opacity = 0.5
fill_color = TintsAndShades.ROYAL_BLUE_20

# Line Chart
line_color = SecondaryColors.ROYAL_BLUE
line_stroke_width = 2.5
line_opacity = 0.9

# Point
point_size = 50
point_color = SecondaryColors.LEAF_GREEN
point_filled = True
point_opacity = 1.0

# Bar Chart
bar_color = SecondaryColors.ROYAL_BLUE
bar_opacity = 0.7
bar_stroke_color = SecondaryColors.MIDNIGHT_BLUE
bar_stroke_width = 1
bar_stroke_opacity = 1.0


class AltairThemes(Enum):
    """
    Altair theme configuration reference:
        https://altair-viz.github.io/user_guide/configuration.html#top-level-chart-configuration
    """

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
            "fontWeight": title_font_weight,
            "dy": title_dy,
            "subtitleFont": subtitle_font,
            "subtitleFontSize": subtitle_font_size,
            "subtitleColor": subtitle_color,
            "subtitleFontWeight": subtitle_font_weight,
        },
        "axis": {
            "titleFontSize": axis_title_font_size,
            "titleFontWeight": axis_title_font_weight,
            "titleColor": axis_title_color,
            "titlePadding": axis_title_padding,
            "labelFontSize": axis_label_font_size,
            "labelFontWeight": axis_label_font_weight,
            "labelColor": axis_label_color,
            "labelFlush": axis_label_flush,
            "labelOverlap": axis_label_overlap_reduction,
        },
        "axisY": {
            "titleX": y_axis_title_x,
        },
        "axisX": {
            "titleY": x_axis_title_y,
            "labelAngle": x_axis_label_angle,
            "labelFlush": x_axis_label_flush,
            "grid": x_axis_grid,
        },
        "legend": {
            "titleColor": legend_title_color,
            "titleFontSize": legend_title_font_size,
            "titleFontWeight": legend_title_font_weight,
        },
        "range": {
            "category": category_color_scheme,
            "diverging": diverging_color_scheme,
            "heatmap": heatmap_color_scheme,
            "ordinal": ordinal_color_scheme,
        },
        "scale": {
            "continuousPadding": scale_continuous_padding,
            "bandPaddingOuter": scale_band_padding_outer,
        },
        "area": {
            "color": fill_color,
            "fillOpacity": fill_opacity,
        },
        "line": {
            "color": line_color,
            "strokeWidth": line_stroke_width,
        },
        "point": {
            "size": point_size,
            "color": point_color,
            "filled": point_filled,
            "opacity": point_opacity,
        },
        "bar": {
            "color": bar_color,
            "opacity": bar_opacity,
            "stroke": bar_stroke_color,
            "strokeWidth": bar_stroke_width,
            "strokeOpacity": bar_stroke_opacity,
        },
    }
