from enum import Enum


class PrimaryColors(str, Enum):
    ORANGE = "#FF6310"
    COAL_GRAY = "#404041"


class SecondaryColors(str, Enum):
    MIDNIGHT_BLUE = "#272763"
    ROYAL_BLUE = "#223F99"
    TURQUOISE_BLUE = "#4DC0B4"
    LEAF_GREEN = "#B6C647"
    GOLD_YELLOW = "#F3C62D"
    POMEGRANATE_PINK = "#E2226B"
    LAVENDER_PURPLE = "#BC87E6"


class TintsAndShades(str, Enum):
    ORANGE_100 = "#803208"
    ORANGE_90 = "#A03e0A"
    ORANGE_80 = "#C04B0C"
    ORANGE_70 = "#df570e"
    ORANGE_60 = "#ff6310"
    ORANGE_50 = "#ff7f3b"
    ORANGE_40 = "#ff9b67"
    ORANGE_30 = "#ffb892"
    ORANGE_20 = "#ffd4be"
    ORANGE_10 = "#FFF0E9"
    YELLOW_100 = "#534207"
    YELLOW_90 = "#7b6311"
    YELLOW_80 = "#a3841a"
    YELLOW_70 = "#cba524"
    YELLOW_60 = "#f3c62d"
    YELLOW_50 = "#f5d053"
    YELLOW_40 = "#f7da78"
    YELLOW_30 = "#fae59e"
    YELLOW_20 = "#fcefc3"
    YELLOW_10 = "#fef9e9"
    PINK_100 = "#70062e"
    PINK_90 = "#960f42"
    PINK_80 = "#bc1957"
    PINK_70 = "#e2226b"
    PINK_60 = "#e64281"
    PINK_50 = "#ea6396"
    PINK_40 = "#ef83ac"
    PINK_30 = "#f3a3c1"
    PINK_20 = "#f7c4d6"
    PINK_10 = "#fbe4ec"
    GRAY_100 = "#000000"
    GRAY_90 = "#404041"
    GRAY_80 = "#58595B"
    GRAY_70 = "#6d6e70"
    GRAY_60 = "#808184"
    GRAY_50 = "#929497"
    GRAY_40 = "#a6a8ab"
    GRAY_30 = "#bbbdbf"
    GRAY_20 = "#d0d2d3"
    GRAY_10 = "#e6e7e8"
    MIDNIGHT_BLUE_100 = "#141432"
    MIDNIGHT_BLUE_90 = "#1e1e48"
    MIDNIGHT_BLUE_80 = "#272763"
    MIDNIGHT_BLUE_70 = "#424277"
    MIDNIGHT_BLUE_60 = "#5d5d8a"
    MIDNIGHT_BLUE_50 = "#78789e"
    MIDNIGHT_BLUE_40 = "#9494b1"
    MIDNIGHT_BLUE_30 = "#afafc5"
    MIDNIGHT_BLUE_20 = "#cacad8"
    MIDNIGHT_BLUE_10 = "#e5e5ec"
    ROYAL_BLUE_100 = "#091a4c"
    ROYAL_BLUE_90 = "#0f235f"
    ROYAL_BLUE_80 = "#162072"
    ROYAL_BLUE_70 = "#1c3686"
    ROYAL_BLUE_60 = "#223f99"
    ROYAL_BLUE_50 = "#4860ab"
    ROYAL_BLUE_40 = "#6e81bc"
    ROYAL_BLUE_30 = "#95a3ce"
    ROYAL_BLUE_20 = "#bbc4df"
    ROYAL_BLUE_10 = "#e1e5f1"
    TURQUOISE_BLUE_100 = "#144944"
    TURQUOISE_BLUE_90 = "#226760"
    TURQUOISE_BLUE_80 = "#31847c"
    TURQUOISE_BLUE_70 = "#3fa298"
    TURQUOISE_BLUE_60 = "#4dc0b4"
    TURQUOISE_BLUE_50 = "#6ccbc1"
    TURQUOISE_BLUE_40 = "#8bd6ce"
    TURQUOISE_BLUE_30 = "#aae1da"
    TURQUOISE_BLUE_20 = "#c9ece7"
    TURQUOISE_BLUE_10 = "#e8f7f4"
    GREEN_100 = "#454c14"
    GREEN_90 = "#616a21"
    GREEN_80 = "#7e892e"
    GREEN_70 = "#9aa83a"
    GREEN_60 = "#b6c647"
    GREEN_50 = "#c3d067"
    GREEN_40 = "#cfda86"
    GREEN_30 = "#dce3a6"
    GREEN_20 = "#e8edc5"
    GREEN_10 = "#f5f7e5"
    PURPLE_100 = "#3b254d"
    PURPLE_90 = "#5b3e73"
    PURPLE_80 = "#7c569a"
    PURPLE_70 = "#9c6fc0"
    PURPLE_60 = "#bc87e6"
    PURPLE_50 = "#c79aea"
    PURPLE_40 = "#d2adee"
    PURPLE_30 = "#dcc1f2"
    PURPLE_20 = "#e7d4f6"
    PURPLE_10 = "#f2e7fa"


class ColorPalettes(Enum):
    CATEGORY_5 = [
        PrimaryColors.ORANGE,
        SecondaryColors.ROYAL_BLUE,
        SecondaryColors.TURQUOISE_BLUE,
        SecondaryColors.LEAF_GREEN,
        SecondaryColors.LAVENDER_PURPLE,
    ]
    CATEGORY_7 = [
        PrimaryColors.ORANGE,
        SecondaryColors.ROYAL_BLUE,
        SecondaryColors.TURQUOISE_BLUE,
        SecondaryColors.LEAF_GREEN,
        SecondaryColors.GOLD_YELLOW,
        SecondaryColors.POMEGRANATE_PINK,
        SecondaryColors.LAVENDER_PURPLE,
    ]
    DIVERGING_7 = [
        TintsAndShades.GREEN_50,
        TintsAndShades.GREEN_30,
        TintsAndShades.GREEN_10,
        "#f1f1f1",
        TintsAndShades.PINK_10,
        TintsAndShades.PINK_30,
        TintsAndShades.PINK_50,
    ]
    HEATMAP_6 = [
        TintsAndShades.TURQUOISE_BLUE_10,
        TintsAndShades.TURQUOISE_BLUE_30,
        TintsAndShades.TURQUOISE_BLUE_50,
        TintsAndShades.MIDNIGHT_BLUE_50,
        TintsAndShades.MIDNIGHT_BLUE_70,
        TintsAndShades.MIDNIGHT_BLUE_90,
    ]
    ORDINAL_5 = [
        TintsAndShades.ORANGE_90,
        TintsAndShades.ORANGE_70,
        TintsAndShades.ORANGE_50,
        TintsAndShades.YELLOW_50,
        TintsAndShades.YELLOW_30,
    ]
