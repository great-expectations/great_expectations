from enum import Enum


class Colors(Enum):
    GREEN = "#00C2A4"
    PINK = "#FD5383"
    PURPLE = "#8784FF"
    BLUE_1 = "#1B2A4D"
    BLUE_2 = "#384B74"
    BLUE_3 = "#8699B7"


class ColorPalettes(Enum):
    CATEGORY = [
        Colors.BLUE_1.value,
        Colors.GREEN.value,
        Colors.PURPLE.value,
        Colors.PINK.value,
        Colors.BLUE_3.value,
    ]
    DIVERGING = [
        Colors.GREEN.value,
        "#7AD3BD",
        "#B8E2D6",
        "#F1F1F1",
        "#FCC1CB",
        "#FF8FA6",
        Colors.PINK.value,
    ]
    HEATMAP = [
        Colors.BLUE_2.value,
        "#56678E",
        "#7584A9",
        "#94A2C5",
        "#B5C2E2",
        "#D6E2FF",
    ]
    ORDINAL = [
        Colors.BLUE_1.value,
        "#273969",
        "#354886",
        "#4657A3",
        "#5966C2",
        "#6f75E0",
        Colors.PURPLE.value,
    ]
