from enum import Enum
from typing import List


class Colors(Enum):
    GREEN: str = "#00C2A4"
    PINK: str = "#FD5383"
    PURPLE: str = "#8784FF"
    BLUE_1: str = "#1B2A4D"
    BLUE_2: str = "#384B74"
    BLUE_3: str = "#8699B7"


class ColorPalettes(Enum):
    CATEGORY: List[str] = [
        Colors.BLUE_1.value,
        Colors.GREEN.value,
        Colors.PURPLE.value,
        Colors.PINK.value,
        Colors.BLUE_3.value,
    ]
    DIVERGING: List[str] = [
        Colors.GREEN.value,
        "#7AD3BD",
        "#B8E2D6",
        "#F1F1F1",
        "#FCC1CB",
        "#FF8FA6",
        Colors.PINK.value,
    ]
    HEATMAP: List[str] = [
        Colors.BLUE_2.value,
        "#56678E",
        "#7584A9",
        "#94A2C5",
        "#B5C2E2",
        "#D6E2FF",
    ]
    ORDINAL: List[str] = [
        Colors.BLUE_1.value,
        "#273969",
        "#354886",
        "#4657A3",
        "#5966C2",
        "#6f75E0",
        Colors.PURPLE.value,
    ]
