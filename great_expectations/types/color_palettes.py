from enum import Enum
from typing import List


class Colors(Enum):
    GREEN_1: str = "#00C2A4"
    PINK: str = "#FD5383"
    PURPLE: str = "#8784FF"
    BLUE_1: str = "#1B2A4D"
    BLUE_2: str = "#384B74"
    BLUE_3: str = "#8699B7"


class ColorPalettes(Enum):
    CATEGORY: List[str] = [
        Colors.BLUE_1.value,
        Colors.GREEN_1.value,
        Colors.PURPLE.value,
        Colors.PINK.value,
        Colors.BLUE_3.value,
    ]
