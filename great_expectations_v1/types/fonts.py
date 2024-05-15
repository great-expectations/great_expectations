from __future__ import annotations

from enum import Enum


class FontFamily(Enum):
    MONTSERRAT = "Montserrat"
    ROBOTO_MONO = "Roboto Mono"
    SOURCE_SANS_PRO = "Source Sans Pro"


class FontFamilyURL(Enum):
    MONTSERRAT = "https://fonts.googleapis.com/css2?family=Montserrat"
    ROBOTO_MONO = "https://fonts.googleapis.com/css2?family=Roboto+Mono"
    SOURCE_SANS_PRO = "https://fonts.googleapis.com/css2?family=Source+Sans+Pro"
