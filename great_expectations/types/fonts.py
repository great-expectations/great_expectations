from enum import Enum


class FontFamily(Enum):
    MONTSERRAT = "Montserrat"
    ROBOTO_MONO = "Roboto Mono"
    SOURCE_SANS_PRO = "Source Sans Pro"

    @classmethod
    def has_member_key(cls, key):
        return key in cls.__members__


class FontFamilyURL(Enum):
    MONTSERRAT = "https://fonts.googleapis.com/css2?family=Montserrat"
    ROBOTO_MONO = "https://fonts.googleapis.com/css2?family=Roboto+Mono"
    SOURCE_SANS_PRO = "https://fonts.googleapis.com/css2?family=Source+Sans+Pro"
