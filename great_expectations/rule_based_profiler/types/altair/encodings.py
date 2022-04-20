from enum import Enum


class DataTypes(Enum):
    QUANTITATIVE = "quantitative"
    ORDINAL = "ordinal"
    NOMINAL = "nominal"
    TEMPORAL = "temporal"
    GEOJSON = "geojson"
