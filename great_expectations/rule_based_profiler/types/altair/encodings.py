from enum import Enum


class AltairDataTypes(Enum):
    QUANTITATIVE = "quantitative"
    ORDINAL = "ordinal"
    NOMINAL = "nominal"
    TEMPORAL = "temporal"
    GEOJSON = "geojson"
