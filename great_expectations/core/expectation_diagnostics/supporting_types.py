from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from great_expectations.types import SerializableDictDot

class Maturity(Enum):
    SCAFFOLDED = "SCAFFOLDED"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"

@dataclass
class GalleryMetadata:
    maturity: Maturity
    # package: str,
    tags: List[str]
    contributors: List[str] #Maybe List[Github contributor?]

@dataclass
class ExpectationDescriptionDiagnostics:
    camel_name : str
    snake_name : str
    short_description : str
    docstring : str

@dataclass
class ExpectationRendererDiagnostics:
    """
        {
        "standard": {
            "renderer.answer": "Less than 90.0% of values in column \"a\" match the regular expression ^a.",
            "renderer.diagnostic.unexpected_statement": "\n\n1 unexpected values found. 20% of 5 total rows.",
            "renderer.diagnostic.observed_value": "20% unexpected",
            "renderer.diagnostic.status_icon": "",
            "renderer.diagnostic.unexpected_table": null,
            "renderer.prescriptive": "a values must match this regular expression: ^a, at least 90 % of the time.",
            "renderer.question": "Do at least 90.0% of values in column \"a\" match the regular expression ^a?"
        },
        "custom": []
        },
    """
    pass

@dataclass
class ExpectationMetricDiagnostics:
    """
    [
      "column_values.nonnull.unexpected_count",
      "column_values.match_regex.unexpected_count",
      "table.row_count",
      "column_values.match_regex.unexpected_values"
    ]
    """
    pass

@dataclass
class ExpectationExecutionEngineDiagnostics:
    PandasExecutionEngine : bool
    SqlAlchemyExecutionEngine : bool
    SparkDFExecutionEngine : bool

@dataclass
class ExpectationTestDiagnostics:
    pass

@dataclass
class ExpectationDiagnosticChecklist:
    pass

@dataclass
class ExpectationGalleryDiagnosticMetadata:
    pass
