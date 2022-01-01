from dataclasses import field
from pydantic.dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from great_expectations.types import SerializableDictDot

class Maturity(Enum):
    CONCEPT_ONLY = "CONCEPT_ONLY"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"

@dataclass
class GithubUser:
    username: str

@dataclass
class GalleryMetadata:
    maturity: Maturity
    tags: List[str]
    contributors: List[str]

@dataclass
class AumentedGalleryMetadata:
    maturity: Maturity
    package_name: str
    class_name: str
    tags: List[str]
    contributors: List[GithubUser]

@dataclass
class ExpectationDescriptionDiagnostics(SerializableDictDot):
    camel_name : str
    snake_name : str
    short_description : str
    docstring : str

# @dataclass
# class RendererDiagnostics:
#     name: str
#     is_supported: bool
#     is_standard: bool
#     samples: List[str]

@dataclass
class ExpectationRendererDiagnostics(SerializableDictDot):
    name: str
    is_supported: bool
    is_standard: bool
    samples: List[str]

    # name: str
    # is_standard: bool
    # is_registered: bool
    # samples: List[str]

    # # standard : List[RendererDiagnostics]
    # # custom : List[RendererDiagnostics]
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

@dataclass
class ExpectationMetricDiagnostics(SerializableDictDot):
    name: str
    has_question_renderer: bool

    """
    [
      "column_values.nonnull.unexpected_count",
      "column_values.match_regex.unexpected_count",
      "table.row_count",
      "column_values.match_regex.unexpected_values"
    ]
    """

@dataclass
class ExpectationExecutionEngineDiagnostics(SerializableDictDot):
    PandasExecutionEngine : bool
    SqlAlchemyExecutionEngine : bool
    SparkDFExecutionEngine : bool

@dataclass
class ExpectationTestDiagnostics(SerializableDictDot):
    test_title: str
    backend: str
    test_passed: bool
    error_message: Union[str, None] = None
    stack_trace: Union[str, None] = None

@dataclass
class ExpectationErrorDiagnostics(SerializableDictDot):
    error_msg: str
    stack_trace: str

@dataclass
class ExpectationDiagnosticChecklist(SerializableDictDot):
    pass

@dataclass
class ExpectationDiagnosticCheckMessage(SerializableDictDot):
    message: str
    passed: bool
    sub_messages: List['ExpectationDiagnosticCheckMessage'] = field(default_factory=list)
