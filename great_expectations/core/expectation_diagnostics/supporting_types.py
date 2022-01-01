from dataclasses import field
from pydantic.dataclasses import dataclass
# from dataclasses import dataclass
from enum import Enum
from typing import List, Union

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
class AugmentedLibraryMetadata(SerializableDictDot):
    maturity: Maturity
    tags: List[str]
    contributors: List[str] #Maybe List[GithubUser]?
    # package_name: str
    library_metadata_passed_checks: bool

@dataclass
class ExpectationDescriptionDiagnostics(SerializableDictDot):
    camel_name : str
    snake_name : str
    short_description : str
    docstring : str

@dataclass
class ExpectationRendererDiagnostics(SerializableDictDot):
    name: str
    is_supported: bool
    is_standard: bool
    samples: List[str]

@dataclass
class ExpectationMetricDiagnostics(SerializableDictDot):
    name: str
    has_question_renderer: bool

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
class ExpectationDiagnosticCheckMessage(SerializableDictDot):
    message: str
    passed: bool
    sub_messages: List['ExpectationDiagnosticCheckMessage'] = field(default_factory=list)
