from dataclasses import field
# import dataclasses
from pydantic.dataclasses import dataclass
from dataclasses import dataclass
from enum import Enum
import inspect
import logging
from typing import List, Union

from great_expectations.types import SerializableDictDot

class Maturity(Enum):
    """The four levels of maturity for features within Great Expectations"""

    CONCEPT_ONLY = "CONCEPT_ONLY"
    EXPERIMENTAL = "EXPERIMENTAL"
    BETA = "BETA"
    PRODUCTION = "PRODUCTION"

@dataclass
class AugmentedLibraryMetadata(SerializableDictDot):
    """An augmented version of the Expectation.library_metadata object, used within ExpectationDiagnostics"""

    maturity: Maturity
    tags: List[str]
    contributors: List[str]
    library_metadata_passed_checks: bool
    package: Union[str, None] = None

@dataclass
class LegacyAugmentedLibraryMetadataAdapter(AugmentedLibraryMetadata):
    """This class is a temporary adaopter to allow typing of legacy library_metadata objects, without needing to immediately clean up every object."""

    maturity_level_substitutions = {
        "experimental": "EXPERIMENTAL",
        "beta": "BETA",
        "production": "PRODUCTION",
    }

    @classmethod
    def from_dict(cls, dict):
        temp_dict = {}
        for k, v in dict.items():
            #Ignore parameters that don't match the type definition
            if k in inspect.signature(cls).parameters:
                temp_dict[k] = v
            else:
                logging.warning(
                    f'WARNING: Got extra parameter: {k} while instantiating LegacyAugmentedLibraryMetadataAdapter.'
                    'This parameter will be ignored.'
                    'You probably need to clean up a library_metadata object.'
                )

            # If necessary, substitute strings for precise Enum values.
            if "maturity" in temp_dict and temp_dict["maturity"] in cls.maturity_level_substitutions:
                temp_dict["maturity"] = cls.maturity_level_substitutions[temp_dict["maturity"]]

        return cls(**temp_dict)

@dataclass
class ExpectationDescriptionDiagnostics(SerializableDictDot):
    """Captures basic descriptive info about an Expectation. Used within the ExpectationDiagnostic object."""

    camel_name : str
    snake_name : str
    short_description : str
    docstring : str

@dataclass
class ExpectationRendererDiagnostics(SerializableDictDot):
    """Captures information about a specific Renderer within an Expectation. Used within the ExpectationDiagnostic object."""

    name: str
    is_supported: bool
    is_standard: bool
    samples: List[str]

@dataclass
class ExpectationMetricDiagnostics(SerializableDictDot):
    """Captures information about a specific Metric dependency for an Expectation. Used within the ExpectationDiagnostic object."""
    
    name: str
    has_question_renderer: bool

@dataclass
class ExpectationExecutionEngineDiagnostics(SerializableDictDot):
    """Captures which of the three Execution Engines are supported by an Expectation. Used within the ExpectationDiagnostic object."""

    PandasExecutionEngine : bool
    SqlAlchemyExecutionEngine : bool
    SparkDFExecutionEngine : bool

@dataclass
class ExpectationTestDiagnostics(SerializableDictDot):
    """Captures information from executing Expectation test cases. Used within the ExpectationDiagnostic object."""

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
    """Summarizes the result of a diagnostic Check. Used within the ExpectationDiagnostic object."""

    message: str
    passed: bool
    sub_messages: List['ExpectationDiagnosticCheckMessage'] = field(default_factory=list)
