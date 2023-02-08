from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Sequence, Union

from typing_extensions import TypedDict

from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,  # noqa: TCH001
)
from great_expectations.types import SerializableDictDot


class Maturity(str, Enum):
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
    requirements: List[str]
    library_metadata_passed_checks: bool
    has_full_test_suite: bool
    manually_reviewed_code: bool
    problems: List[str] = field(default_factory=list)

    legacy_maturity_level_substitutions = {
        "experimental": "EXPERIMENTAL",
        "beta": "BETA",
        "production": "PRODUCTION",
    }

    @classmethod
    def from_legacy_dict(cls, dict):
        """This method is a temporary adapter to allow typing of legacy library_metadata objects, without needing to immediately clean up every object."""
        temp_dict = {}
        for k, v in dict.items():
            # Ignore parameters that don't match the type definition
            if k in inspect.signature(cls).parameters:
                temp_dict[k] = v
            else:
                logging.warning(
                    f"WARNING: Got extra parameter: {k} while instantiating AugmentedLibraryMetadata."
                    "This parameter will be ignored."
                    "You probably need to clean up a library_metadata object."
                )

            # If necessary, substitute strings for precise Enum values.
            if (
                "maturity" in temp_dict
                and temp_dict["maturity"] in cls.legacy_maturity_level_substitutions
            ):
                temp_dict["maturity"] = cls.legacy_maturity_level_substitutions[
                    temp_dict["maturity"]
                ]

        return cls(**temp_dict)


@dataclass
class ExpectationDescriptionDiagnostics(SerializableDictDot):
    """Captures basic descriptive info about an Expectation. Used within the ExpectationDiagnostic object."""

    camel_name: str
    snake_name: str
    short_description: str
    docstring: str


@dataclass
class RendererTestDiagnostics(SerializableDictDot):
    """Captures information from executing Renderer test cases. Used within the ExpectationRendererDiagnostics object."""

    test_title: str
    rendered_successfully: bool
    renderered_str: Union[str, None]
    error_message: Union[str, None] = None
    stack_trace: Union[str, None] = None


@dataclass
class ExpectationRendererDiagnostics(SerializableDictDot):
    """Captures information about a specific Renderer within an Expectation. Used within the ExpectationDiagnostic object."""

    name: str
    is_supported: bool
    is_standard: bool
    samples: List[RendererTestDiagnostics]


@dataclass
class ExpectationMetricDiagnostics(SerializableDictDot):
    """Captures information about a specific Metric dependency for an Expectation. Used within the ExpectationDiagnostic object."""

    name: str
    has_question_renderer: bool


@dataclass
class ExpectationExecutionEngineDiagnostics(SerializableDictDot):
    """Captures which of the three Execution Engines are supported by an Expectation. Used within the ExpectationDiagnostic object."""

    PandasExecutionEngine: bool
    SqlAlchemyExecutionEngine: bool
    SparkDFExecutionEngine: bool


@dataclass
class ExpectationErrorDiagnostics(SerializableDictDot):
    error_msg: str
    stack_trace: str
    test_title: Optional[str] = None
    test_backend: Optional[str] = None


@dataclass
class ExpectationTestDiagnostics(SerializableDictDot):
    """Captures information from executing Expectation test cases. Used within the ExpectationDiagnostic object."""

    test_title: str
    backend: str
    test_passed: bool
    include_in_gallery: bool
    validation_result: Optional[ExpectationValidationResult]
    error_diagnostics: Optional[ExpectationErrorDiagnostics]


@dataclass
class ExpectationBackendTestResultCounts(SerializableDictDot):
    """Has each tested backend and the number of passing/failing tests"""

    backend: str
    num_passed: int
    num_failed: int
    failing_names: Optional[List[str]]


class ExpectationDiagnosticCheckMessageDict(TypedDict):
    message: str
    passed: bool


@dataclass
class ExpectationDiagnosticCheckMessage(SerializableDictDot):
    """Summarizes the result of a diagnostic Check. Used within the ExpectationDiagnostic object."""

    message: str
    passed: bool
    doc_url: Optional[str] = None
    sub_messages: Sequence[
        ExpectationDiagnosticCheckMessage | ExpectationDiagnosticCheckMessageDict
    ] = field(default_factory=list)


@dataclass
class ExpectationDiagnosticMaturityMessages(SerializableDictDot):
    """A holder for ExpectationDiagnosticCheckMessages, grouping them by maturity level. Used within the ExpectationDiagnostic object."""

    experimental: List[ExpectationDiagnosticCheckMessage]
    beta: List[ExpectationDiagnosticCheckMessage]
    production: List[ExpectationDiagnosticCheckMessage]
