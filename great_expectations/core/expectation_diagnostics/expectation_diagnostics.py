from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from great_expectations.types import SerializableDictDot
from great_expectations.core.expectation_diagnostics.supporting_types import (
    GalleryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationDiagnosticChecklist,
    ExpectationGalleryDiagnosticMetadata,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationTestDataCases
)

@dataclass(frozen=True)
class ExpectationDiagnostics(SerializableDictDot):
    """An immutable object created by Expectation.run_diagnostics.
    It contains information introspected from the Expectation class, in formats that can be renderered at the command line, and by the Gallery.
    """

    # These two objects are taken directly from the Expectation class, without modification
    library_metadata: GalleryMetadata
    examples: List[ExpectationTestDataCases]

    # These objects are derived from the Expectation class
    # They're a combination of direct introspection of existing properties, and instantiating the Expectation with test data and actually executing methods.
    # For example, we can verify the existence of certain Renderers through introspection alone, but in order to see what they return, we need to instantiate the Expectation and actually run the method.
    description: ExpectationDescriptionDiagnostics
    renderers: ExpectationRendererDiagnostics 
    metrics: List[ExpectationMetricDiagnostics]
    execution_engines: ExpectationExecutionEngineDiagnostics
    tests: ExpectationTestDiagnostics

    # These objects are rollups of other information, formatted for display at the command line and in the Gallery
    checklist: ExpectationDiagnosticChecklist
    gallery_metadata: ExpectationGalleryDiagnosticMetadata

    def to_checklist_str(self):
        pass

    def to_json_dict(self) -> dict:
        return super().to_json_dict()