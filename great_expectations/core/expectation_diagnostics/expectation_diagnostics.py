# from dataclasses import dataclass
from pydantic.dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from great_expectations.types import SerializableDictDot
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationDiagnosticCheckMessage,
    ExpectationErrorDiagnostics,
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
    library_metadata: AugmentedLibraryMetadata
    examples: List[ExpectationTestDataCases]

    # These objects are derived from the Expectation class
    # They're a combination of direct introspection of existing properties, and instantiating the Expectation with test data and actually executing methods.
    # For example, we can verify the existence of certain Renderers through introspection alone, but in order to see what they return, we need to instantiate the Expectation and actually run the method.
    description: ExpectationDescriptionDiagnostics
    execution_engines: ExpectationExecutionEngineDiagnostics
    renderers: List[ExpectationRendererDiagnostics] 
    metrics: List[ExpectationMetricDiagnostics]
    tests: List[ExpectationTestDiagnostics]
    errors: List[ExpectationErrorDiagnostics]

    # These objects are rollups of other information, formatted for display at the command line and in the Gallery
    @property
    def checklist_str(self) -> str:
        return self._convert_checks_into_output_message(
            self.description["camel_name"],
            self.checklist,
        )

    @property
    def checklist(self) -> List[ExpectationDiagnosticCheckMessage] :

        checks: List[ExpectationDiagnosticCheckMessage] = []

        # Check whether this Expectation has a library_metadata object
        checks.append(
            {
                "message": "library_metadata object exists",
                "passed": self.library_metadata.library_metadata_passed_checks,
            }
        )

        # Check whether this Expectation has an informative docstring
        message = "Has a docstring, including a one-line short description"
        if "short_description" in self.description:
            short_description = self.description["short_description"]
        else:
            short_description = None
        if short_description not in {"", "\n", "TODO: Add a docstring here", None}:
            checks.append(
                {
                    "message": message,
                    "sub_messages": [
                        {
                            "message": '"' + short_description + '"',
                            "passed": True,
                        }
                    ],
                    "passed": True,
                }
            )
        else:
            checks.append(
                {
                    "message": message,
                    "passed": False,
                }
            )

        # Check whether this Expectation has at least one positive and negative example case (and all test cases return the expected output)
        message = "Has at least one positive and negative example case, and all test cases pass"
        (
            positive_cases,
            negative_cases,
        ) = self._count_positive_and_negative_example_cases(
            self.examples
        )
        unexpected_cases = self._count_unexpected_test_cases(
            self.tests
        )
        passed = (
            (positive_cases > 0) and (negative_cases > 0) and (unexpected_cases == 0)
        )
        if passed:
            checks.append(
                {
                    "message": message,
                    "passed": passed,
                }
            )
        else:
            checks.append(
                {
                    "message": message,
                    "passed": passed,
                }
            )

        # Check whether core logic for this Expectation exists and passes tests on at least one Execution Engine
        message = "Core logic exists and passes tests on at least one Execution Engine"
        successful_execution_engines = 0
        for k, v in self.execution_engines.items():
            if v == True:
                successful_execution_engines += 1

        if successful_execution_engines > 0:
            checks.append(
                {
                    "message": message,
                    "passed": True,
                }
            )
        else:
            checks.append(
                {
                    "message": message,
                    "passed": False,
                }
            )

        return checks

    @staticmethod
    def _count_positive_and_negative_example_cases(
        examples : List[ExpectationTestDataCases]
    ) -> Tuple[int, int]:
        """Scans self.examples and returns a 2-ple with the numbers of cases with success == True and success == False"""

        positive_cases: int = 0
        negative_cases: int = 0

        for test_data_cases in examples:
            for test in test_data_cases["tests"]:
                if test["output"]["success"] == True:
                    positive_cases += 1
                elif test["output"]["success"] == False:
                    negative_cases += 1

        return positive_cases, negative_cases

    @staticmethod
    def _count_unexpected_test_cases(
        test_diagnostics : ExpectationTestDiagnostics
    ) -> int:
        """Scans self.examples and returns the number of cases that did not pass."""

        unexpected_cases: int = 0

        for test in test_diagnostics:
            passed = test["test_passed"] == "true"
            if not passed:
                unexpected_cases += 1

        return unexpected_cases

    @staticmethod
    def _convert_checks_into_output_message(
        class_name : str,
        checks: List[dict]
    ) -> str:
        """Converts a list of checks into an output string (potentially nested), with ✔ to indicate checks that passed."""

        output_message = f"Completeness checklist for {class_name}:"
        for check in checks:
            if check["passed"]:
                output_message += "\n ✔ " + check["message"]
            else:
                output_message += "\n   " + check["message"]

            if "sub_messages" in check:
                for sub_message in check["sub_messages"]:
                    if sub_message["passed"]:
                        output_message += "\n    ✔ " + sub_message["message"]
                    else:
                        output_message += "\n      " + sub_message["message"]
        output_message += "\n"

        return output_message