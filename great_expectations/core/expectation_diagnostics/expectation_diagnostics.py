from dataclasses import asdict, dataclass
from typing import List, Tuple

from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationTestDataCases,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticCheckMessage,
    ExpectationDiagnosticMaturityMessages,
    ExpectationErrorDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import SerializableDictDot

# from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class ExpectationDiagnostics(SerializableDictDot):
    """An immutable object created by Expectation.run_diagnostics.
    It contains information introspected from the Expectation class, in formats that can be renderered at the command line, and by the Gallery.

    It has three external-facing use cases:

    1. `ExpectationDiagnostics.to_dict()` creates the JSON object that populates the Gallery.
    2. `ExpectationDiagnostics.generate_checklist()` creates CLI-type string output to assist with development.
    """

    # This object is taken directly from the Expectation class, without modification
    examples: List[ExpectationTestDataCases]
    gallery_examples: List[ExpectationTestDataCases]

    # These objects are derived from the Expectation class
    # They're a combination of direct introspection of existing properties,
    # and instantiating the Expectation with test data and actually executing
    # methods.
    # For example, we can verify the existence of certain Renderers through
    # introspection alone, but in order to see what they return, we need to
    # instantiate the Expectation and actually run the method.

    library_metadata: AugmentedLibraryMetadata
    description: ExpectationDescriptionDiagnostics
    execution_engines: ExpectationExecutionEngineDiagnostics

    renderers: List[ExpectationRendererDiagnostics]
    metrics: List[ExpectationMetricDiagnostics]
    tests: List[ExpectationTestDiagnostics]
    errors: List[ExpectationErrorDiagnostics]
    maturity_checklist: ExpectationDiagnosticMaturityMessages

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=asdict(self))

    def generate_checklist(self) -> str:
        """Generates the checklist in CLI-appropriate string format."""
        str_ = self._convert_checks_into_output_message(
            self.description["camel_name"],
            self.maturity_checklist,
        )
        return str_

    @staticmethod
    def _check_library_metadata(
        library_metadata: AugmentedLibraryMetadata,
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether the Expectation has a library_metadata object"""

        return ExpectationDiagnosticCheckMessage(
            message="Has a library_metadata object",
            passed=library_metadata.library_metadata_passed_checks,
        )

    @staticmethod
    def _check_docstring(
        description: ExpectationDescriptionDiagnostics,
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether the Expectation has an informative docstring"""

        message = "Has a docstring, including a one-line short description"
        if "short_description" in description:
            short_description = description["short_description"]
        else:
            short_description = None
        if short_description not in {"", "\n", "TODO: Add a docstring here", None}:
            return ExpectationDiagnosticCheckMessage(
                message=message,
                sub_messages=[
                    {
                        "message": '"' + short_description + '"',
                        "passed": True,
                    }
                ],
                passed=True,
            )

        else:
            return ExpectationDiagnosticCheckMessage(
                message=message,
                passed=False,
            )

    @classmethod
    def _check_example_cases(
        cls,
        examples: List[ExpectationTestDataCases],
        tests: List[ExpectationTestDiagnostics],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether this Expectation has at least one positive and negative example case (and all test cases return the expected output)"""

        message = "Has at least one positive and negative example case, and all test cases pass"
        (
            positive_case_count,
            negative_case_count,
        ) = cls._count_positive_and_negative_example_cases(examples)
        unexpected_case_count = cls._count_unexpected_test_cases(tests)
        passed = (
            (positive_case_count > 0)
            and (negative_case_count > 0)
            and (unexpected_case_count == 0)
        )
        print(positive_case_count, negative_case_count, unexpected_case_count, passed)
        return ExpectationDiagnosticCheckMessage(
            message=message,
            passed=passed,
        )

    @staticmethod
    def _check_core_logic_for_at_least_one_execution_engine(
        execution_engines: ExpectationExecutionEngineDiagnostics,
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether core logic for this Expectation exists and passes tests on at least one Execution Engine"""

        message = "Has core logic and passes tests on at least one Execution Engine"
        successful_execution_engines = 0
        for k, v in execution_engines.items():
            if v is True:
                successful_execution_engines += 1

        if successful_execution_engines > 0:
            return ExpectationDiagnosticCheckMessage(
                message=message,
                passed=True,
            )
        else:
            return ExpectationDiagnosticCheckMessage(
                message=message,
                passed=False,
            )

    @staticmethod
    def _count_positive_and_negative_example_cases(
        examples: List[ExpectationTestDataCases],
    ) -> Tuple[int, int]:
        """Scans examples and returns a 2-ple with the numbers of cases with success == True and success == False"""

        positive_cases: int = 0
        negative_cases: int = 0

        for test_data_cases in examples:
            for test in test_data_cases["tests"]:
                if test["output"]["success"] is True:
                    positive_cases += 1
                elif test["output"]["success"] is False:
                    negative_cases += 1

        return positive_cases, negative_cases

    @staticmethod
    def _count_unexpected_test_cases(
        test_diagnostics: ExpectationTestDiagnostics,
    ) -> int:
        """Scans test_diagnostics and returns the number of cases that did not pass."""

        unexpected_cases: int = 0

        for test in test_diagnostics:
            passed = test["test_passed"] is True
            if not passed:
                unexpected_cases += 1

        return unexpected_cases

    @staticmethod
    def _convert_checks_into_output_message(
        class_name: str, maturity_messages: ExpectationDiagnosticMaturityMessages
    ) -> str:
        """Converts a list of checks into an output string (potentially nested), with ✔ to indicate checks that passed."""

        output_message = f"Completeness checklist for {class_name}:"

        checks = (
            maturity_messages.experimental
            + maturity_messages.beta
            + maturity_messages.production
        )

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
