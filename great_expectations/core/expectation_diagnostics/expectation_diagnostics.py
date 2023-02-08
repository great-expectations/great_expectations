from __future__ import annotations

import inspect
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import List, Sequence, Tuple, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationTestDataCases,  # noqa: TCH001
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationBackendTestResultCounts,
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticCheckMessage,
    ExpectationDiagnosticCheckMessageDict,
    ExpectationDiagnosticMaturityMessages,
    ExpectationErrorDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.types import SerializableDictDot


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

    library_metadata: Union[AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics]
    description: ExpectationDescriptionDiagnostics
    execution_engines: ExpectationExecutionEngineDiagnostics

    renderers: List[ExpectationRendererDiagnostics]
    metrics: List[ExpectationMetricDiagnostics]
    tests: List[ExpectationTestDiagnostics]
    backend_test_result_counts: List[ExpectationBackendTestResultCounts]
    errors: List[ExpectationErrorDiagnostics]
    maturity_checklist: ExpectationDiagnosticMaturityMessages
    coverage_score: float

    def to_json_dict(self) -> dict:
        result = convert_to_json_serializable(data=asdict(self))
        result["execution_engines_list"] = sorted(
            [
                engine
                for engine, _bool in result["execution_engines"].items()
                if _bool is True
            ]
        )
        return result

    def generate_checklist(self) -> str:
        """Generates the checklist in CLI-appropriate string format."""
        str_ = self._convert_checks_into_output_message(
            self.description["camel_name"],
            self.library_metadata.maturity,  # type: ignore[union-attr] # could be ExpectationDescriptionDiagnostics
            self.maturity_checklist,
        )
        return str_

    @staticmethod
    def _check_library_metadata(
        library_metadata: Union[
            AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics
        ],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether the Expectation has a library_metadata object"""
        sub_messages: list[ExpectationDiagnosticCheckMessageDict] = []
        for problem in library_metadata.problems:  # type: ignore[union-attr] # could be ExpectationDescriptionDiagnostics
            sub_messages.append(
                {
                    "message": problem,
                    "passed": False,
                }
            )

        return ExpectationDiagnosticCheckMessage(
            message="Has a valid library_metadata object",
            passed=library_metadata.library_metadata_passed_checks,  # type: ignore[union-attr] # could be ExpectationDescriptionDiagnostics
            sub_messages=sub_messages,
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
                        "message": f'"{short_description}"',
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
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether core logic for this Expectation exists and passes tests on at least one Execution Engine"""

        sub_messages: List[ExpectationDiagnosticCheckMessageDict] = []
        passed = False
        message = "Has core logic and passes tests on at least one Execution Engine"
        all_passing = [
            backend_test_result
            for backend_test_result in backend_test_result_counts
            if backend_test_result.failing_names is None
            and backend_test_result.num_passed >= 1
        ]

        if len(all_passing) > 0:
            passed = True
            for result in all_passing:
                sub_messages.append(
                    {
                        "message": f"All {result.num_passed} tests for {result.backend} are passing",
                        "passed": True,
                    }
                )

        if not backend_test_result_counts:
            sub_messages.append(
                {
                    "message": "There are no test results",
                    "passed": False,
                }
            )

        return ExpectationDiagnosticCheckMessage(
            message=message,
            passed=passed,
            sub_messages=sub_messages,
        )

    @staticmethod
    def _get_backends_from_test_results(
        test_results: List[ExpectationTestDiagnostics],
    ) -> List[ExpectationBackendTestResultCounts]:
        """Has each tested backend and the number of passing/failing tests"""
        backend_results = defaultdict(list)
        backend_failing_names = defaultdict(list)
        results: List[ExpectationBackendTestResultCounts] = []

        for test_result in test_results:
            backend_results[test_result.backend].append(test_result.test_passed)
            if test_result.test_passed is False:
                backend_failing_names[test_result.backend].append(
                    test_result.test_title
                )

        for backend in backend_results:
            result_counts = ExpectationBackendTestResultCounts(
                backend=backend,
                num_passed=backend_results[backend].count(True),
                num_failed=backend_results[backend].count(False),
                failing_names=backend_failing_names.get(backend),
            )
            results.append(result_counts)

        return results

    @staticmethod
    def _check_core_logic_for_all_applicable_execution_engines(
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check whether core logic for this Expectation exists and passes tests on all applicable Execution Engines"""

        sub_messages: list[ExpectationDiagnosticCheckMessageDict] = []
        passed = False
        message = "Has core logic that passes tests for all applicable Execution Engines and SQL dialects"
        all_passing = [
            backend_test_result
            for backend_test_result in backend_test_result_counts
            if backend_test_result.failing_names is None
            and backend_test_result.num_passed >= 1
        ]
        some_failing = [
            backend_test_result
            for backend_test_result in backend_test_result_counts
            if backend_test_result.failing_names is not None
        ]

        if len(all_passing) > 0 and len(some_failing) == 0:
            passed = True

        for result in all_passing:
            sub_messages.append(
                {
                    "message": f"All {result.num_passed} tests for {result.backend} are passing",
                    "passed": True,
                }
            )

        for result in some_failing:
            sub_messages.append(
                {
                    "message": f"Only {result.num_passed} / {result.num_passed + result.num_failed} tests for {result.backend} are passing",
                    "passed": False,
                }
            )
            sub_messages.append(
                {
                    "message": f"  - Failing: {', '.join(result.failing_names)}",  # type: ignore[arg-type]
                    "passed": False,
                }
            )

        if not backend_test_result_counts:
            sub_messages.append(
                {
                    "message": "There are no test results",
                    "passed": False,
                }
            )

        return ExpectationDiagnosticCheckMessage(
            message=message,
            passed=passed,
            sub_messages=sub_messages,
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
                success = test["output"].get("success")
                if success is True:
                    positive_cases += 1
                elif success is False:
                    negative_cases += 1
        return positive_cases, negative_cases

    @staticmethod
    def _count_unexpected_test_cases(
        test_diagnostics: Sequence[ExpectationTestDiagnostics],
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
        class_name: str,
        maturity_level: str,
        maturity_messages: ExpectationDiagnosticMaturityMessages,
    ) -> str:
        """Converts a list of checks into an output string (potentially nested), with ✔ to indicate checks that passed."""

        output_message = f"Completeness checklist for {class_name} ({maturity_level}):"

        checks = (
            maturity_messages.experimental
            + maturity_messages.beta
            + maturity_messages.production
        )

        for check in checks:
            if check["passed"]:
                output_message += f"\n ✔ {check['message']}"
            else:
                output_message += f"\n   {check['message']}"

            if "sub_messages" in check:
                for sub_message in check["sub_messages"]:
                    if sub_message["passed"]:
                        output_message += f"\n    ✔ {sub_message['message']}"
                    else:
                        output_message += f"\n      {sub_message['message']}"
        output_message += "\n"

        return output_message

    @staticmethod
    def _check_input_validation(
        expectation_instance,
        examples: List[ExpectationTestDataCases],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check that the validate_configuration exists and doesn't raise a config error"""
        passed = False
        sub_messages: list[ExpectationDiagnosticCheckMessageDict] = []
        rx = re.compile(r"^[\s]+assert", re.MULTILINE)
        try:
            first_test = examples[0]["tests"][0]
        except IndexError:
            sub_messages.append(
                {
                    "message": "No example found to get kwargs for ExpectationConfiguration",
                    "passed": passed,
                }
            )
        else:
            if "validate_configuration" not in expectation_instance.__class__.__dict__:
                sub_messages.append(
                    {
                        "message": "No validate_configuration method defined on subclass",
                        "passed": passed,
                    }
                )
            else:
                expectation_config = ExpectationConfiguration(
                    expectation_type=expectation_instance.expectation_type,
                    kwargs=first_test.input,
                )
                validate_configuration_source = inspect.getsource(
                    expectation_instance.__class__.validate_configuration
                )
                if rx.search(validate_configuration_source):
                    sub_messages.append(
                        {
                            "message": "Custom 'assert' statements in validate_configuration",
                            "passed": True,
                        }
                    )
                else:
                    sub_messages.append(
                        {
                            "message": "Using default validate_configuration from template",
                            "passed": False,
                        }
                    )
                try:
                    expectation_instance.validate_configuration(expectation_config)
                except InvalidExpectationConfigurationError:
                    pass
                else:
                    passed = True

        return ExpectationDiagnosticCheckMessage(
            message="Has basic input validation and type checking",
            passed=passed,
            sub_messages=sub_messages,
        )

    @staticmethod
    def _check_renderer_methods(
        expectation_instance,
    ) -> ExpectationDiagnosticCheckMessage:
        """Check if all statment renderers are defined"""
        passed = False
        # For now, don't include the "question", "descriptive", or "answer"
        # types since they are so sparsely implemented
        # all_renderer_types = {"diagnostic", "prescriptive", "question", "descriptive", "answer"}
        all_renderer_types = {"diagnostic", "prescriptive"}
        renderer_names = [
            name
            for name in dir(expectation_instance)
            if name.endswith("renderer") and name.startswith("_")
        ]
        renderer_types = {name.split("_")[1] for name in renderer_names}
        if all_renderer_types & renderer_types == all_renderer_types:
            passed = True
        return ExpectationDiagnosticCheckMessage(
            # message="Has all four statement Renderers: question, descriptive, prescriptive, diagnostic",
            message="Has both statement Renderers: prescriptive and diagnostic",
            passed=passed,
        )

    @staticmethod
    def _check_full_test_suite(
        library_metadata: Union[
            AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics
        ],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check library_metadata to see if Expectation has a full test suite"""
        return ExpectationDiagnosticCheckMessage(
            message="Has a full suite of tests, as determined by a code owner",
            passed=library_metadata.has_full_test_suite,  # type: ignore[union-attr] # could be ExpectationDescriptionDiagnostics
        )

    @staticmethod
    def _check_manual_code_review(
        library_metadata: Union[
            AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics
        ],
    ) -> ExpectationDiagnosticCheckMessage:
        """Check library_metadata to see if a manual code review has been performed"""
        return ExpectationDiagnosticCheckMessage(
            message="Has passed a manual review by a code owner for code standards and style guides",
            passed=library_metadata.manually_reviewed_code,  # type: ignore[union-attr] # could be ExpectationDescriptionDiagnostics
        )
