from __future__ import annotations

import copy
import json
import logging
import pathlib
import sys
import time
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING, Final, List, Optional, Union

from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationLegacyTestCaseAdapter,
    ExpectationTestDataCases,
    TestBackend,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationBackendTestResultCounts,
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticMaturityMessages,
    ExpectationErrorDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    Maturity,
    RendererTestDiagnostics,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import (
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.render import (
    CollapseContent,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    ValueListContent,
)
from great_expectations.self_check.util import (
    evaluate_json_test_v3_api,
    generate_dataset_name_from_expectation_name,
    generate_expectation_tests,
)
from great_expectations.util import camel_to_snake

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.expectations.expectation import Expectation
    from great_expectations.validator.validator import ValidationDependencies

_TEST_DEFS_DIR: Final = pathlib.Path(
    __file__, "..", "..", "..", "..", "tests", "test_definitions"
).resolve()


class ExpectationDoctor:
    def __init__(self, expectation: Expectation) -> None:
        self._expectation = expectation

    def print_diagnostic_checklist(
        self,
        diagnostics: Optional[ExpectationDiagnostics] = None,
        show_failed_tests: bool = False,
        backends: Optional[List[str]] = None,
        show_debug_messages: bool = False,
    ) -> str:
        if diagnostics is None:
            debug_logger = None
            if show_debug_messages:
                debug_logger = logging.getLogger()
                chandler = logging.StreamHandler(stream=sys.stdout)
                chandler.setLevel(logging.DEBUG)
                chandler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S"
                    )
                )
                debug_logger.addHandler(chandler)
                debug_logger.setLevel(logging.DEBUG)

            diagnostics = self.run_diagnostics(
                debug_logger=debug_logger, only_consider_these_backends=backends
            )
        if show_failed_tests:
            for test in diagnostics.tests:
                if test.test_passed is False:
                    print(f"=== {test.test_title} ({test.backend}) ===\n")
                    print(f"{80 * '='}\n")

        checklist: str = diagnostics.generate_checklist()
        print(checklist)

        return checklist

    def run_diagnostics(  # noqa: PLR0913
        self,
        raise_exceptions_for_backends: bool = False,
        ignore_suppress: bool = False,
        ignore_only_for: bool = False,
        for_gallery: bool = False,
        debug_logger: Optional[logging.Logger] = None,
        only_consider_these_backends: Optional[List[str]] = None,
        context: Optional[AbstractDataContext] = None,
    ) -> ExpectationDiagnostics:
        if debug_logger is not None:
            _debug = lambda x: debug_logger.debug(  # noqa: E731
                f"(run_diagnostics) {x}"
            )
            _error = lambda x: debug_logger.error(  # noqa: E731
                f"(run_diagnostics) {x}"
            )
        else:
            _debug = lambda x: x  # noqa: E731
            _error = lambda x: x  # noqa: E731

        library_metadata: AugmentedLibraryMetadata = self._get_augmented_library_metadata()
        examples: List[ExpectationTestDataCases] = self._get_examples(
            return_only_gallery_examples=False
        )
        gallery_examples: List[ExpectationTestDataCases] = []
        for example in examples:
            _tests_to_include = [test for test in example.tests if test.include_in_gallery]
            example = copy.deepcopy(example)  # noqa: PLW2901
            if _tests_to_include:
                example.tests = _tests_to_include
                gallery_examples.append(example)

        description_diagnostics: ExpectationDescriptionDiagnostics = (
            self._get_description_diagnostics()
        )

        _expectation_config: Optional[ExpectationConfiguration] = (
            self._get_expectation_configuration_from_examples(examples)
        )
        if not _expectation_config:
            _error(
                f"Was NOT able to get Expectation configuration for {self._expectation.expectation_type}. "  # noqa: E501
                "Is there at least one sample test where 'success' is True?"
            )
        metric_diagnostics_list: List[ExpectationMetricDiagnostics] = (
            self._get_metric_diagnostics_list(
                expectation_config=_expectation_config,
            )
        )

        introspected_execution_engines: ExpectationExecutionEngineDiagnostics = (
            self._get_execution_engine_diagnostics(
                metric_diagnostics_list=metric_diagnostics_list,
                registered_metrics=_registered_metrics,
            )
        )
        engines_implemented = [
            e.replace("ExecutionEngine", "")
            for e, i in introspected_execution_engines.items()
            if i is True
        ]
        _debug(
            f"Implemented engines for {self._expectation.expectation_type}: {', '.join(engines_implemented)}"  # noqa: E501
        )

        _debug("Getting test results")
        test_results: List[ExpectationTestDiagnostics] = self._get_test_results(
            expectation_type=description_diagnostics.snake_name,
            test_data_cases=examples,
            execution_engine_diagnostics=introspected_execution_engines,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
            ignore_suppress=ignore_suppress,
            ignore_only_for=ignore_only_for,
            debug_logger=debug_logger,
            only_consider_these_backends=only_consider_these_backends,
            context=context,
        )

        backend_test_result_counts: List[ExpectationBackendTestResultCounts] = (
            ExpectationDiagnostics._get_backends_from_test_results(test_results)
        )

        renderers: List[ExpectationRendererDiagnostics] = self._get_renderer_diagnostics(
            expectation_type=description_diagnostics.snake_name,
            test_diagnostics=test_results,
            registered_renderers=_registered_renderers,  # type: ignore[arg-type]
        )

        maturity_checklist: ExpectationDiagnosticMaturityMessages = self._get_maturity_checklist(
            library_metadata=library_metadata,
            description=description_diagnostics,
            examples=examples,
            tests=test_results,
            backend_test_result_counts=backend_test_result_counts,
        )

        coverage_score: float = self._get_coverage_score(
            backend_test_result_counts=backend_test_result_counts,
            execution_engines=introspected_execution_engines,
        )

        _debug(f"coverage_score: {coverage_score} for {self._expectation.expectation_type}")

        # Set final maturity level based on status of all checks
        library_metadata.maturity = self._get_final_maturity_level(
            maturity_checklist=maturity_checklist
        )

        # Set the errors found when running tests
        errors = [
            test_result.error_diagnostics
            for test_result in test_results
            if test_result.error_diagnostics
        ]

        # If run for the gallery, don't include a bunch of stuff
        #   - Don't set examples and test_results to empty lists here since these
        #     returned attributes will be needed to re-calculate the maturity
        #     checklist later (after merging results from different runs of the
        #     build_gallery.py script per backend)
        if for_gallery:
            gallery_examples = []
            renderers = []
            errors = []

        return ExpectationDiagnostics(
            library_metadata=library_metadata,
            examples=examples,
            gallery_examples=gallery_examples,
            description=description_diagnostics,
            renderers=renderers,
            metrics=metric_diagnostics_list,
            execution_engines=introspected_execution_engines,
            tests=test_results,
            backend_test_result_counts=backend_test_result_counts,
            maturity_checklist=maturity_checklist,
            errors=errors,
            coverage_score=coverage_score,
        )

    def _get_augmented_library_metadata(self):
        """Introspect the Expectation's library_metadata object (if it exists), and augment it with additional information."""  # noqa: E501

        augmented_library_metadata = {
            "maturity": Maturity.CONCEPT_ONLY,
            "tags": [],
            "contributors": [],
            "requirements": [],
            "library_metadata_passed_checks": False,
            "has_full_test_suite": False,
            "manually_reviewed_code": False,
        }
        required_keys = {"contributors", "tags"}
        allowed_keys = {
            "contributors",
            "has_full_test_suite",
            "manually_reviewed_code",
            "maturity",
            "requirements",
            "tags",
        }
        problems = []

        if hasattr(self._expectation, "library_metadata"):
            augmented_library_metadata.update(self._expectation.library_metadata)
            keys = set(self._expectation.library_metadata.keys())
            missing_required_keys = required_keys - keys
            forbidden_keys = keys - allowed_keys

            if missing_required_keys:
                problems.append(f"Missing required key(s): {sorted(missing_required_keys)}")
            if forbidden_keys:
                problems.append(f"Extra key(s) found: {sorted(forbidden_keys)}")
            if not isinstance(augmented_library_metadata["requirements"], list):
                problems.append("library_metadata['requirements'] is not a list ")
            if not problems:
                augmented_library_metadata["library_metadata_passed_checks"] = True
        else:
            problems.append("No library_metadata attribute found")

        augmented_library_metadata["problems"] = problems
        return AugmentedLibraryMetadata.from_legacy_dict(augmented_library_metadata)

    def _get_maturity_checklist(
        self,
        library_metadata: Union[AugmentedLibraryMetadata, ExpectationDescriptionDiagnostics],
        description: ExpectationDescriptionDiagnostics,
        examples: List[ExpectationTestDataCases],
        tests: List[ExpectationTestDiagnostics],
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
    ) -> ExpectationDiagnosticMaturityMessages:
        """Generate maturity checklist messages"""
        experimental_checks = []
        beta_checks = []
        production_checks = []

        experimental_checks.append(ExpectationDiagnostics._check_library_metadata(library_metadata))
        experimental_checks.append(ExpectationDiagnostics._check_docstring(description))
        experimental_checks.append(ExpectationDiagnostics._check_example_cases(examples, tests))
        experimental_checks.append(
            ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
                backend_test_result_counts
            )
        )
        beta_checks.append(
            ExpectationDiagnostics._check_input_validation(self._expectation, examples)
        )
        beta_checks.append(ExpectationDiagnostics._check_renderer_methods(self._expectation))
        beta_checks.append(
            ExpectationDiagnostics._check_core_logic_for_all_applicable_execution_engines(
                backend_test_result_counts
            )
        )

        production_checks.append(ExpectationDiagnostics._check_full_test_suite(library_metadata))
        production_checks.append(ExpectationDiagnostics._check_manual_code_review(library_metadata))

        return ExpectationDiagnosticMaturityMessages(
            experimental=experimental_checks,
            beta=beta_checks,
            production=production_checks,
        )

    @staticmethod
    def _get_coverage_score(
        backend_test_result_counts: List[ExpectationBackendTestResultCounts],
        execution_engines: ExpectationExecutionEngineDiagnostics,
    ) -> float:
        """Generate coverage score"""
        _total_passed = 0
        _total_failed = 0
        _num_backends = 0
        _num_engines = sum(x for x in execution_engines.values() if x)
        for result in backend_test_result_counts:
            _num_backends += 1
            _total_passed += result.num_passed
            _total_failed += result.num_failed

        coverage_score = _num_backends + _num_engines + _total_passed - (1.5 * _total_failed)

        return coverage_score

    @staticmethod
    def _get_final_maturity_level(
        maturity_checklist: ExpectationDiagnosticMaturityMessages,
    ) -> Maturity:
        """Get final maturity level based on status of all checks"""
        maturity = ""
        all_experimental = all(check.passed for check in maturity_checklist.experimental)
        all_beta = all(check.passed for check in maturity_checklist.beta)
        all_production = all(check.passed for check in maturity_checklist.production)
        if all_production and all_beta and all_experimental:
            maturity = Maturity.PRODUCTION
        elif all_beta and all_experimental:
            maturity = Maturity.BETA
        else:
            maturity = Maturity.EXPERIMENTAL

        return maturity

    def _get_examples_from_json(self):
        """Only meant to be called by self._get_examples"""
        results = []
        found = next(_TEST_DEFS_DIR.rglob(f"**/{self._expectation.expectation_type}.json"), None)
        if found:
            with open(found) as fp:
                data = json.load(fp)
            results = data["datasets"]
        return results

    def _get_examples(  # noqa: C901 - too complex
        self, return_only_gallery_examples: bool = True
    ) -> List[ExpectationTestDataCases]:
        """
        Get a list of examples from the object's `examples` member variable.

        For core expectations, the examples are found in tests/test_definitions/

        :param return_only_gallery_examples: if True, include only test examples where `include_in_gallery` is true
        :return: list of examples or [], if no examples exist
        """  # noqa: E501
        # Currently, only community contrib expectations have an examples attribute
        all_examples: List[dict] = self._expectation.examples or self._get_examples_from_json()

        included_examples = []
        for i, example in enumerate(all_examples, 1):
            included_test_cases = []
            # As of commit 7766bb5caa4e0 on 1/28/22, only_for does not need to be applied to individual tests  # noqa: E501
            # See:
            #   - https://github.com/great-expectations/great_expectations/blob/7766bb5caa4e0e5b22fa3b3a5e1f2ac18922fdeb/tests/test_definitions/column_map_expectations/expect_column_values_to_be_unique.json#L174
            #   - https://github.com/great-expectations/great_expectations/pull/4073
            top_level_only_for = example.get("only_for")
            top_level_suppress_test_for = example.get("suppress_test_for")
            for test in example["tests"]:
                if (
                    test.get("include_in_gallery") == True  # noqa: E712
                    or return_only_gallery_examples == False  # noqa: E712
                ):
                    copied_test = copy.deepcopy(test)
                    if top_level_only_for:
                        if "only_for" not in copied_test:
                            copied_test["only_for"] = top_level_only_for
                        else:
                            copied_test["only_for"].extend(top_level_only_for)
                    if top_level_suppress_test_for:
                        if "suppress_test_for" not in copied_test:
                            copied_test["suppress_test_for"] = top_level_suppress_test_for
                        else:
                            copied_test["suppress_test_for"].extend(top_level_suppress_test_for)
                    included_test_cases.append(ExpectationLegacyTestCaseAdapter(**copied_test))

            # If at least one ExpectationTestCase from the ExpectationTestDataCases was selected,
            # then keep a copy of the ExpectationTestDataCases including data and the selected ExpectationTestCases.  # noqa: E501
            if len(included_test_cases) > 0:
                copied_example = copy.deepcopy(example)
                copied_example["tests"] = included_test_cases
                copied_example.pop("_notes", None)
                copied_example.pop("only_for", None)
                copied_example.pop("suppress_test_for", None)
                if "test_backends" in copied_example:
                    copied_example["test_backends"] = [
                        TestBackend(**tb) for tb in copied_example["test_backends"]
                    ]

                if "dataset_name" not in copied_example:
                    dataset_name = generate_dataset_name_from_expectation_name(
                        dataset=copied_example,
                        expectation_type=self._expectation.expectation_type,
                        index=i,
                    )
                    copied_example["dataset_name"] = dataset_name

                included_examples.append(ExpectationTestDataCases(**copied_example))

        return included_examples

    def _get_docstring_and_short_description(self) -> tuple[str, str]:
        """Conveninence method to get the Exepctation's docstring and first line"""

        if self._expectation.__doc__ is not None:
            docstring = self._expectation.__doc__
            short_description = next(line for line in self._expectation.__doc__.split("\n") if line)
        else:
            docstring = ""
            short_description = ""

        return docstring, short_description

    def _get_description_diagnostics(self) -> ExpectationDescriptionDiagnostics:
        """Introspect the Expectation and create its ExpectationDescriptionDiagnostics object"""

        camel_name = self._expectation.__class__.__name__
        snake_name = camel_to_snake(camel_name)
        docstring, short_description = self._get_docstring_and_short_description()

        return ExpectationDescriptionDiagnostics(
            **{
                "camel_name": camel_name,
                "snake_name": snake_name,
                "short_description": short_description,
                "docstring": docstring,
            }
        )

    def _get_expectation_configuration_from_examples(  # noqa: C901 - too complex
        self,
        examples: List[ExpectationTestDataCases],
    ) -> Optional[ExpectationConfiguration]:
        """Return an ExpectationConfiguration instance using test input expected to succeed"""
        if examples:
            for example in examples:
                tests = example.tests
                if tests:
                    for test in tests:
                        if test.output.get("success"):
                            return ExpectationConfiguration(
                                type=self._expectation.expectation_type,
                                kwargs=test.input,
                            )

            # There is no sample test where `success` is True, or there are no tests
            for example in examples:
                tests = example.tests
                if tests:
                    for test in tests:
                        if test.input:
                            return ExpectationConfiguration(
                                type=self._expectation.expectation_type,
                                kwargs=test.input,
                            )
        return None

    @staticmethod
    def _get_execution_engine_diagnostics(
        metric_diagnostics_list: List[ExpectationMetricDiagnostics],
        registered_metrics: dict,
        execution_engine_names: Optional[List[str]] = None,
    ) -> ExpectationExecutionEngineDiagnostics:
        """Check to see which execution_engines are fully supported for this Expectation.

        In order for a given execution engine to count, *every* metric must have support on that execution engines.
        """  # noqa: E501
        if not execution_engine_names:
            execution_engine_names = [
                "PandasExecutionEngine",
                "SqlAlchemyExecutionEngine",
                "SparkDFExecutionEngine",
            ]

        execution_engines = {}
        for provider in execution_engine_names:
            all_true = True
            if not metric_diagnostics_list:
                all_true = False
            for metric_diagnostics in metric_diagnostics_list:
                try:
                    has_provider = (
                        provider in registered_metrics[metric_diagnostics.name]["providers"]
                    )
                    if not has_provider:
                        all_true = False
                        break
                except KeyError:
                    # https://github.com/great-expectations/great_expectations/blob/abd8f68a162eaf9c33839d2c412d8ba84f5d725b/great_expectations/expectations/core/expect_table_row_count_to_equal_other_table.py#L174-L181
                    # expect_table_row_count_to_equal_other_table does tricky things and replaces
                    # registered metric "table.row_count" with "table.row_count.self" and "table.row_count.other"  # noqa: E501
                    if "table.row_count" in metric_diagnostics.name:
                        continue

            execution_engines[provider] = all_true

        return ExpectationExecutionEngineDiagnostics(**execution_engines)

    def _get_metric_diagnostics_list(
        self,
        expectation_config: Optional[ExpectationConfiguration],
    ) -> List[ExpectationMetricDiagnostics]:
        """Check to see which Metrics are upstream validation_dependencies for this Expectation."""

        # NOTE: Abe 20210102: Strictly speaking, identifying upstream metrics shouldn't need to rely on an expectation config.  # noqa: E501
        # There's probably some part of get_validation_dependencies that can be factored out to remove the dependency.  # noqa: E501

        if not expectation_config:
            return []

        validation_dependencies: ValidationDependencies = (
            self._expectation.get_validation_dependencies()
        )

        metric_name: str
        metric_diagnostics_list: List[ExpectationMetricDiagnostics] = [
            ExpectationMetricDiagnostics(
                name=metric_name,
                has_question_renderer=False,
            )
            for metric_name in validation_dependencies.get_metric_names()
        ]

        return metric_diagnostics_list

    @classmethod
    def _get_test_results(  # noqa: PLR0913
        cls,
        expectation_type: str,
        test_data_cases: List[ExpectationTestDataCases],
        execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics,
        raise_exceptions_for_backends: bool = False,
        ignore_suppress: bool = False,
        ignore_only_for: bool = False,
        debug_logger: Optional[logging.Logger] = None,
        only_consider_these_backends: Optional[List[str]] = None,
        context: Optional[AbstractDataContext] = None,
    ) -> List[ExpectationTestDiagnostics]:
        """Generate test results. This is an internal method for run_diagnostics."""

        if debug_logger is not None:
            _debug = lambda x: debug_logger.debug(  # noqa: E731
                f"(_get_test_results) {x}"
            )
            _error = lambda x: debug_logger.error(  # noqa: E731
                f"(_get_test_results) {x}"
            )
        else:
            _debug = lambda x: x  # noqa: E731
            _error = lambda x: x  # noqa: E731
        _debug("Starting")

        test_results = []

        exp_tests = generate_expectation_tests(
            expectation_type=expectation_type,
            test_data_cases=test_data_cases,
            execution_engine_diagnostics=execution_engine_diagnostics,
            raise_exceptions_for_backends=raise_exceptions_for_backends,
            ignore_suppress=ignore_suppress,
            ignore_only_for=ignore_only_for,
            debug_logger=debug_logger,
            only_consider_these_backends=only_consider_these_backends,
            context=context,
        )

        error_diagnostics: Optional[ExpectationErrorDiagnostics]
        backend_test_times = defaultdict(list)
        for exp_test in exp_tests:
            if exp_test["test"] is None:
                _debug(f"validator_with_data failure for {exp_test['backend']}--{expectation_type}")

                error_diagnostics = ExpectationErrorDiagnostics(
                    error_msg=exp_test["error"],
                    stack_trace="",
                    test_title="all",
                    test_backend=exp_test["backend"],
                )

                test_results.append(
                    ExpectationTestDiagnostics(
                        test_title="all",
                        backend=exp_test["backend"],
                        test_passed=False,
                        include_in_gallery=False,
                        validation_result=None,
                        error_diagnostics=error_diagnostics,
                    )
                )
                continue

            exp_combined_test_name = (
                f"{exp_test['backend']}--{exp_test['test']['title']}--{expectation_type}"
            )
            _debug(f"Starting {exp_combined_test_name}")
            _start = time.time()
            validation_result, error_message, stack_trace = evaluate_json_test_v3_api(
                validator=exp_test["validator_with_data"],
                expectation_type=exp_test["expectation_type"],
                test=exp_test["test"],
                raise_exception=False,
                debug_logger=debug_logger,
            )
            _end = time.time()
            _duration = _end - _start
            backend_test_times[exp_test["backend"]].append(_duration)
            _debug(
                f"Took {_duration} seconds to evaluate_json_test_v3_api for {exp_combined_test_name}"  # noqa: E501
            )
            if error_message is None:
                _debug(f"PASSED {exp_combined_test_name}")
                test_passed = True
                error_diagnostics = None
            else:
                _error(f"{error_message!r} for {exp_combined_test_name}")
                print(f"{stack_trace[0]}")
                error_diagnostics = ExpectationErrorDiagnostics(
                    error_msg=error_message,
                    stack_trace=stack_trace,
                    test_title=exp_test["test"]["title"],
                    test_backend=exp_test["backend"],
                )
                test_passed = False

            if validation_result:
                # The ExpectationTestDiagnostics instance will error when calling it's to_dict()
                # method (AttributeError: 'ExpectationConfiguration' object has no attribute 'raw_kwargs')  # noqa: E501
                validation_result.expectation_config.raw_kwargs = (
                    validation_result.expectation_config._raw_kwargs
                )

            test_results.append(
                ExpectationTestDiagnostics(
                    test_title=exp_test["test"]["title"],
                    backend=exp_test["backend"],
                    test_passed=test_passed,
                    include_in_gallery=exp_test["test"]["include_in_gallery"],
                    validation_result=validation_result,
                    error_diagnostics=error_diagnostics,
                )
            )

        for backend_name, test_times in sorted(backend_test_times.items()):
            _debug(
                f"Took {sum(test_times)} seconds to run {len(test_times)} tests {backend_name}--{expectation_type}"  # noqa: E501
            )

        return test_results

    def _get_renderer_diagnostics(
        self,
        expectation_type: str,
        test_diagnostics: List[ExpectationTestDiagnostics],
        registered_renderers: List[str],
        standard_renderers: Optional[
            List[Union[str, LegacyRendererType, LegacyDiagnosticRendererType]]
        ] = None,
    ) -> List[ExpectationRendererDiagnostics]:
        """Generate Renderer diagnostics for this Expectation, based primarily on a list of ExpectationTestDiagnostics."""  # noqa: E501

        if not standard_renderers:
            standard_renderers = [
                LegacyRendererType.ANSWER,
                LegacyDiagnosticRendererType.UNEXPECTED_STATEMENT,
                LegacyDiagnosticRendererType.OBSERVED_VALUE,
                LegacyDiagnosticRendererType.STATUS_ICON,
                LegacyDiagnosticRendererType.UNEXPECTED_TABLE,
                LegacyRendererType.PRESCRIPTIVE,
                LegacyRendererType.QUESTION,
            ]

        supported_renderers = self._get_registered_renderers(
            expectation_type=expectation_type,
            registered_renderers=registered_renderers,  # type: ignore[arg-type]
        )

        renderer_diagnostic_list = []
        for renderer_name in set(standard_renderers).union(set(supported_renderers)):
            samples = []
            if renderer_name in supported_renderers:
                _, renderer = registered_renderers[expectation_type][renderer_name]  # type: ignore[call-overload]

                for test_diagnostic in test_diagnostics:
                    test_title = test_diagnostic["test_title"]

                    try:
                        rendered_result = renderer(
                            configuration=test_diagnostic["validation_result"][
                                "expectation_config"
                            ],
                            result=test_diagnostic["validation_result"],
                        )
                        rendered_result_str = self._get_rendered_result_as_string(rendered_result)

                    except Exception as e:
                        new_sample = RendererTestDiagnostics(
                            test_title=test_title,
                            renderered_str=None,
                            rendered_successfully=False,
                            error_message=str(e),
                            stack_trace=traceback.format_exc(),
                        )

                    else:
                        new_sample = RendererTestDiagnostics(
                            test_title=test_title,
                            renderered_str=rendered_result_str,
                            rendered_successfully=True,
                        )

                    finally:
                        samples.append(new_sample)

            new_renderer_diagnostics = ExpectationRendererDiagnostics(
                name=renderer_name,
                is_supported=renderer_name in supported_renderers,
                is_standard=renderer_name in standard_renderers,
                samples=samples,
            )
            renderer_diagnostic_list.append(new_renderer_diagnostics)

        # Sort to enforce consistency for testing
        renderer_diagnostic_list.sort(key=lambda x: x.name)

        return renderer_diagnostic_list

    @staticmethod
    def _get_registered_renderers(
        expectation_type: str,
        registered_renderers: dict,
    ) -> List[str]:
        """Get a list of supported renderers for this Expectation, in sorted order."""
        supported_renderers = list(registered_renderers[expectation_type].keys())
        supported_renderers.sort()
        return supported_renderers

    def _get_rendered_result_as_string(  # noqa: C901, PLR0912
        self, rendered_result
    ) -> str:
        """Convenience method to get rendered results as strings."""

        result: str = ""

        if isinstance(rendered_result, str):
            result = rendered_result

        elif isinstance(rendered_result, list):
            sub_result_list = []
            for sub_result in rendered_result:
                res = self._get_rendered_result_as_string(sub_result)
                if res is not None:
                    sub_result_list.append(res)

            result = "\n".join(sub_result_list)

        elif isinstance(rendered_result, (CollapseContent, RenderedStringTemplateContent)):
            result = rendered_result.__str__()

        elif isinstance(rendered_result, RenderedAtomicContent):
            result = f"(RenderedAtomicContent) {rendered_result.to_json_dict()!r}"

        elif isinstance(rendered_result, RenderedContentBlockContainer):
            result = "(RenderedContentBlockContainer) " + repr(rendered_result.to_json_dict())

        elif isinstance(rendered_result, RenderedTableContent):
            result = f"(RenderedTableContent) {rendered_result.to_json_dict()!r}"

        elif isinstance(rendered_result, RenderedGraphContent):
            result = f"(RenderedGraphContent) {rendered_result.to_json_dict()!r}"

        elif isinstance(rendered_result, ValueListContent):
            result = f"(ValueListContent) {rendered_result.to_json_dict()!r}"

        elif isinstance(rendered_result, dict):
            result = f"(dict) {rendered_result!r}"

        elif isinstance(rendered_result, int):
            result = repr(rendered_result)

        elif rendered_result is None:
            result = ""

        else:
            raise TypeError(  # noqa: TRY003
                f"Expectation._get_rendered_result_as_string can't render type {type(rendered_result)} as a string."  # noqa: E501
            )

        if "inf" in result:
            result = ""
        return result
