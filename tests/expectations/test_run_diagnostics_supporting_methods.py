import pytest

from great_expectations.core.expectation_diagnostics.expectation_doctor import (
    ExpectationDoctor,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationTestDataCases,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationExecutionEngineDiagnostics,
    ExpectationMetricDiagnostics,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from tests.expectations.fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)

### Tests for _get_augmented_library_metadata


@pytest.mark.unit
def test__get_augmented_library_metadata_on_a_class_with_no_library_metadata_object():
    expectation = ExpectColumnValuesToEqualThree(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    augmented_library_metadata = doctor._get_augmented_library_metadata()
    assert augmented_library_metadata == AugmentedLibraryMetadata(
        maturity="CONCEPT_ONLY",
        tags=[],
        contributors=[],
        requirements=[],
        library_metadata_passed_checks=False,
        has_full_test_suite=False,
        manually_reviewed_code=False,
        problems=["No library_metadata attribute found"],
    )


@pytest.mark.unit
def test__get_augmented_library_metadata_on_a_class_with_a_basic_library_metadata_object():
    expectation = ExpectColumnValuesToEqualThree__SecondIteration(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    augmented_library_metadata = doctor._get_augmented_library_metadata()
    assert augmented_library_metadata == AugmentedLibraryMetadata(
        maturity="EXPERIMENTAL",
        tags=["tag", "other_tag"],
        contributors=["@abegong"],
        requirements=[],
        library_metadata_passed_checks=True,
        has_full_test_suite=False,
        manually_reviewed_code=False,
    )


### Tests for _get_examples


@pytest.mark.unit
def test__get_examples_from_a_class_with_no_examples():
    expectation = ExpectColumnValuesToEqualThree(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    examples = doctor._get_examples()
    assert examples == []


@pytest.mark.unit
def test__get_examples_from_a_class_with_some_examples():
    expectation = ExpectColumnValuesToEqualThree__SecondIteration(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    examples = doctor._get_examples()
    assert len(examples) == 1

    first_example = examples[0]
    assert isinstance(first_example, ExpectationTestDataCases)
    assert first_example.data == {"mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None]}
    assert len(first_example.tests) == 1


@pytest.mark.unit
def test__get_examples_from_a_class_with_return_only_gallery_examples_equals_false():
    expectation = ExpectColumnValuesToEqualThree__SecondIteration(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    examples = doctor._get_examples(return_only_gallery_examples=False)
    assert len(examples) == 1

    first_example = examples[0]
    assert isinstance(first_example, ExpectationTestDataCases)
    assert first_example.data == {"mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None]}
    assert len(first_example.tests) == 3


### Tests for _get_description_diagnostics


@pytest.mark.unit
def test__get_description_diagnostics():
    class ExpectColumnValuesToBeAwesome(ColumnMapExpectation):
        """Lo, here is a docstring

        It has more to it.
        """

    expectation = ExpectColumnValuesToBeAwesome(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    description_diagnostics = doctor._get_description_diagnostics()
    assert description_diagnostics == ExpectationDescriptionDiagnostics(
        camel_name="ExpectColumnValuesToBeAwesome",
        snake_name="expect_column_values_to_be_awesome",
        short_description="Lo, here is a docstring",
        docstring="""Lo, here is a docstring

        It has more to it.
        """,
    )


### Tests for _get_metric_diagnostics_list
@pytest.mark.unit
def test__get_metric_diagnostics_list_on_a_class_without_metrics():
    _config = None
    expectation = ExpectColumnValuesToEqualThree(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    metric_diagnostics_list = doctor._get_metric_diagnostics_list(expectation_config=_config)
    assert len(metric_diagnostics_list) == 0
    ExpectationMetricDiagnostics(
        name="column_values.something",
        has_question_renderer=False,
    )


@pytest.mark.unit
def test__get_metric_diagnostics_list_on_a_class_with_metrics():
    _config = None
    expectation = ExpectColumnValuesToEqualThree__ThirdIteration(column="values")
    doctor = ExpectationDoctor(expectation=expectation)
    metric_diagnostics_list = doctor._get_metric_diagnostics_list(expectation_config=_config)
    assert len(metric_diagnostics_list) == 0
    ExpectationMetricDiagnostics(
        name="column_values.something",
        has_question_renderer=False,
    )


### Tests for _get_execution_engine_diagnostics
@pytest.mark.skip(
    """
The business logic for _get_execution_engine_diagnostics is just plain wrong.
We should be verifying that all expectations test cases run on any given Execution Engine.
Metrics could be used to make inferences, but they'd never provide comparably compelling evidence.
"""
)
@pytest.mark.unit
def test__get_execution_engine_diagnostics_with_no_metrics_diagnostics():
    assert ExpectColumnValuesToEqualThree__ThirdIteration._get_execution_engine_diagnostics(
        metric_diagnostics_list=[],
        registered_metrics={},
    ) == ExpectationExecutionEngineDiagnostics(
        PandasExecutionEngine=False,
        SqlAlchemyExecutionEngine=False,
        SparkDFExecutionEngine=False,
    )


@pytest.mark.skip(
    """
The business logic for _get_execution_engine_diagnostics is just plain wrong.
We should be verifying that all expectations test cases run on any given Execution Engine.
Metrics could be used to make inferences, but they'd never provide comparably compelling evidence.
"""
)
@pytest.mark.unit
def test__get_execution_engine_diagnostics_with_one_metrics_diagnostics():
    metrics_diagnostics_list = [
        ExpectationMetricDiagnostics(
            name="colum_values.something",
            has_question_renderer=True,
        )
    ]
    registered_metrics = {"colum_values.something": {"providers": ["PandasExecutionEngine"]}}
    assert ExpectColumnValuesToEqualThree__ThirdIteration._get_execution_engine_diagnostics(
        metric_diagnostics_list=metrics_diagnostics_list,
        registered_metrics=registered_metrics,
    ) == ExpectationExecutionEngineDiagnostics(
        PandasExecutionEngine=False,
        SqlAlchemyExecutionEngine=False,
        SparkDFExecutionEngine=False,
    )


### Tests for _get_test_results
@pytest.mark.skip(
    reason="Timeout of 30 seconds reached trying to connect to localhost:8088 (trino port)"
)
@pytest.mark.all_backends
def test__get_test_results():
    test_results = ExpectColumnValuesToEqualThree__ThirdIteration(
        column="values"
    )._get_test_results(
        expectation_type="expect_column_values_to_equal_three",
        test_data_cases=ExpectColumnValuesToEqualThree__ThirdIteration(
            column="values"
        )._get_examples(return_only_gallery_examples=False),
        execution_engine_diagnostics=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=True,
            SqlAlchemyExecutionEngine=False,
            SparkDFExecutionEngine=False,
        ),
    )
    assert len(test_results) == 3
    for result in test_results:
        assert result.test_passed

    test_results = ExpectColumnValuesToEqualThree__ThirdIteration(
        column="values"
    )._get_test_results(
        expectation_type="expect_column_values_to_equal_three",
        test_data_cases=ExpectColumnValuesToEqualThree__ThirdIteration(
            column="values"
        )._get_examples(return_only_gallery_examples=False),
        execution_engine_diagnostics=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=True,
            SqlAlchemyExecutionEngine=True,
            SparkDFExecutionEngine=False,
        ),
    )
    for result in test_results:
        # Abe: 1/1/2022: I'm not sure this is the behavior we want long term. How does backend relate to ExecutionEngine?  # noqa: E501
        if result.backend == "pandas":
            assert result.test_passed is True
        elif result.backend == "sqlite":
            assert result.test_passed is False
