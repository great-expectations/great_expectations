import json
import pandas as pd

from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationMetricDiagnostics,
    ExpectationRendererDiagnostics,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    TestData,
    ExpectationTestCase,
    ExpectationTestDataCases,
)
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation
)

from .fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__BrokenIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)

### Tests for _get_augmented_library_metadata

def test__get_augmented_library_metadata_on_a_class_with_no_library_metadata_object():
    augmented_library_metadata = ExpectColumnValuesToEqualThree()._get_augmented_library_metadata()
    assert augmented_library_metadata == AugmentedLibraryMetadata(
        maturity='CONCEPT_ONLY',
        tags=[],
        contributors=[],
        library_metadata_passed_checks=False,
        package=None
    )

def test__get_augmented_library_metadata_on_a_class_with_a_basic_library_metadata_object():
    augmented_library_metadata = ExpectColumnValuesToEqualThree__SecondIteration()._get_augmented_library_metadata()
    assert augmented_library_metadata == AugmentedLibraryMetadata(
        maturity='EXPERIMENTAL',
        tags=["tag", "other_tag"],
        contributors=["@abegong"],
        library_metadata_passed_checks=True,
        package=None
    )

def test__get_augmented_library_metadata_on_a_class_with_a_package_in_its_library_metadata_object():
    class MyExpectation(ExpectColumnValuesToEqualThree__SecondIteration):
        library_metadata = {
            "maturity": "EXPERIMENTAL",
            "package": "whatsit_expectations",
            "tags": ["tag", "other_tag"],
            "contributors": [
                "@abegong",
            ],
        }

    augmented_library_metadata = MyExpectation()._get_augmented_library_metadata()
    assert augmented_library_metadata == AugmentedLibraryMetadata(
        maturity='EXPERIMENTAL',
        tags=["tag", "other_tag"],
        contributors=["@abegong"],
        library_metadata_passed_checks=True,
        package="whatsit_expectations"
    )

### Tests for _get_examples

def test__get_examples_from_a_class_with_no_examples():
    assert ExpectColumnValuesToEqualThree()._get_examples() == []

def test__get_examples_from_a_class_with_some_examples():
    examples = ExpectColumnValuesToEqualThree__SecondIteration()._get_examples()
    assert len(examples) == 1

    first_example = examples[0]
    assert isinstance(first_example, ExpectationTestDataCases)
    assert first_example.data == {'mostly_threes': [3, 3, 3, 3, 3, 3, 2, -1, None, None]}
    assert len(first_example.tests) == 1

def test__get_examples_from_a_class_with_return_only_gallery_examples_equals_false():
    examples = ExpectColumnValuesToEqualThree__SecondIteration()._get_examples(
        return_only_gallery_examples=False
    )
    assert len(examples) == 1

    first_example = examples[0]
    assert isinstance(first_example, ExpectationTestDataCases)
    assert first_example.data == {'mostly_threes': [3, 3, 3, 3, 3, 3, 2, -1, None, None]}
    assert len(first_example.tests) == 3

### Tests for _get_description_diagnostics

def test__get_description_diagnostics():
    class ExpectColumnValuesToBeAwesome(ColumnMapExpectation):
        """Lo, here is a docstring
        
        It has more to it.
        """

    description_diagnostics = ExpectColumnValuesToBeAwesome()._get_description_diagnostics()
    assert description_diagnostics == ExpectationDescriptionDiagnostics(
        camel_name= "ExpectColumnValuesToBeAwesome",
        snake_name= "expect_column_values_to_be_awesome",
        short_description= "Lo, here is a docstring",
        docstring= """Lo, here is a docstring
        
        It has more to it.
        """,
    )

### Tests for _execute_test_examples

def test__execute_test_examples__with_an_empty_list():
    executed_test_examples, errors = ExpectColumnValuesToEqualThree__ThirdIteration()._execute_test_examples(
        expectation_type="expect_column_values_to_equal_three",
        examples=[],
    )

def test__execute_test_examples__with_a_single_example():
    example = ExpectationTestDataCases(
        data=TestData(**{
            "mostly_threes": [3, 3, 3, 5, None],


        }),
        tests=[
            ExpectationTestCase(
                title= "positive_test_with_mostly",
                include_in_gallery= True,
                exact_match_out= False,
                input= {"column": "mostly_threes", "mostly": 0.6},
                output= {
                    "success": True,
                    "unexpected_index_list": [6, 7],
                    "unexpected_list": [2, -1],
                }
            )
        ]
    )

    executed_test_examples = ExpectColumnValuesToEqualThree__ThirdIteration()._execute_test_examples(
        expectation_type="expect_column_values_to_equal_three",
        examples=[example],
    )
    print(executed_test_examples)
    print(json.dumps(executed_test_examples[0].to_dict(), indent=2))
    print(type(executed_test_examples[0]))
    assert len(executed_test_examples) == 1

# ### Tests for _get_metric_diagnostics_list
# def test__get_metric_diagnostics_list_on_a_class_without_metrics():
#     metric_diagnostics_list = ExpectColumnValuesToEqualThree()._get_metric_diagnostics_list(executed_test_examples)
#     print(metric_diagnostics_list)
#     assert len(metric_diagnostics_list) == 0
#     ExpectationMetricDiagnostics(
#         has_question_renderer=False
#     )

# def test__get_execution_engine_diagnostics():
#     pass
# introspected_execution_engines : ExpectationExecutionEngineDiagnostics = self._get_execution_engine_diagnostics(
#     metric_diagnostics_list,
# )
# test_results : List[ExpectationTestDiagnostics] = self._get_test_results(
#     expectation_type=description_diagnostics.snake_name,
#     test_data_cases=self._get_examples(return_only_gallery_examples=False),
#     execution_engine_diagnostics=introspected_execution_engines,
# )
# executed_test_examples, errors = self._execute_test_examples(
#     expectation_type=description_diagnostics.snake_name,
#     examples=examples,
# )
# renderers : List[ExpectationRendererDiagnostics] = self._get_renderer_diagnostics(
#     expectation_name=description_diagnostics.snake_name,
#     executed_test_examples=executed_test_examples,
# )

