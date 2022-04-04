"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import json
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

import simple_icd_10 as icd

# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesMatchValidIcdTenCategoryOrSubcategory(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.match_valid_icd_ten_category_or_subcategory"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def check_if_icd_ten_category_or_subcategory(code: str) -> bool:
            return icd.is_category_or_subcategory(code)
    
        column_icd_ten_category_or_subcategory=column.apply(check_if_icd_ten_category_or_subcategory)
        return column_icd_ten_category_or_subcategory

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeIcdTenCategoryOrSubcategory(ColumnMapExpectation):
    """     
    Expect column values to consist only of ICD-10 categories or subcategories. 
    """
        
    examples = [
        {
            "data": {
                "all_valid_categories_or_subcategories": ['C00', 'C00.1', 'C002', 'C01', 'C00.4'],
                "mostly_valid_categories_or_subcategories": ['D10', 'D17.1', 'D171', 'C00.3', 'INVALID'],
                "some_invalid_categories_or_subcategories": ['C00','C00.1','C00-C14','DOG','Z999'],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_valid_categories_or_subcategories"},
                    "out": {"success": True,},
                },
                {
                    "title": "mostly_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_valid_categories_or_subcategories", "mostly":.8},
                    "out": {"success": True,},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "some_invalid_categories_or_subcategories", "mostly": 1},
                    "out": { "success": False,},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.match_valid_icd_ten_category_or_subcategory"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
                "hackathon",
                "semantic-type",
                "experimental",
                "typed-entities",],  
        "contributors": ["@andyjessen",],
        "requirements": "simple_icd_10",
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeIcdTenCategoryOrSubcategory().print_diagnostic_checklist()
