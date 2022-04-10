"""
Developed from a template for creating custom ColumnExpectations.

For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
"""
import json
import matplotlib.colors as mcolors

from typing import Optional, Dict

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial
)

"""
Medicine list from https://www.webmd.com/pill-identification/default.htm
"""

medicines = ['tylenol', 'vicodin', 'ms_contin', 'atenolol', 'oxycodone', 'hydrocodone', 'cetirizine',
     'ibuprofin', 'tizanidine', 'tramadol', 'naproxen', 'clonazapem', 'alprazolam']
colors = list(mcolors.CSS4_COLORS)
shapes = ['oblong', 'round', 'oval', 'rectangle']

"""
Method checks whether input x is in one or all of the three above lists
"""

def check_validity(x):
    return x in medicines or x in colors or x in shapes

# Class defines a Metric to support your Expectation.
# Main business logic for calculation will live in this class for most ColumnExpecations

class ColumnMapContainsMedicineNames(ColumnMapMetricProvider):
    """
    Contains metric to check validity of medicine names.
        It also considers colors and shapes of medicine by means of the check_validity() function
    """
    # This is the id string that will be used to reference your Metric.
    condition_metric_name = "column_values.valid_medicine_names"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls, 
        column,
        **kwargs
    ):
        return column.apply(lambda x : check_validity(x))

# Class defines the Expectation itself

class ExpectColumnValuesToContainValidMedicineNames(ColumnMapExpectation):

    """Determines where or not a table contains valid medicine properties
        (i.e., valid medicine name that matches list 'medicines', valid color from 'colors', and valid shape from 'shapes')"""

    # Examples shown in public gallery
    # Executed as unit tests for for your Expectation

    map_metric = "column_values.valid_medicine_names"

    examples = [
        {
            "data": {
                "medicine_name": ['tylenol', 'atenolol', 'tizanidine', 'clonazapem'], 
                "color": ['white', 'white', 'lightcyan', 'white'],
                "shape": ['oblong', 'round', 'oval', 'round']
            }, 
            "tests": [
                {
                    "title": "medicine_names_all_match_test", 
                    "exact_match_out": False, 
                    "include_in_gallery": True,
                    "in": {
                        "column": "medicine_name"
                    }, 
                    "out": {
                        "success": True,
                    }
                }, 
                {
                    "title": "medicine_colors_all_match_test", 
                    "exact_match_out": False, 
                    "include_in_gallery": True,
                    "in": {
                        "column": "color"
                    }, 
                    "out": {
                        "success": True,
                    }
                }, 
                {
                    "title": "medicine_shapes_all_match_test", 
                    "exact_match_out": False, 
                    "include_in_gallery": True,
                    "in": {
                        "column": "shape"
                    }, 
                    "out": {
                        "success": True,
                    }
                }
            ]
        }, 
        {
            "data": {
                "medicine_name": ['ms_contin', 'ibuprofin', 'alprazolam', 'some_ordinary_medicine', 'penguinz_medicine'], 
                "color": ['white', 'white', 'lightcyan', 'green', 'penguin_color'],
                "shape": ['round', 'round', 'rectangle', 'octagon', 'hexagon']
            },
            "tests": [
                {
                    "title": "medicine_names_one_mismatch_test", 
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "medicine_name", 
                    },
                    "out": {
                        "success": True
                    }
                }, 
                {
                    "title": "medicine_shapes_one_mismatch_test", 
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "shape"
                    },
                    "out": {
                        "success": True
                    }               
                },
            ]
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation

    metric_dependencies = ("column_values.valid_medicine_names", "column_values.valid_colors", "column_values.valid_shapes")

    success_keys = ("medicine_name", "color", "shape",)

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

        # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
        def _validate(
            self, 
            configuration: ExpectationConfiguration, 
            metrics: Dict, 
            runtime_configuration: dict = None, 
            execution_engine: ExecutionEngine = None, 
        ):

            column_medicine_names = metrics["column_values.valid_medicine_name"]
            column_colors = metrics["column_values.valid_color"]
            column_shapes = metrics["column_values.valid_shape"]

            success = all(column_medicine_names) and all(column_colors) and all(column_shapes)

            return {
                "success": success, 
                "result": { 
                    "observed_value": { 
                        "valid_medicine_names": column_medicine_names, 
                        "valid_colors": column_colors,
                        "valid_shapes": column_shapes
                    }
                }
            }

    # This object contains metadata for display in the public Gallery
        library_metadata = {
            "maturity": "experimental", 
            "tags": ["experimental", "hackathon-22", "medicines", "test", "wip"], 
            "contributors": [
                "@CypherCrow"
            ], 
            "requirements": ["matplotlib"]
        }

if __name__ == "__main__":
    ExpectColumnValuesToContainValidMedicineNames().print_diagnostic_checklist()