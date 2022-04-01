import json
from typing import Optional, Union

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


def is_valid_luhn_number(num: Union[int, str], min_digits: int) -> bool:
    if isinstance(num, int):
        num = str(num)

    # Remove any whitespace that might be present
    num = num.replace(" ", "")

    n_digits: int = len(num)
    # Most Credit Card numbers are 16-digits.
    # However, some card issuers like VISA and JCB issue 13 and 15-digit numbers respectively
    # which are valid Luhn numbers
    if(n_digits < min_digits): 
        return False

    sum: float = 0
    is_second: bool = False

    for i in range(n_digits-1, -1, -1):
        d = ord(num[i]) - ord("0")

        if(is_second):
            d = d * 2
        
        sum += d // 10
        sum += d % 10

        is_second = not is_second
    
    if(sum % 10 == 0):
        return True
    return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesContainLuhnNumbers(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_luhn_number"
    condition_value_keys = (
        "min_length",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_length, **kwargs):
        raise column.apply(lambda x: is_valid_luhn_number(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidLuhnNumber(ColumnMapExpectation):
    """
        This Expectation validates data as conforming to the Luhn Algorithmic Standard
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [{
        # The following credit card numbers were generated by means of an external tool.
        # These numbers are also *most likely* not in use in financial systems today, but represent valid 
        # numbers satisfied by the Luhn Algorithm
        "data": {
            "valid_numbers": [
                "79927398713",
                "4534 9128 9647 3231",
                "4930 7981 1144 8417",
                "4811 7406 1378 2814",
                "5518 8676 9303 8206",
                "5273 0969 7731 2153",
                "3459 1369 4497 239",
                "3779 8778 8360 017",
                "347071884397020",
                "342025750561957",
                "370609206167777",
                "349878269549530",
                "349058669459818",
                "5468766368079125",
                "5199153034449600",
                "5221444429161450",
                "5173739314869309",
                "5334891378365033",
            ],
            "invalid_numbers": [
                "",
                "4533",
                "4930 7981 1144 8497",
                "4811 736234  2 1378 2814",
                "5518 20276 9303 8206",
                "5273 0219 7731 2153",
                "3459 1369 1197 239",
            ],
        },
        "tests": [
            {
                "title": "pass_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "valid_numbers"
                },
                "out": {
                    "success": True,
                    "unexpected_index_list": [],
                    "unexpected_list": [],
                },
            },
            {
                "title": "fail_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "invalid_numbers"
                },
                "out": {
                    "success": False,
                },
            },
        ],
    }]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_luhn_number"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "min_digits"
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "min_digits": 1,
    }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "hackathon", "typed-entities", "column map expectation"],
        "contributors": [ 
            "@jasmcaus",
        ],
    }
    

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


    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        # password = configuration.kwargs.get("password")
        mostly = "{:.2%}".format(float(configuration.kwargs.get("mostly", 1)))

        return f'Are at least {mostly} of all values in column "{column}" Luhn numbers?'


    @classmethod
    @renderer(renderer_type="renderer.answer")
    def _answer_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        # password = result.expectation_config.kwargs.get("password")
        mostly = "{:.2%}".format(float(configuration.kwargs.get("mostly", 1)))

        if result.success:
            return f'At least {mostly} of all values in column "{column}" are Luhn numbers.'
        else:
            return f'Less than {mostly} of all values in column "{column}" are Luhn numbers.'


if __name__ == "__main__":
    ExpectColumnValuesToBeValidLuhnNumber().print_diagnostic_checklist()