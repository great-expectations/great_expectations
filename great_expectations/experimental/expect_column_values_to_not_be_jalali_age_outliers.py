"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import re
from typing import Optional

from persiantools.jdatetime import JalaliDate

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.


class ColumnValuesNotJalaliAgeOutliers(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.not_jalali_age_outliers"
    condition_value_keys = ("lower_bound", "upper_bound")

    # this is method is used to check for patterns for
    # jalali date format

    def calculate_age(jalali_date, min=0, max=100):
        if jalali_date is None:
            return None
        jalali_date = jalali_date.strip()
        # regex without any problem
        true_reg = r"\d{4}[/|-]\d{2}[/|-]\d{2}"

        if re.search(true_reg, jalali_date):
            year = jalali_date[0:4]
            month = jalali_date[5:7]
            day = jalali_date[8:]

            try:
                if int(month) < 1 or int(month) > 12 or int(day) < 1 or int(day) > 31:
                    return None
            except:
                return None

            today = JalaliDate.today()
            try:
                birth_date = JalaliDate(int(year), int(month), int(day))
            except:
                return None
            age = int((today - birth_date).days / 365)
            return age if min < age < max else None

        # a true jalali input with all digits
        true_reg_all_digits = r"^\d{8}$"
        if re.search(true_reg_all_digits, jalali_date):
            year = jalali_date[0:4]
            month = jalali_date[4:6]
            day = jalali_date[6:8]
            try:
                if int(month) < 1 or int(month) > 12 or int(day) < 1 or int(day) > 31:
                    return None
            except:
                return None

            today = JalaliDate.today()
            try:
                birth_date = JalaliDate(int(year), int(month), int(day))
            except:
                return None
            age = int((today - birth_date).days / 365)
            return age if min < age < max else None

        # regex to find problems
        year_reg = r"^\d{0,3}[/|-]"
        month_reg = r"[/|-]\d{1}[/|-]"
        day_reg = r"[/|-]\d{1}$"

        if re.search(year_reg, jalali_date):
            return None

        if re.search(month_reg, jalali_date) or re.search(day_reg, jalali_date):
            # correct month
            if re.search(month_reg, jalali_date):
                index = re.search(month_reg, jalali_date).span()
                temp = list(jalali_date)
                temp.insert(index[0] + 1, "0")
                jalali_date = "".join(temp)

            # correct day
            if re.search(day_reg, jalali_date):
                index = re.search(day_reg, jalali_date).span()
                temp = list(jalali_date)
                temp.insert(index[0] + 1, "0")
                jalali_date = "".join(temp)

            year = jalali_date[0:4]
            month = jalali_date[5:7]
            day = jalali_date[8:]

            try:
                if int(month) < 1 or int(month) > 12 or int(day) < 1 or int(day) > 31:
                    return None
            except:
                return None

            today = JalaliDate.today()
            try:
                birth_date = JalaliDate(int(year), int(month), int(day))
            except:
                return None
            age = int((today - birth_date).days / 365)
            return age if min < age < max else None

        return None

    # This method implements the core logic for the PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, lower_bound, upper_bound, **kwargs):
        age = column.map(cls.calculate_age)
        return (age > lower_bound) & (age < upper_bound)


# This class defines the Expectation itself
class ExpectColumnValuesToNotBeJalaliAgeOutliers(ColumnMapExpectation):
    """column values should follow jalali date format"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "all_good": [
                    "1401/01/01",
                    "1395-03-1",
                    "1342/08/5",
                    "1365-7-9",
                    "1376-11-23",
                ],
                "some_not_good": [
                    "1401/12/26",
                    "139-01-1",
                    "13/08/15",
                    "13656-07-19",
                    "1376*11-23",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_good", "lower_bound": 0, "upper_bound": 120},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "some_not_good",
                        "lower_bound": 100,
                        "upper_bound": 200,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_jalali_age_outliers"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("lower_bound", "upper_bound")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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
        configuration = configuration or self.configuration

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
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@mortezahajipourworks",
        ],
        "package": "experimental_expectations",
    }


if __name__ == "__main__":
    ExpectColumnValuesToNotBeJalaliAgeOutliers().print_diagnostic_checklist()
