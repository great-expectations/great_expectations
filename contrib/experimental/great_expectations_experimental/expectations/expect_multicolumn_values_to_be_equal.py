from typing import Optional
from great_expectations.core import (
    ExpectationConfiguration,
)
from great_expectations.exceptions  import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)
import sqlalchemy as sa
from great_expectations.compatibility.pyspark import functions as F
from functools import reduce
    
class MulticolumnValuesToBeEqual(MulticolumnMapMetricProvider):
    condition_metric_name = "multicolumn_values_to_be_equal"
    condition_domain_keys = ("column_list",)
    condition_value_keys = ()
    filter_column_isnull = False

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        row_wise_cond = column_list.nunique(dropna=False, axis=1) <=1
        return row_wise_cond

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        conditions = sa.or_(*[
            column_list[idx] != column_list[0] for idx in range(1, len(column_list))
        ])
        row_wise_cond = sa.not_(conditions)
        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        column_names = column_list.columns
        num_columns = len(column_names)
        conditions = []
        for idx_src in range(num_columns - 1):
            for idx_dest in range(idx_src + 1, num_columns):
                conditions.append(
                    F.col(column_names[idx_src]).eqNullSafe(F.col(column_names[idx_dest]))
                )
        row_wise_cond = reduce(lambda a, b: a & b, conditions, F.lit(True))
        return row_wise_cond


class ExpectMulticolumnValuesToBeEqual(MulticolumnMapExpectation):
    """Expect the list of multicolumn values to be equal.

    To be counted as an exception if any one column in the given column list \
        is not equal to any other column in the list
    
    expect_multicolumn_values_to_be_equal is a \
    [Multicolumn Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations).

    Args:
        column_list (List): \
            The list of column names.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): If True, then catch exceptions and \
            include them as part of the result object. \
        For more detail, see [catch_exceptions]\
            (https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included \
                in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format \
            , include_config, catch_exceptions, and meta.

    See Also:
        [expect_column_pair_values_to_be_equal](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal)
    """

    map_metric = "multicolumn_values_to_be_equal"

    examples = [
        {
            "data": {
                "OPEN_DATE": ['20220531', '20220430', '20230728'],
                "VALUE_DATE": ['20220531', '20220430', '20230728'],
                "DUE_DATE": ['20220531', '20220430', '20230728'],
                "EXPIRY_DATE": ['20230831', '20220430', '20240531'],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "in": {
                        "column_list": ["OPEN_DATE", "VALUE_DATE", "DUE_DATE"],
                    },
                    "out": {
                        "success": True,
                        "unexpected_list": [],
                        "unexpected_index_list": [],
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "in": {
                        "column_list": ["OPEN_DATE", "DUE_DATE", "EXPIRY_DATE"],
                    },
                    "out": {
                        "success": False,
                        "unexpected_list": [
                            {"OPEN_DATE": '20220531', "DUE_DATE": '20220531',\
                              "EXPIRY_DATE": '20230831'},
                            {"OPEN_DATE": '20230728', "DUE_DATE": '20230728', \
                             "EXPIRY_DATE": '20240531'},

                        ],
                        "unexpected_index_list": [0,2],
                    },
                },
            ],
        }
    ]
    success_keys = (
        "column_list",
        "mostly",
    )

    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates the configuration of an Expectation.

        The configuration will also be validated using each of the \
            `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. \
                If no configuration is provided, it will be pulled \
                    from the configuration attribute of the Expectation instance.

        Raises:
            `InvalidExpectationConfigurationError`: The configuration does \
                not contain the values required by the Expectation."
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

        try:
            assert "column_list" in configuration.kwargs, "column_list is required"
            assert isinstance(configuration.kwargs["column_list"],list), \
                "column_list must be a list"
            assert len(configuration.kwargs["column_list"]) >= 2, \
                "column_list must have at least 2 elements"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "multicolumn-map-expectation",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@karthigaiselvanm",  # Don't forget to add your github handle here!
            "@jayamnatraj",
        ],
    }

if __name__ == "__main__":
    ExpectMulticolumnValuesToBeEqual().print_diagnostic_checklist(
        show_failed_tests=True
    )
    
                 