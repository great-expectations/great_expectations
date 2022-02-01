import json 
from typing import Any, Dict, Optional, Tuple
import dataprofiler as dp


from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMetricProvider,
    column_condition_partial,
)


class ColumnValueDataLabelConfidenceToBeBetween(ColumnMetricProvider):
    """MetricProvider Class for Data Label Probability greater than \
    or equal to the user-specified threshold"""
    
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.are_probibalistically_greater_or_equal_to_user_threshold"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, threshold, **kwargs):
        labeler = dp.DataLabeler(labeler_type='structured')
        try:
            labels, confidence = labeler.predict(column, predict_options={"show_confidences": True})
        except:
            labels, confidence = None, None
        return confidence >= threshold

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, **kwargs):
        raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        raise NotImplementedError


class ExpectColumnDataLabelConfidenceToBeBetween(ColumnMapExpectation):
    """
    This function 

    :param: column,  
    :param: data_label,
    :param: min,
    :param: max,
    :param: trained_model

    df.expect_column_proportion_of_data_labels_to_be_between(
        column,
        data_label=<>,
        min,
        max,
        trained_model
    )
    """

    examples = [
        {
            "data": {
                "OPEID6": ['1002', '1052', '25034'],
                "INSTNM": ['Alabama A & M University',
                    'University of Alabama at Birmingham', 'Amridge University'],
                "ZIP": ['35762', '35294-0110', '36117-3553'],
                "ACCREDAGENCY": ['Southern Association of Colleges and Schools Commission on Colleges',
                    'Southern Association of Colleges and Schools Commission on Colleges',
                    'Southern Association of Colleges and Schools Commission on Colleges'],
                "INSTURL": ['www.aamu.edu/', 'https://www.uab.edu',
                    'www.amridgeuniversity.edu'],
                "NPCURL": ['www.aamu.edu/admissions-aid/tuition-fees/net-price-calculator.html',
                    'https://uab.studentaidcalculator.com/survey.aspx',
                    'www2.amridgeuniversity.edu:9091/'],
                "LATITUDE": ['34.783368', '33.505697', '32.362609'],
                "LONGITUDE": ['-86.568502', '-86.799345', '-86.17401'],
                "RELAFFIL": ['NULL', 'NULL', '74'],
                "DEATH_YR2_RT": ['PrivacySuppressed', 'PrivacySuppressed', 'PrivacySuppressed'],
                "SEARCH_STRING": ['Alabama A & M University AAMU',
                    'University of Alabama at Birmingham ',
                    'Amridge University Southern Christian University  Regions University']
            },
            "tests": [
                {
                    "title": "positive_test_with_column_one",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "OPEID6",  "threshold": .65},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": [4, 5],
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_probibalistically_greater_or_equal_to_user_threshold"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ()

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["nlp", "dataprofiler"],
        "contributors": ["@taylorfturner"],
        "package": "experimental_expectations",
        "requirements": ["dataprofiler[full]>=0.7.2"],
    }

if __name__ == "__main__":
    diagnostics_report = ExpectColumnDataLabelConfidenceToBeBetween().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
