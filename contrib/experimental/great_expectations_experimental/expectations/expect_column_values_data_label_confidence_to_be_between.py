"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import dataprofiler as dp
import json
import numpy as np
# remove extra tf loggin
import tensorflow as tf
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

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


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesDataLabelConfidenceToBeBetween(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.data_label_confidence_to_be_between"

    condition_value_keys = ("min_value", "max_value",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_value, max_value, **kwargs):
        """
        implement the yes/no question for the expectation
        """
        labeler = dp.DataLabeler(labeler_type='structured')
        try:
            preds = labeler.predict(column, predict_options={"show_confidences": True})
        except:
            preds = None
        return np.logical_and(preds['conf']>=min_value, preds['conf']<=max_value)


# This class defines the Expectation itself
class ExpectColumnValuesDataLabelConfidenceToBeBetween(ColumnMapExpectation):
    """
    df.expect_column_values_data_label_confidence_to_be_between(

    )
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "OPEID6": ['1002', '1052', '25034', 'McRoomyRoom'],
                "INSTNM": ['Alabama A & M University',
                    'University of Alabama at Birmingham', 'Amridge University', 'McRoomyRoom'],
                "ZIP": ['35762', '35294-0110', '36117-3553', 'McRoomyRoom'],
                "ACCREDAGENCY": ['Southern Association of Colleges and Schools Commission on Colleges',
                    'Southern Association of Colleges and Schools Commission on Colleges',
                    'Southern Association of Colleges and Schools Commission on Colleges', 'McRoomyRoom'],
                "INSTURL": ['www.aamu.edu/', 'https://www.uab.edu',
                    'www.amridgeuniversity.edu', 'McRoomyRoom'],
                "NPCURL": ['www.aamu.edu/admissions-aid/tuition-fees/net-price-calculator.html',
                    'https://uab.studentaidcalculator.com/survey.aspx',
                    'www2.amridgeuniversity.edu:9091/', 'McRoomyRoom'],
                "LATITUDE": ['34.783368', '33.505697', '32.362609', 'McRoomyRoom'],
                "LONGITUDE": ['-86.568502', '-86.799345', '-86.17401', 'McRoomyRoom'],
                "RELAFFIL": ['NULL', 'NULL', '74', 'McRoomyRoom'],
                "DEATH_YR2_RT": ['PrivacySuppressed', 'PrivacySuppressed', 'PrivacySuppressed', 'McRoomyRoom'],
                "SEARCH_STRING": ['Alabama A & M University AAMU',
                    'University of Alabama at Birmingham ',
                    'Amridge University Southern Christian University  Regions University', 'McRoomyRoom'],
            },
            "tests": [
                {
                    "title": "test_longitude_between_max_min_confidence",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "OPEID6",  "min_value": 0.00, "max_value": 1.00},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": ["McRoomyRoom"],
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.data_label_confidence_to_be_between"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("min_value", "max_value", "mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",  # "concept_only", "experimental", "beta", or "production"
        "tags": ["dataprofiler"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@taylorfturner",  # Don't forget to add your github handle here!
        ],
    }

if __name__ == "__main__":
    report = ExpectColumnValuesDataLabelConfidenceToBeBetween().run_diagnostics()
    print (json.dumps(report, indent=2))
