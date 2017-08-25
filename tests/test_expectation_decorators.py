# import json
# import hashlib
# import datetime
# import numpy as np
# import random
# import os
# import inspect

# from nose.tools import *
import sys
import unittest
import great_expectations as ge
#reload(ge)
# from great_expectations.dataset import PandasDataSet
PandasDataSet = ge.dataset.PandasDataSet
MetaPandasDataSet = ge.dataset.MetaPandasDataSet

# from ge.decorators import expectation, column_map_expectation, column_aggregate_expectation

class TestExpectationDecorators(unittest.TestCase):
    
    def test_column_map_expectation_decorator(self):

        # Create a new CustomPandasDataSet to 
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataSet(PandasDataSet):

            @MetaPandasDataSet.column_map_expectation
            def expect_column_values_to_be_odd(self, column):
                return column.map(lambda x: x % 2 )

            @MetaPandasDataSet.column_map_expectation
            def expectation_that_crashes_on_sixes(self, column):
                return column.map(lambda x: (x-6)/0 != "duck")


        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,5,5,7,9,9,9,11],
            'mostly_odd' : [1,3,5,7,9,2,4,1,3,5],
            'all_even' : [2,4,4,6,6,6,8,8,8,8],
            'odd_missing' : [1,3,5,None,None,None,None,1,3,None],
            'mixed_missing' : [1,3,5,None,None,2,4,1,3,None],
            'all_missing' : [None,None,None,None,None,None,None,None,None,None,],
        })
        df.set_default_expectation_argument("output_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_values_to_be_odd("all_odd"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("all_missing"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("odd_missing"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mixed_missing"),
            {
                'exception_list': [2,4],
                'exception_index_list': [5,6],
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd"),
            {
                'exception_list': [2, 4],
                'exception_index_list': [5, 6],
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", mostly=.6),
            {
                'exception_list': [2, 4],
                'exception_index_list': [5, 6],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", output_format="BOOLEAN_ONLY"),
            False
        )

        df.default_expectation_args["output_format"] = "BOOLEAN_ONLY"

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd"),
            False
        )

        df.default_expectation_args["output_format"] = "BASIC"

        import json
        print json.dumps(df.expect_column_values_to_be_odd("mostly_odd", include_config=True), indent=2)

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", include_config=True),
            {
                "expectation_kwargs": {
                    "column": "mostly_odd", 
                    "output_format": "BASIC"
                }, 
                "summary_obj": {
                    "exception_percent": 0.2, 
                    "partial_exception_list": [
                        2, 
                        4
                    ], 
                    "exception_count": 2
                }, 
                "success": False, 
                "expectation_type": "expect_column_values_to_be_odd"
            }
            # {
            #     'exception_list': [2, 4],
            #     'exception_index_list': [5, 6],
            #     'success': False,
            #     'expectation_type' : 'expect_column_values_to_be_odd',
            #     'expectation_kwargs' : {
            #         'column' : 'mostly_odd'
            #     }
            # }
        )

        # self.assertEqual(
        #     df.expect_column_value_to_be_odd("all_odd"),
        #     {
        #         'exception_list': [],
        #         'success': True
        #     }
        # )


    def test_column_aggregate_expectation_decorator(self):

        # Create a new CustomPandasDataSet to 
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataSet(PandasDataSet):

            @PandasDataSet.column_aggregate_expectation
            def expect_column_median_to_be_odd(self, column):
                return {"success": column.median() % 2, "true_value": column.median(), "summary_obj": None}

        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,7,9],
            'all_even' : [2,4,6,8,10],
            'odd_missing' : [1,3,5,None,None],
            'mixed_missing' : [1,2,None,None,6],
            'mixed_missing_2' : [1,3,None,None,6],
            'all_missing' : [None,None,None,None,None,],
        })
        df.set_default_expectation_argument("output_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_odd"),
            {
                'true_value': 5,
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            {
                'true_value': 6,
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", output_format="SUMMARY"),
            {
                'true_value': 6,
                'success': False,
                'summary_obj': None
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", output_format="BOOLEAN_ONLY"),
            False
        )

        df.default_expectation_args["output_format"] = "BOOLEAN_ONLY"
        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            False
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", output_format="BASIC"),
            {
                'true_value': 6,
                'success': False
            }
        )

    def test_expectation_decorator_catch_exceptions(self):

        class CustomPandasDataSet(PandasDataSet):

            @PandasDataSet.column_map_expectation
            def expect_column_values_to_be_odd(self, column):
                return column.map(lambda x: x % 2 )

            @PandasDataSet.column_map_expectation
            def expectation_that_crashes_on_sixes(self, column):
                return column.map(lambda x: 1/(x-6) != "duck")


        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,5,5,7,9,9,9,11],
            'mostly_odd' : [1,3,5,7,9,2,4,1,3,5],
            'all_even' : [2,4,4,6,6,6,8,8,8,8],
            'odd_missing' : [1,3,5,None,None,None,None,1,3,None],
            'mixed_missing' : [1,3,5,None,None,2,4,1,3,None],
            'all_missing' : [None,None,None,None,None,None,None,None,None,None,],
        })
        df.set_default_expectation_argument("output_format", "COMPLETE")

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", catch_exceptions=False),
            {
                'success': True,
                'exception_list': [],
                'exception_index_list': [],
            }
        )

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", catch_exceptions=True),
            {
                'success': True,
                'exception_list': [],
                'exception_index_list': [],
                'raised_exception': False,
                'exception_traceback': None,
            }
        )

        with self.assertRaises(ZeroDivisionError):
            df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=False)

        result_obj = df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=True)
        comparison_obj = {
            'exception_list': None,
            'exception_index_list': None,
            'success': False,
            'raised_exception': True,
        }

        self.assertEqual(
            set(result_obj.keys()),
            set(list(comparison_obj.keys())+['exception_traceback']),
        )

        for k,v in comparison_obj.items():
            self.assertEqual(result_obj[k], v)

        self.assertEqual(
            result_obj["exception_traceback"].split('\n')[-1],
            "",
        )

        if sys.version_info[0] == 3:
            self.assertEqual(
                result_obj["exception_traceback"].split('\n')[-2],
                "ZeroDivisionError: division by zero",
            )

        else:
            self.assertEqual(
                result_obj["exception_traceback"].split('\n')[-2],
                "ZeroDivisionError: integer division or modulo by zero",
            )

        self.assertEqual(
            result_obj["exception_traceback"].split('\n')[-3],
            "    return column.map(lambda x: 1/(x-6) != \"duck\")",
        )


        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", output_format="BOOLEAN_ONLY", catch_exceptions=True),
            True
        )

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_even", output_format="BOOLEAN_ONLY", catch_exceptions=True),
            False
        )

        # with self.assertRaises(ZeroDivisionError):
        #     df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=False)

if __name__ == "__main__":
    unittest.main()
