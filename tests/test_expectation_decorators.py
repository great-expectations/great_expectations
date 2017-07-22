# import json
# import hashlib
# import datetime
# import numpy as np
# import random
# import os
# import sys
# import inspect

# from nose.tools import *
import unittest
import great_expectations as ge
reload(ge)
# from great_expectations.dataset import PandasDataSet
PandasDataSet = ge.dataset.PandasDataSet

# from ge.decorators import expectation, column_map_expectation, column_aggregate_expectation

class TestExpectationDecorators(unittest.TestCase):
    
    def test_column_map_expectation_decorator(self):

        # Create a new CustomPandasDataSet to 
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataSet(PandasDataSet):

            @PandasDataSet.column_map_expectation
            def expect_column_value_to_be_odd(self, series):
                return series.map(lambda x: x % 2 )


        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,5,5,7,9,9,9,11],
            'mostly_odd' : [1,3,5,7,9,2,4,1,3,5],
            'all_even' : [2,4,4,6,6,6,8,8,8,8],
            'odd_missing' : [1,3,5,None,None,None,None,1,3,None],
            'mixed_missing' : [1,3,5,None,None,2,4,1,3,None],
            'all_missing' : [None,None,None,None,None,None,None,None,None,None,],
        })

        self.assertEqual(
            df.expect_column_value_to_be_odd("all_odd"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("all_missing"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("odd_missing"),
            {
                'exception_list': [],
                'exception_index_list': [],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("mixed_missing"),
            {
                'exception_list': [2,4],
                'exception_index_list': [5,6],
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("mostly_odd"),
            {
                'exception_list': [2, 4],
                'exception_index_list': [5, 6],
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("mostly_odd", mostly=.6),
            {
                'exception_list': [2, 4],
                'exception_index_list': [5, 6],
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_value_to_be_odd("mostly_odd", output_format="BOOLEAN_ONLY"),
            False
        )

        df.default_expectation_args["output_format"] = "BOOLEAN_ONLY"

        self.assertEqual(
            df.expect_column_value_to_be_odd("mostly_odd"),
            False
        )

        df.default_expectation_args["output_format"] = "BASIC"

        # df.expect_column_value_to_be_odd("mostly_odd", include_config=True)

        self.assertEqual(
            df.expect_column_value_to_be_odd("mostly_odd", include_config=True),
            {
                'exception_list': [2, 4],
                'exception_index_list': [5, 6],
                'success': False,
                'expectation_config' : {
                    'expectation_type' : 'expect_column_value_to_be_odd',
                    'kwargs' : {
                        'column' : 'mostly_odd'
                    }
                }
            }
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
            def expect_column_median_to_be_odd(self, series):
                return series.median() % 2, series.median()

        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,7,9],
            'all_even' : [2,4,6,8,10],
            'odd_missing' : [1,3,5,None,None],
            'mixed_missing' : [1,2,None,None,6],
            'mixed_missing_2' : [1,3,None,None,6],
            'all_missing' : [None,None,None,None,None,],
        })

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
                'success': False
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
