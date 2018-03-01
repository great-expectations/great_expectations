from __future__ import division

import sys
import unittest
from great_expectations.dataset import PandasDataSet, MetaPandasDataSet


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
            'all_missing' : [None,None,None,None,None,None,None,None,None,None]
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_values_to_be_odd("all_odd"),
            {'result_obj': {'element_count': 10,
                            'missing_count': 0,
                            'missing_percent': 0.0,
                            'partial_unexpected_counts': [],
                            'partial_unexpected_index_list': [],
                            'partial_unexpected_list': [],
                            'unexpected_count': 0,
                            'unexpected_index_list': [],
                            'unexpected_list': [],
                            'unexpected_percent': 0.0,
                            'unexpected_percent_nonmissing': 0.0},
             'success': True}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("all_missing"),
            {'result_obj': {'element_count': 10,
                            'missing_count': 10,
                            'missing_percent': 1,
                            'partial_unexpected_counts': [],
                            'partial_unexpected_index_list': [],
                            'partial_unexpected_list': [],
                            'unexpected_count': 0,
                            'unexpected_index_list': [],
                            'unexpected_list': [],
                            'unexpected_percent': 0.0,
                            'unexpected_percent_nonmissing': None},
             'success': True}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("odd_missing"),
            {'result_obj': {'element_count': 10,
                            'missing_count': 5,
                            'missing_percent': 0.5,
                            'partial_unexpected_counts': [],
                            'partial_unexpected_index_list': [],
                            'partial_unexpected_list': [],
                            'unexpected_count': 0,
                            'unexpected_index_list': [],
                            'unexpected_list': [],
                            'unexpected_percent': 0.0,
                            'unexpected_percent_nonmissing': 0.0},
             'success': True}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mixed_missing"),
            {'result_obj': {'element_count': 10,
                            'missing_count': 3,
                            'missing_percent': 0.3,
                            'partial_unexpected_counts': [{'value': 2., 'count': 1}, {'value': 4., 'count': 1}],
                            'partial_unexpected_index_list': [5, 6],
                            'partial_unexpected_list': [2., 4.],
                            'unexpected_count': 2,
                            'unexpected_index_list': [5, 6],
                            'unexpected_list': [2., 4.],
                            'unexpected_percent': 0.2,
                            'unexpected_percent_nonmissing': 2/7},
             'success': False}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd"),
            {'result_obj': {'element_count': 10,
                            'missing_count': 0,
                            'missing_percent': 0,
                            'partial_unexpected_counts': [{'value': 2., 'count': 1}, {'value': 4., 'count': 1}],
                            'partial_unexpected_index_list': [5, 6],
                            'partial_unexpected_list': [2., 4.],
                            'unexpected_count': 2,
                            'unexpected_index_list': [5, 6],
                            'unexpected_list': [2., 4.],
                            'unexpected_percent': 0.2,
                            'unexpected_percent_nonmissing': 0.2},
             'success': False}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", mostly=.6),
            {'result_obj': {'element_count': 10,
                            'missing_count': 0,
                            'missing_percent': 0,
                            'partial_unexpected_counts': [{'value': 2., 'count': 1}, {'value': 4., 'count': 1}],
                            'partial_unexpected_index_list': [5, 6],
                            'partial_unexpected_list': [2., 4.],
                            'unexpected_count': 2,
                            'unexpected_index_list': [5, 6],
                            'unexpected_list': [2., 4.],
                            'unexpected_percent': 0.2,
                            'unexpected_percent_nonmissing': 0.2},
             'success': True}
        )

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", result_format="BOOLEAN_ONLY"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BASIC"

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd", include_config=True),
            {
                "expectation_kwargs": {
                    "column": "mostly_odd", 
                    "result_format": "BASIC"
                },
                'result_obj': {'element_count': 10,
                               'missing_count': 0,
                               'missing_percent': 0,
                               'partial_unexpected_list': [2., 4.],
                               'unexpected_count': 2,
                               'unexpected_percent': 0.2,
                               'unexpected_percent_nonmissing': 0.2},
                'success': False,
                "expectation_type": "expect_column_values_to_be_odd"
            }
        )



    def test_column_aggregate_expectation_decorator(self):

        # Create a new CustomPandasDataSet to 
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataSet(PandasDataSet):

            @PandasDataSet.column_aggregate_expectation
            def expect_column_median_to_be_odd(self, column):
                return {"success": column.median() % 2, "result_obj": {"observed_value": column.median()}}

        df = CustomPandasDataSet({
            'all_odd' : [1,3,5,7,9],
            'all_even' : [2,4,6,8,10],
            'odd_missing' : [1,3,5,None,None],
            'mixed_missing' : [1,2,None,None,6],
            'mixed_missing_2' : [1,3,None,None,6],
            'all_missing' : [None,None,None,None,None,],
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_odd"),
            {
                'result_obj': {'observed_value': 5, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            {
                'result_obj': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", result_format="SUMMARY"),
            {
                'result_obj': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", result_format="BOOLEAN_ONLY"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"
        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            {'success': False}
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even", result_format="BASIC"),
            {
                'result_obj': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
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
            'all_missing' : [None,None,None,None,None,None,None,None,None,None]
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", catch_exceptions=False),
            {'result_obj': {'element_count': 10,
                            'missing_count': 0,
                            'missing_percent': 0.0,
                            'partial_unexpected_counts': [],
                            'partial_unexpected_index_list': [],
                            'partial_unexpected_list': [],
                            'unexpected_count': 0,
                            'unexpected_index_list': [],
                            'unexpected_list': [],
                            'unexpected_percent': 0.0,
                            'unexpected_percent_nonmissing': 0.0},
             'success': True}
        )

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", catch_exceptions=True),
            {'result_obj': {'element_count': 10,
                            'missing_count': 0,
                            'missing_percent': 0.0,
                            'partial_unexpected_counts': [],
                            'partial_unexpected_index_list': [],
                            'partial_unexpected_list': [],
                            'unexpected_count': 0,
                            'unexpected_index_list': [],
                            'unexpected_list': [],
                            'unexpected_percent': 0.0,
                            'unexpected_percent_nonmissing': 0.0},
                'success': True,
                'raised_exception': False,
                'exception_traceback': None,
            }
        )

        with self.assertRaises(ZeroDivisionError):
            df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=False)

        result_obj = df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=True)
        comparison_obj = {
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
                "ZeroDivisionError: division by zero"
            )

        else:
            self.assertEqual(
                result_obj["exception_traceback"].split('\n')[-2].split(':')[0],
                "ZeroDivisionError"
            )

        self.assertEqual(
            result_obj["exception_traceback"].split('\n')[-3],
            "    return column.map(lambda x: 1/(x-6) != \"duck\")",
        )


        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_odd", result_format="BOOLEAN_ONLY", catch_exceptions=True),
            {'success': True}
        )

        self.assertEqual(
            df.expectation_that_crashes_on_sixes("all_even", result_format="BOOLEAN_ONLY", catch_exceptions=True),
            {'success': False}
        )

        # with self.assertRaises(ZeroDivisionError):
        #     df.expectation_that_crashes_on_sixes("all_even", catch_exceptions=False)

if __name__ == "__main__":
    unittest.main()
