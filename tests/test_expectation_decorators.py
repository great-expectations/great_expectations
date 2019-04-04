from __future__ import division

import unittest
from great_expectations.data_asset import DataAsset
from great_expectations.dataset import PandasDataset, MetaPandasDataset


class ExpectationOnlyDataAsset(DataAsset):

    @DataAsset.expectation([])
    def no_op_expectation(self, result_format=None, include_config=False, catch_exceptions=None, meta=None):
        return {"success": True}

    @DataAsset.expectation(['value'])
    def no_op_value_expectation(self, value=None,
                                result_format=None, include_config=False, catch_exceptions=None, meta=None):
        return {"success": True}

    @DataAsset.expectation([])
    def exception_expectation(self,
                              result_format=None, include_config=False, catch_exceptions=None, meta=None):
        raise ValueError("Gotcha!")


class TestExpectationDecorators(unittest.TestCase):

    def test_expectation_decorator_build_config(self):
        eds = ExpectationOnlyDataAsset()
        eds.no_op_expectation()
        eds.no_op_value_expectation('a')

        config = eds.get_expectations_config()
        self.assertEqual({'expectation_type': 'no_op_expectation', 'kwargs': {}},
                         config['expectations'][0])

        self.assertEqual({'expectation_type': 'no_op_value_expectation', 'kwargs': {'value': 'a'}},
                         config['expectations'][1])

    def test_expectation_decorator_include_config(self):
        eds = ExpectationOnlyDataAsset()
        out = eds.no_op_value_expectation('a', include_config=True)

        self.assertEqual({'expectation_type': 'no_op_value_expectation',
                          'kwargs': {'value': 'a', 'result_format': 'BASIC'}
                          },
                         out['expectation_config'])

    def test_expectation_decorator_meta(self):
        metadata = {'meta_key': 'meta_value'}
        eds = ExpectationOnlyDataAsset()
        out = eds.no_op_value_expectation('a', meta=metadata)
        config = eds.get_expectations_config()

        self.assertEqual({'success': True,
                          'meta': metadata},
                         out)

        self.assertEqual({'expectation_type': 'no_op_value_expectation',
                          'kwargs': {'value': 'a'},
                          'meta': metadata},
                         config['expectations'][0])

    def test_expectation_decorator_catch_exceptions(self):
        eds = ExpectationOnlyDataAsset()

        # Confirm that we would raise an error without catching exceptions
        with self.assertRaises(ValueError):
            eds.exception_expectation(catch_exceptions=False)

        # Catch exceptions and validate results
        out = eds.exception_expectation(catch_exceptions=True)
        self.assertEqual(True,
                         out['exception_info']['raised_exception'])

        # Check only the first and last line of the traceback, since formatting can be platform dependent.
        self.assertEqual('Traceback (most recent call last):',
                         out['exception_info']['exception_traceback'].split('\n')[0])
        self.assertEqual('ValueError: Gotcha!',
                         out['exception_info']['exception_traceback'].split('\n')[-2])

    def test_pandas_column_map_decorator_partial_exception_counts(self):
        df = PandasDataset({'a': [0, 1, 2, 3, 4]})
        out = df.expect_column_values_to_be_between('a', 3, 4,
                                                    result_format={'result_format': 'COMPLETE', 'partial_unexpected_count': 1})

        self.assertTrue(1, len(out['result']['partial_unexpected_counts']))
        self.assertTrue(3, len(out['result']['unexpected_list']))

    def test_column_map_expectation_decorator(self):

        # Create a new CustomPandasDataset to
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataset(PandasDataset):

            @MetaPandasDataset.column_map_expectation
            def expect_column_values_to_be_odd(self, column):
                return column.map(lambda x: x % 2)

            @MetaPandasDataset.column_map_expectation
            def expectation_that_crashes_on_sixes(self, column):
                return column.map(lambda x: (x-6)/0 != "duck")

        df = CustomPandasDataset({
            'all_odd': [1, 3, 5, 5, 5, 7, 9, 9, 9, 11],
            'mostly_odd': [1, 3, 5, 7, 9, 2, 4, 1, 3, 5],
            'all_even': [2, 4, 4, 6, 6, 6, 8, 8, 8, 8],
            'odd_missing': [1, 3, 5, None, None, None, None, 1, 3, None],
            'mixed_missing': [1, 3, 5, None, None, 2, 4, 1, 3, None],
            'all_missing': [None, None, None, None, None, None, None, None, None, None]
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_values_to_be_odd("all_odd"),
            {'result': {'element_count': 10,
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
            {'result': {'element_count': 10,
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
            {'result': {'element_count': 10,
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
            {'result': {'element_count': 10,
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
            {'result': {'element_count': 10,
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
            {'result': {'element_count': 10,
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
            df.expect_column_values_to_be_odd(
                "mostly_odd", result_format="BOOLEAN_ONLY"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"

        self.assertEqual(
            df.expect_column_values_to_be_odd("mostly_odd"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BASIC"

        self.assertEqual(
            df.expect_column_values_to_be_odd(
                "mostly_odd", include_config=True),
            {
                "expectation_config": {
                    "expectation_type": "expect_column_values_to_be_odd",
                    "kwargs": {
                        "column": "mostly_odd",
                        "result_format": "BASIC"
                    }
                },
                'result': {'element_count': 10,
                           'missing_count': 0,
                           'missing_percent': 0,
                           'partial_unexpected_list': [2., 4.],
                           'unexpected_count': 2,
                           'unexpected_percent': 0.2,
                           'unexpected_percent_nonmissing': 0.2},
                'success': False,
            }
        )

    def test_column_aggregate_expectation_decorator(self):

        # Create a new CustomPandasDataset to
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataset(PandasDataset):

            @PandasDataset.column_aggregate_expectation
            def expect_column_median_to_be_odd(self, column):
                return {"success": column.median() % 2, "result": {"observed_value": column.median()}}

        df = CustomPandasDataset({
            'all_odd': [1, 3, 5, 7, 9],
            'all_even': [2, 4, 6, 8, 10],
            'odd_missing': [1, 3, 5, None, None],
            'mixed_missing': [1, 2, None, None, 6],
            'mixed_missing_2': [1, 3, None, None, 6],
            'all_missing': [None, None, None, None, None, ],
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_odd"),
            {
                'result': {'observed_value': 5, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': True
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            {
                'result': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd(
                "all_even", result_format="SUMMARY"),
            {
                'result': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': False
            }
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd(
                "all_even", result_format="BOOLEAN_ONLY"),
            {'success': False}
        )

        df.default_expectation_args["result_format"] = "BOOLEAN_ONLY"
        self.assertEqual(
            df.expect_column_median_to_be_odd("all_even"),
            {'success': False}
        )

        self.assertEqual(
            df.expect_column_median_to_be_odd(
                "all_even", result_format="BASIC"),
            {
                'result': {'observed_value': 6, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0},
                'success': False
            }
        )

    def test_column_pair_map_expectation_decorator(self):

        # Create a new CustomPandasDataset to
        # (1) Prove that custom subclassing works, AND
        # (2) Test expectation business logic without dependencies on any other functions.
        class CustomPandasDataset(PandasDataset):

            @PandasDataset.column_pair_map_expectation
            def expect_column_pair_values_to_be_different(self,
                                                          column_A,
                                                          column_B,
                                                          keep_missing="either",
                                                          output_format=None, include_config=False, catch_exceptions=None
                                                          ):
                return column_A != column_B

        df = CustomPandasDataset({
            'all_odd': [1, 3, 5, 7, 9],
            'all_even': [2, 4, 6, 8, 10],
            'odd_missing': [1, 3, 5, None, None],
            'mixed_missing': [1, 2, None, None, 6],
            'mixed_missing_2': [1, 3, None, None, 6],
            'all_missing': [None, None, None, None, None, ],
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd", "all_even"),
            {
                "success": True,
                "result": {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "unexpected_list": [],
                    "unexpected_index_list": [],
                    "partial_unexpected_list": [],
                    "partial_unexpected_index_list": [],
                    "partial_unexpected_counts": [],
                }
            }
        )

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "all_even",
                ignore_row_if="both_values_are_missing",
            ),
            {
                'success': True,
                'result': {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "unexpected_list": [],
                    "unexpected_index_list": [],
                    "partial_unexpected_list": [],
                    "partial_unexpected_index_list": [],
                    "partial_unexpected_counts": [],
                }
            }
        )

        self.maxDiff = None
        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd", "odd_missing"),
            {
                'success': False,
                'result': {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 3,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.6,
                    "unexpected_percent_nonmissing": 0.6,
                    "unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "partial_unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_counts": [
                        {'count': 1, 'value': [1, 1.0]},
                        {'count': 1, 'value': [3, 3.0]},
                        {'count': 1, 'value': [5, 5.0]}
                    ]
                }
            }
        )

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "odd_missing",
                ignore_row_if="both_values_are_missing"
            ),
            {
                'success': False,
                'result': {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 3,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.6,
                    "unexpected_percent_nonmissing": 0.6,
                    "unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "partial_unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_counts": [
                        {'count': 1, 'value': [1, 1.0]},
                        {'count': 1, 'value': [3, 3.0]},
                        {'count': 1, 'value': [5, 5.0]}
                    ]
                }
            }
        )

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "odd_missing",
                ignore_row_if="either_value_is_missing"
            ),
            {
                'success': False,
                'result': {
                    "element_count": 5,
                    "missing_count": 2,
                    "unexpected_count": 3,
                    "missing_percent": 0.4,
                    "unexpected_percent": 0.6,
                    "unexpected_percent_nonmissing": 1.0,
                    "unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_list": [[1, 1], [3, 3], [5, 5]],
                    "partial_unexpected_index_list": [0, 1, 2],
                    "partial_unexpected_counts": [
                        {'count': 1, 'value': [1, 1.0]},
                        {'count': 1, 'value': [3, 3.0]},
                        {'count': 1, 'value': [5, 5.0]}
                    ]
                }
            }
        )

        # print json.dumps(
        #     df.expect_column_pair_values_to_be_different(
        #         "all_missing",
        #         "odd_missing",
        #         ignore_row_if="never"
        #     ),
        #     indent=2
        # )
        # self.assertEqual(
        #     df.expect_column_pair_values_to_be_different(
        #         "all_missing",
        #         "odd_missing",
        #         ignore_row_if="never"
        #     ),
        #     {
        #         'success' : True,
        #         'result': {
        #             "element_count": 5,
        #             "missing_count": 0,
        #             "unexpected_count": 2,
        #             "missing_percent": 0.0,
        #             "unexpected_percent": 0.4,
        #             "unexpected_percent_nonmissing": 0.4,
        #             "unexpected_list": [[None, None],[None, None]],
        #             "unexpected_index_list": [3,4],
        #             "partial_unexpected_list": [[None, None],[None, None]],
        #             "partial_unexpected_index_list": [3,4],
        #             "partial_unexpected_counts": [
        #                 {'count': 2, 'value': [[None, None]]}
        #             ]
        #         }
        #     }
        # )

        with self.assertRaises(ValueError):
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "odd_missing",
                ignore_row_if="blahblahblah"
            )

        # Test SUMMARY, BASIC, and BOOLEAN_ONLY output_formats
        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "all_even",
                result_format="SUMMARY"
            ),
            {
                "success": True,
                "result": {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_list": [],
                    "partial_unexpected_index_list": [],
                    "partial_unexpected_counts": [],
                }
            }
        )

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "all_even",
                result_format="BASIC"
            ),
            {
                "success": True,
                "result": {
                    "element_count": 5,
                    "missing_count": 0,
                    "unexpected_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_list": [],
                }
            }
        )

        self.assertEqual(
            df.expect_column_pair_values_to_be_different(
                "all_odd",
                "all_even",
                result_format="BOOLEAN_ONLY"
            ),
            {
                "success": True,
            }
        )


if __name__ == "__main__":
    unittest.main()
