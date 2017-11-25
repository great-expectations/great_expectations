import json
import tempfile
import shutil

import pandas as pd
import great_expectations as ge

import unittest

class TestDataset(unittest.TestCase):

    def test_dataset(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        # print D._expectations_config.keys()
        # print json.dumps(D._expectations_config, indent=2)

        self.assertEqual(
            D._expectations_config,
            {
                "dataset_name" : None,
                "expectations" : [{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "x" }
                },{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "y" }
                },{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "z" }
                }]
            }
        )

        self.maxDiff = None
        self.assertEqual(
            D.get_expectations_config(),
            {
                "dataset_name" : None,
                "expectations" : [{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "x" }
                },{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "y" }
                },{
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : { "column" : "z" }
                }]
            }
        )


        #!!! Add tests for expectation and column_expectation
        #!!! Add tests for save_expectation

    def test_set_default_expectation_argument(self):
        df = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        self.assertEqual(
            df.get_default_expectation_arguments(),
            {
                "include_config" : False,
                "catch_exceptions" : False,
                "output_format" : 'BASIC',
            }
        )

        df.set_default_expectation_argument("output_format", "SUMMARY")

        self.assertEqual(
            df.get_default_expectation_arguments(),
            {
                "include_config" : False,
                "catch_exceptions" : False,
                "output_format" : 'SUMMARY',
            }
        )

    def test_get_and_save_expectation_config(self):
        directory_name = tempfile.mkdtemp()

        df = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })
        df.expect_column_values_to_be_in_set('x', [1,2,4])
        df.expect_column_values_to_be_in_set('y', [1,2,4])
        df.expect_column_values_to_match_regex('z', 'ello')

        ### First test set ###

        output_config = {
          "expectations": [
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "x"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "y"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "z"
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_be_in_set", 
              "kwargs": {
                "column": "x", 
                "values_set": [
                  1, 
                  2, 
                  4
                ]
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_match_regex", 
              "kwargs": {
                "column": "z", 
                "regex": "ello"
              }
            }
          ], 
          "dataset_name": None
        }

        self.assertEqual(
            df.get_expectations_config(),
            output_config,
        )

        df.save_expectations_config(directory_name+'/temp1.json')
        temp_file = open(directory_name+'/temp1.json')
        self.assertEqual(
            json.load(temp_file),
            output_config,
        )
        temp_file.close()

        ### Second test set ###

        output_config = {
          "expectations": [
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "x"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "y"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "z"
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_be_in_set", 
              "kwargs": {
                "column": "x", 
                "values_set": [
                  1, 
                  2, 
                  4
                ]
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_be_in_set", 
              "kwargs": {
                "column": "y", 
                "values_set": [
                  1, 
                  2, 
                  4
                ]
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_match_regex", 
              "kwargs": {
                "column": "z", 
                "regex": "ello"
              }
            }
          ], 
          "dataset_name": None
        }

        self.assertEqual(
            df.get_expectations_config(
                discard_failed_expectations=False
            ),
            output_config
        )

        df.save_expectations_config(
          directory_name+'/temp2.json',
          discard_failed_expectations=False
        )
        temp_file = open(directory_name+'/temp2.json')
        self.assertEqual(
            json.load(temp_file),
            output_config,
        )
        temp_file.close()

        ### Third test set ###

        output_config = {
          "expectations": [
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "x"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "y"
              }
            }, 
            {
              "expectation_type": "expect_column_to_exist", 
              "kwargs": {
                "column": "z"
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_be_in_set", 
              "kwargs": {
                "column": "x", 
                "values_set": [
                  1, 
                  2, 
                  4
                ], 
                "output_format": "BASIC"
              }
            }, 
            {
              "expectation_type": "expect_column_values_to_match_regex", 
              "kwargs": {
                "column": "z", 
                "regex": "ello", 
                "output_format": "BASIC"
              }
            }
          ], 
          "dataset_name": None
        }

        self.assertEqual(
            df.get_expectations_config(
                discard_output_format_kwargs=False,
                discard_include_configs_kwargs=False,
                discard_catch_exceptions_kwargs=False,
            ),
            output_config
        )

        df.save_expectations_config(
          directory_name+'/temp3.json',
          discard_output_format_kwargs=False,
          discard_include_configs_kwargs=False,
          discard_catch_exceptions_kwargs=False,
        )
        temp_file = open(directory_name+'/temp3.json')
        self.assertEqual(
            json.load(temp_file),
            output_config,
        )
        temp_file.close()

        # Clean up the output directory
        shutil.rmtree(directory_name)

    def test_format_column_map_output(self):
        df = ge.dataset.PandasDataSet({
            "x" : list("abcdefghijklmnopqrstuvwxyz")
        })

        success = True
        element_count = 20
        nonnull_values = pd.Series(range(15))
        nonnull_count = 15
        boolean_mapped_success_values = pd.Series([True for i in range(15)])
        success_count = 15
        exception_list = []
        exception_index_list = []

        self.assertEqual(
            df.format_column_map_output(
                "BOOLEAN_ONLY",
                success,
                element_count,
                nonnull_values, nonnull_count,
                boolean_mapped_success_values, success_count,
                exception_list, exception_index_list
            ),
            True
        )

        self.assertEqual(
            df.format_column_map_output(
                "BASIC",
                success,
                element_count,
                nonnull_values, nonnull_count,
                boolean_mapped_success_values, success_count,
                exception_list, exception_index_list
            ),
            {
                'success': True,
                'summary_obj': {
                    'exception_percent': 0.0,
                    'partial_exception_list': [],
                    'exception_percent_nonmissing': 0.0,
                    'exception_count': 0
                }
            }
        )

        self.assertEqual(
            df.format_column_map_output(
                "COMPLETE",
                success,
                element_count,
                nonnull_values, nonnull_count,
                boolean_mapped_success_values, success_count,
                exception_list, exception_index_list
            ),
            {
                'success': True,
                'exception_list': [],
                'exception_index_list': [],
            }
        )

        self.assertEqual(
            df.format_column_map_output(
                "SUMMARY",
                success,
                element_count,
                nonnull_values, nonnull_count,
                boolean_mapped_success_values, success_count,
                exception_list, exception_index_list
            ),
            {
                'success': True,
                'summary_obj': {
                    'element_count': 20,
                    'exception_count': 0,
                    'exception_percent': 0.0,
                    'exception_percent_nonmissing': 0.0,
                    'missing_count': 5,
                    'missing_percent': 0.25,
                    'partial_exception_counts': {},
                    'partial_exception_index_list': [],
                    'partial_exception_list': []
                }
            }
        )



    def test_calc_map_expectation_success(self):
        df = ge.dataset.PandasDataSet({
            "x" : list("abcdefghijklmnopqrstuvwxyz")
        })
        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=10,
                nonnull_count=10,
                mostly=None
            ),
            (True, 1.0)
        )

        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=90,
                nonnull_count=100,
                mostly=.9
            ),
            (True, .9)
        )

        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=90,
                nonnull_count=100,
                mostly=.8
            ),
            (True, .9)
        )

        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=80,
                nonnull_count=100,
                mostly=.9
            ),
            (False, .8)
        )

        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=0,
                nonnull_count=0,
                mostly=None
            ),
            (True, None)
        )

        self.assertEqual(
            df.calc_map_expectation_success(
                success_count=0,
                nonnull_count=100,
                mostly=None
            ),
            (False, 0.0)
        )

    def test_test_expectation_function(self):
        pass

    def test_test_column_map_expectation_function(self):
        D = ge.dataset.PandasDataSet({
            'x' : [1,3,5,7,9],
            'y' : [1,2,5,7,9],
        })
        def is_odd(x):
            return x % 2 == 1

        print D.test_column_map_expectation_function(is_odd, column='x', output_format="BOOLEAN_ONLY")
        self.assertEqual(
            D.test_column_map_expectation_function(is_odd, column='x', output_format="BOOLEAN_ONLY"),
            True
        )
        self.assertEqual(
            D.test_column_map_expectation_function(is_odd, column='x', output_format="BOOLEAN_ONLY", mostly=.7),
            True
        )

        # self.assertEqual(
        #     D.test_column_map_expectation_function(is_odd, column='x', output_format="SUMMARY"),
        #     {
        #         "expectation_type": "is_odd",
        #         "kwargs" : {
        #             "column": "x"
        #         }
        #     }
        # )
        

    def test_test_column_aggregate_expectation_function(self):
        pass


if __name__ == "__main__":
    unittest.main()
