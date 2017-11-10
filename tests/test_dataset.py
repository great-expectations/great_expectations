import json
import hashlib
import datetime
import numpy as np
import tempfile
# import os
import shutil

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
        self.assertEqual(
            json.load(open(directory_name+'/temp1.json')),
            output_config,
        )


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
        self.assertEqual(
            json.load(open(directory_name+'/temp2.json')),
            output_config,
        )

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
        self.assertEqual(
            json.load(open(directory_name+'/temp3.json')),
            output_config,
        )

        # Clean up the output directory
        shutil.rmtree(directory_name)

if __name__ == "__main__":
    unittest.main()
