import json
import hashlib
import datetime
import numpy as np

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

    def test_expectation_meta(self):
        df = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        result = df.expect_column_median_to_be_between('x', 2, 2, meta={"notes": "This expectation is for lolz."})
        k = 0
        self.assertEqual(result['success'], True)
        config = df.get_expectations_config()
        for expectation_config in config['expectations']:
            if expectation_config['expectation_type'] == 'expect_column_median_to_be_between':
                k += 1
                self.assertEqual(
                    expectation_config['meta'],
                    {"notes": "This expectation is for lolz."}
                )

        self.assertEqual(k,1)

    def test_expectation_meta_notes(self):
        df = ge.dataset.PandasDataSet({
            'x': [1, 2, 4],
            'y': [1, 2, 5],
            'z': ['hello', 'jello', 'mello'],
        })

        result = df.expect_column_median_to_be_between('x', 2, 2, meta_notes="This expectation is also for lolz.")
        k = 0
        self.assertEqual(result['success'], True)
        config = df.get_expectations_config()
        for expectation_config in config['expectations']:
            if expectation_config['expectation_type'] == 'expect_column_median_to_be_between':
                k += 1
                self.assertEqual(
                    expectation_config['meta'],
                    {"notes": "This expectation is also for lolz."}
                )

        self.assertEqual(k, 1)



    #TODO: !!! Add tests for expectation and column_expectation
    #TODO: !!! Add tests for save_expectation

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

if __name__ == "__main__":
    unittest.main()
