import unittest
import json
# import hashlib
import datetime
import time
# import numpy as np
import inspect
import os
import subprocess

import great_expectations as ge
from .util import assertDeepAlmostEqual

def get_system_command_result(command_str):
    p = subprocess.Popen(
        command_str.split(' '),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    output, errors = p.communicate()

    return {
        "output" : output.decode('utf-8'),
        "errors" : errors.decode('utf-8')
    }

class TestCLI(unittest.TestCase):

    def test_cli_arguments(self):
        filepath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        #!!! These tests require you to uninstall and re-install great_expectations.
        #!!! Otherwise, they test the CLI, but not great_expectations itself.

        #print(get_system_command_result('python '+filepath+'/../bin/great_expectations '))
        self.assertEqual(
            get_system_command_result('python '+filepath+'/../bin/great_expectations '),
            {'output': '', 'errors': "usage: great_expectations [-h] {initialize,validate} ...\ngreat_expectations: error: invalid choice: '' (choose from 'initialize', 'validate')\n"}
        )

        # print get_system_command_result('python '+filepath+'/../bin/great_expectations validate')
        # self.assertEqual(
        #     get_system_command_result('python '+filepath+'/../bin/great_expectations validate'),
        #     {'output': '', 'errors': 'usage: great_expectations validate [-h] [--output_format OUTPUT_FORMAT]\n                                   [--catch_exceptions]\n                                   [--include_config INCLUDE_CONFIG]\n                                   [--only_return_failures]\n                                   [--custom_dataset_module CUSTOM_DATASET_MODULE]\n                                   [--custom_dataset_class CUSTOM_DATASET_CLASS]\n                                   data_set expectations_config_file\ngreat_expectations validate: error: too few arguments\n'}
        # )

        command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/test_sets/Titanic.csv '+filepath+'/test_sets/titanic_expectations.json'
        print(command_str)

        try:
          result = get_system_command_result(command_str)
          json_result = json.loads(result["output"])
        except ValueError as ve:
          print ("=== Result ==================================================")
          print (result)
          print ("=== Error ===================================================")
          print(ve)
          json_result = {}

        print("^"*80)
        print(json.dumps(json_result, indent=2))
        self.maxDiff = None

        assertDeepAlmostEqual(self,
            json_result,
            {
              "results": [
                {
                  "kwargs": {
                    "column": "Name", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "kwargs": {
                    "column": "PClass", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "kwargs": {
                    "column": "Age", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "kwargs": {
                    "column": "Sex", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "kwargs": {
                    "column": "Survived", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "kwargs": {
                    "column": "SexCode", 
                    "output_format": "SUMMARY"
                  }, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "success": True, 
                  "raised_exception": False
                }, 
                {
                  "exception_traceback": None, 
                  "true_value": 30.397989417989415, 
                  "expectation_type": "expect_column_mean_to_be_between", 
                  "success": True, 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Age", 
                    "max_value": 40, 
                    "output_format": "SUMMARY", 
                    "min_value": 20
                  }, 
                  "summary_obj": {
                    "element_count": 1313, 
                    "missing_percent": 0.4242193450114242, 
                    "missing_count": 557
                  }
                }, 
                {
                  "exception_traceback": None, 
                  "summary_obj": {
                    "unexpected_percent_nonmissing": 0.0,
                    "missing_count": 557, 
                    "partial_unexpected_index_list": [],
                    "element_count": 1313, 
                    "unexpected_count": 0,
                    "missing_percent": 0.4242193450114242, 
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "partial_unexpected_counts": []
                  }, 
                  "success": True, 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Age", 
                    "max_value": 80, 
                    "output_format": "SUMMARY", 
                    "min_value": 0
                  }, 
                  "expectation_type": "expect_column_values_to_be_between"
                }, 
                {
                  "exception_traceback": None, 
                  "summary_obj": {
                    "unexpected_percent_nonmissing": 0.002284843869002285,
                    "missing_count": 0, 
                    "partial_unexpected_index_list": [
                      394, 
                      456, 
                      1195
                    ], 
                    "element_count": 1313, 
                    "unexpected_count": 3,
                    "missing_percent": 0.0, 
                    "unexpected_percent": 0.002284843869002285,
                    "partial_unexpected_list": [
                      "Downton (?Douton), Mr William James", 
                      "Jacobsohn Mr Samuel", 
                      "Seman Master Betros"
                    ], 
                    "partial_unexpected_counts": [
                      {"value": "Downton (?Douton), Mr William James",
                       "count": 1},
                      {"value": "Jacobsohn Mr Samuel",
                       "count": 1},
                      {"value": "Seman Master Betros",
                       "count": 1}
                    ]
                  }, 
                  "success": True, 
                  "raised_exception": False, 
                  "kwargs": {
                    "regex": "[A-Z][a-z]+(?: \\([A-Z][a-z]+\\))?, ", 
                    "column": "Name", 
                    "output_format": "SUMMARY", 
                    "mostly": 0.95
                  }, 
                  "expectation_type": "expect_column_values_to_match_regex"
                }, 
                {
                  "exception_traceback": None, 
                  "summary_obj": {
                    "unexpected_percent_nonmissing": 0.0007616146230007616,
                    "missing_count": 0, 
                    "partial_unexpected_index_list": [
                      456
                    ], 
                    "element_count": 1313, 
                    "unexpected_count": 1,
                    "missing_percent": 0.0, 
                    "unexpected_percent": 0.0007616146230007616,
                    "partial_unexpected_list": [
                      "*"
                    ], 
                    "partial_unexpected_counts": [
                      {"value": "*",
                       "count": 1}
                    ]
                  }, 
                  "success": False, 
                  "raised_exception": False,
                  "kwargs": {
                    "column": "PClass", 
                    "values_set": [
                      "1st", 
                      "2nd", 
                      "3rd"
                    ], 
                    "output_format": "SUMMARY"
                  }, 
                  "expectation_type": "expect_column_values_to_be_in_set"
                }
              ]
            }
        )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "summary_obj": {\n        "unexpected_percent": 0.0007616146230007616, \n        "partial_unexpected_list": [\n          "*"\n        ], \n        "unexpected_percent_nonmissing": 0.0007616146230007616, \n        "unexpected_count": 1\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -o=COMPLETE'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "unexpected_list": [\n        "*"\n      ], \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "COMPLETE", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "unexpected_index_list": [\n        456\n      ]\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -o=BOOLEAN_ONLY'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "BOOLEAN_ONLY", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -e'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "summary_obj": {\n        "unexpected_percent": 0.0007616146230007616, \n        "partial_unexpected_list": [\n          "*"\n        ], \n        "unexpected_percent_nonmissing": 0.0007616146230007616, \n        "unexpected_count": 1\n      }, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -e'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str)["output"],
        #     "{\n  \"results\": [\n    {\n      \"summary_obj\": {\n        \"unexpected_percent\": 0.0007616146230007616, \n        \"partial_unexpected_list\": [\n          \"*\"\n        ], \n        \"unexpected_percent_nonmissing\": 0.0007616146230007616, \n        \"unexpected_count\": 1\n      }, \n      \"expectation_type\": \"expect_column_values_to_be_in_set\", \n      \"success\": false, \n      \"kwargs\": {\n        \"column\": \"PClass\", \n        \"output_format\": \"BASIC\", \n        \"value_set\": [\n          \"1st\", \n          \"2nd\", \n          \"3rd\"\n        ]\n      }\n    }\n  ]\n}\n"
        # )
        #print(filepath)

        command_str = 'python ' + filepath + '/../bin/great_expectations validate ' \
                      + filepath + '/test_sets/Titanic.csv '\
                      + filepath + '/test_sets/titanic_custom_expectations.json -f -m='\
                      + filepath + '/test_fixtures/custom_dataset.py -c=CustomPandasDataSet'
        try:
          result = get_system_command_result(command_str)
          json_result = json.loads(result["output"])
        except ValueError as ve:
          print ("=== Result ==================================================")
          print (result)
          print ("=== Error ===================================================")
          print(ve)
          json_result = {}

        self.maxDiff = None
        print(json.dumps(json_result, indent=2))

        #Remove partial unexpected counts, because we can't guarantee that they'll be the same every time.
        del json_result["results"][0]["summary_obj"]['partial_unexpected_counts']
        self.assertEqual(
            json_result,
            {
              "results": [
                {
                  "exception_traceback": None, 
                  "summary_obj": {
                    "unexpected_percent_nonmissing": 0.5026656511805027,
                    "missing_count": 0, 
                    "partial_unexpected_index_list": [
                      0, 
                      5, 
                      6, 
                      7, 
                      8, 
                      9, 
                      11, 
                      12, 
                      13, 
                      14, 
                      15, 
                      18, 
                      20, 
                      21, 
                      22, 
                      23, 
                      27, 
                      31, 
                      32, 
                      33
                    ], 
                    "element_count": 1313, 
                    "unexpected_count": 660,
                    "missing_percent": 0.0, 
                    "unexpected_percent": 0.5026656511805027,
                    "partial_unexpected_list": [
                      "Allen, Miss Elisabeth Walton", 
                      "Anderson, Mr Harry", 
                      "Andrews, Miss Kornelia Theodosia", 
                      "Andrews, Mr Thomas, jr", 
                      "Appleton, Mrs Edward Dale (Charlotte Lamson)", 
                      "Artagaveytia, Mr Ramon", 
                      "Astor, Mrs John Jacob (Madeleine Talmadge Force)", 
                      "Aubert, Mrs Leontine Pauline", 
                      "Barkworth, Mr Algernon H", 
                      "Baumann, Mr John D", 
                      "Baxter, Mrs James (Helene DeLaudeniere Chaput)", 
                      "Beckwith, Mr Richard Leonard", 
                      "Behr, Mr Karl Howell", 
                      "Birnbaum, Mr Jakob", 
                      "Bishop, Mr Dickinson H", 
                      "Bishop, Mrs Dickinson H (Helen Walton)", 
                      "Bonnell, Miss Caroline", 
                      "Bowerman, Miss Elsie Edith", 
                      "Bradley, Mr George", 
                      "Brady, Mr John Bertram"
                    ], 
                  }, 
                  "success": False, 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Name", 
                    "output_format": "SUMMARY"
                  }, 
                  "expectation_type": "expect_column_values_to_have_odd_lengths"
                }
              ]
            }

        )
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str)["output"],
        #     {
        #       "results": [
        #         {
        #           "exception_traceback": None, 
        #           "expectation_type": "expect_column_values_to_be_in_set", 
        #           "success": False,
        #           "raised_exception": False, 
        #           "kwargs": {
        #             "column": "PClass", 
        #             "output_format": "BASIC", 
        #             "include_config": False, 
        #             "value_set": [
        #               "1st", 
        #               "2nd", 
        #               "3rd"
        #             ]
        #           }, 
        #           "summary_obj": {
        #             "unexpected_percent": 0.0007616146230007616,
        #             "partial_unexpected_list": [
        #               "*"
        #             ], 
        #             "unexpected_percent_nonmissing": 0.0007616146230007616,
        #             "unexpected_count": 1
        #           }
        #         }
        #       ]
        #     }
        # )

if __name__ == "__main__":
    unittest.main()
