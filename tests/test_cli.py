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
from .test_utils import assertDeepAlmostEqual

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
        #     {'output': '', 'errors': 'usage: great_expectations validate [-h] [--result_format result_format]\n                                   [--catch_exceptions]\n                                   [--include_config INCLUDE_CONFIG]\n                                   [--only_return_failures]\n                                   [--custom_dataset_module CUSTOM_DATASET_MODULE]\n                                   [--custom_dataset_class CUSTOM_DATASET_CLASS]\n                                   data_set expectations_config_file\ngreat_expectations validate: error: too few arguments\n'}
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

        # print("^"*80)
        # print(json.dumps(json_result, indent=2))
        self.maxDiff = None

        with open(filepath + '/test_sets/expected_cli_results_default.json', 'r') as f:
            expected_cli_results = json.load(f)

        assertDeepAlmostEqual(self,
            json_result,
            expected_cli_results
        )

        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "result_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "summary_obj": {\n        "unexpected_percent": 0.0007616146230007616, \n        "partial_unexpected_list": [\n          "*"\n        ], \n        "unexpected_percent_nonmissing": 0.0007616146230007616, \n        "unexpected_count": 1\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -o=COMPLETE'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": False, \n      "unexpected_list": [\n        "*"\n      ], \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "result_format": "COMPLETE", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "unexpected_index_list": [\n        456\n      ]\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -o=BOOLEAN_ONLY'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "kwargs": {\n        "column": "PClass", \n        "result_format": "BOOLEAN_ONLY", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -e'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "summary_obj": {\n        "unexpected_percent": 0.0007616146230007616, \n        "partial_unexpected_list": [\n          "*"\n        ], \n        "unexpected_percent_nonmissing": 0.0007616146230007616, \n        "unexpected_count": 1\n      }, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "kwargs": {\n        "column": "PClass", \n        "result_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -e'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str)["output"],
        #     "{\n  \"results\": [\n    {\n      \"summary_obj\": {\n        \"unexpected_percent\": 0.0007616146230007616, \n        \"partial_unexpected_list\": [\n          \"*\"\n        ], \n        \"unexpected_percent_nonmissing\": 0.0007616146230007616, \n        \"unexpected_count\": 1\n      }, \n      \"expectation_type\": \"expect_column_values_to_be_in_set\", \n      \"success\": false, \n      \"kwargs\": {\n        \"column\": \"PClass\", \n        \"result_format\": \"BASIC\", \n        \"value_set\": [\n          \"1st\", \n          \"2nd\", \n          \"3rd\"\n        ]\n      }\n    }\n  ]\n}\n"
        # )
        #print(filepath)

        command_str = 'python ' + filepath + '/../bin/great_expectations validate ' \
                      + filepath + '/test_sets/Titanic.csv '\
                      + filepath + '/test_sets/titanic_custom_expectations.json -f -m='\
                      + filepath + '/test_fixtures/custom_dataset.py -c=CustomPandasDataset'
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
        # print(json.dumps(json_result, indent=2))

        #Remove partial unexpected counts, because we can't guarantee that they'll be the same every time.
        del json_result["results"][0]["result"]['partial_unexpected_counts']

        with open(filepath + '/test_sets/expected_cli_results_custom.json', 'r') as f:
            expected_cli_results = json.load(f)

        self.assertEqual(
            json_result,
            expected_cli_results
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
        #             "result_format": "BASIC", 
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
