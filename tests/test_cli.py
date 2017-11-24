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
        try:
          result = get_system_command_result(command_str)["output"]
          json_result = json.loads(result)
        except ValueError as ve:
          print ("=== Result ==================================================")
          print (result)
          print ("=== Error ===================================================")
          print(ve)
          json_result = {}

        assertDeepAlmostEqual(self,
            json_result,
            {
              "results": [
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Name", 
                    "output_format": "BASIC"
                  }
                }, 
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "PClass", 
                    "output_format": "BASIC"
                  }
                }, 
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Age", 
                    "output_format": "BASIC"
                  }
                }, 
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Sex", 
                    "output_format": "BASIC"
                  }
                }, 
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Survived", 
                    "output_format": "BASIC"
                  }
                }, 
                {
                  "success": True, 
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_to_exist", 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "SexCode", 
                    "output_format": "BASIC"
                  }
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
                    "output_format": "BASIC", 
                    "min_value": 20
                  }
                }, 
                {
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_values_to_be_between", 
                  "success": True, 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "Age", 
                    "max_value": 80, 
                    "output_format": "BASIC", 
                    "min_value": 0
                  }, 
                  "summary_obj": {
                    "exception_percent": 0.0, 
                    "partial_exception_list": [], 
                    "exception_percent_nonmissing": 0.0, 
                    "exception_count": 0
                  }
                }, 
                {
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_values_to_match_regex", 
                  "success": True, 
                  "raised_exception": False, 
                  "kwargs": {
                    "regex": "[A-Z][a-z]+(?: \\([A-Z][a-z]+\\))?, ", 
                    "column": "Name", 
                    "output_format": "BASIC", 
                    "mostly": 0.95
                  }, 
                  "summary_obj": {
                    "exception_percent": 0.002284843869002285, 
                    "partial_exception_list": [
                      "Downton (?Douton), Mr William James", 
                      "Jacobsohn Mr Samuel", 
                      "Seman Master Betros"
                    ], 
                    "exception_percent_nonmissing": 0.002284843869002285, 
                    "exception_count": 3
                  }
                }, 
                {
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_values_to_be_in_set", 
                  "success": False, 
                  "raised_exception": False, 
                  "kwargs": {
                    "column": "PClass", 
                    "output_format": "BASIC", 
                    "values_set": [
                      "1st", 
                      "2nd", 
                      "3rd"
                    ]
                  }, 
                  "summary_obj": {
                    "exception_percent": 0.0007616146230007616, 
                    "partial_exception_list": [
                      "*"
                    ], 
                    "exception_percent_nonmissing": 0.0007616146230007616, 
                    "exception_count": 1
                  }
                }
              ]
            }
        )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "summary_obj": {\n        "exception_percent": 0.0007616146230007616, \n        "partial_exception_list": [\n          "*"\n        ], \n        "exception_percent_nonmissing": 0.0007616146230007616, \n        "exception_count": 1\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -o=COMPLETE'
        # self.assertEqual(
        #     get_system_command_result(command_str),
        #     {'output': '{\n  "results": [\n    {\n      "exception_traceback": null, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "exception_list": [\n        "*"\n      ], \n      "raised_exception": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "COMPLETE", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }, \n      "exception_index_list": [\n        456\n      ]\n    }\n  ]\n}\n', 'errors': ''}
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
        #     {'output': '{\n  "results": [\n    {\n      "summary_obj": {\n        "exception_percent": 0.0007616146230007616, \n        "partial_exception_list": [\n          "*"\n        ], \n        "exception_percent_nonmissing": 0.0007616146230007616, \n        "exception_count": 1\n      }, \n      "expectation_type": "expect_column_values_to_be_in_set", \n      "success": false, \n      "kwargs": {\n        "column": "PClass", \n        "output_format": "BASIC", \n        "value_set": [\n          "1st", \n          "2nd", \n          "3rd"\n        ]\n      }\n    }\n  ]\n}\n', 'errors': ''}
        # )
        #
        # command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/examples/Titanic.csv '+filepath+'/examples/titanic_expectations.json -f -e'
        # # print get_system_command_result(command_str)
        # self.assertEqual(
        #     get_system_command_result(command_str)["output"],
        #     "{\n  \"results\": [\n    {\n      \"summary_obj\": {\n        \"exception_percent\": 0.0007616146230007616, \n        \"partial_exception_list\": [\n          \"*\"\n        ], \n        \"exception_percent_nonmissing\": 0.0007616146230007616, \n        \"exception_count\": 1\n      }, \n      \"expectation_type\": \"expect_column_values_to_be_in_set\", \n      \"success\": false, \n      \"kwargs\": {\n        \"column\": \"PClass\", \n        \"output_format\": \"BASIC\", \n        \"value_set\": [\n          \"1st\", \n          \"2nd\", \n          \"3rd\"\n        ]\n      }\n    }\n  ]\n}\n"
        # )
        #print(filepath)

        command_str = 'python ' + filepath + '/../bin/great_expectations validate ' \
                      + filepath + '/test_sets/Titanic.csv '\
                      + filepath + '/test_sets/titanic_custom_expectations.json -f -m='\
                      + filepath + '/test_fixtures/custom_dataset.py -c=CustomPandasDataSet'
        try:
          result = get_system_command_result(command_str)["output"]
          json_result = json.loads(result)
        except ValueError as ve:
          print(ve)
          json_result = {}

        self.assertEqual(
            json_result,
            {
              "results": [
                {
                  "exception_traceback": None, 
                  "expectation_type": "expect_column_values_to_have_odd_lengths", 
                  "success": False, 
                  "raised_exception": False,
                  "kwargs": {
                    "column": "Name", 
                    "output_format": "BASIC"
                  }, 
                  "summary_obj": {
                    "exception_percent": 0.5026656511805027, 
                    "partial_exception_list": [
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
                    "exception_percent_nonmissing": 0.5026656511805027, 
                    "exception_count": 660
                  }
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
        #             "exception_percent": 0.0007616146230007616, 
        #             "partial_exception_list": [
        #               "*"
        #             ], 
        #             "exception_percent_nonmissing": 0.0007616146230007616, 
        #             "exception_count": 1
        #           }
        #         }
        #       ]
        #     }
        # )

if __name__ == "__main__":
    unittest.main()
