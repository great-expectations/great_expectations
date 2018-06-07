import unittest
import json
import inspect
import os
import subprocess

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

    def test_cli_help_message(self):
        filepath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        # !!! These tests require you to uninstall and re-install great_expectations.
        # !!! Otherwise, they test the CLI, but not great_expectations itself.

        command_result = get_system_command_result('python '+filepath+'/../bin/great_expectations ')
        assert "usage: great_expectations [-h] {initialize,validate} ...\ngreat_expectations: error: invalid choice: '' (choose from 'initialize', 'validate')" in command_result['errors']

        command_str = 'python '+filepath+'/../bin/great_expectations validate '+filepath+'/test_sets/Titanic.csv '+filepath+'/test_sets/titanic_expectations.json'
        # print(command_str)

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

        with open(filepath + '/test_sets/expected_cli_results_default.json', 'r') as f:
            expected_cli_results = json.load(f)

        assertDeepAlmostEqual(self,
            json_result,
            expected_cli_results
        )

    def test_cli_custom_dataset(self):
        filepath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

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

        #Remove partial unexpected counts, because we can't guarantee that they'll be the same every time.
        del json_result["results"][0]["result"]['partial_unexpected_counts']

        with open(filepath + '/test_sets/expected_cli_results_custom.json', 'r') as f:
            expected_cli_results = json.load(f)

        self.assertEqual(
            json_result,
            expected_cli_results
        )

    def test_cli_evaluation_parameters(self):
        filepath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

        command_str = 'python ' + filepath + '/../bin/great_expectations validate ' \
                      + '--evaluation_parameters ' + filepath + '/test_sets/titanic_evaluation_parameters.json ' \
                      + '--only_return_failures ' \
                      + filepath + '/test_sets/Titanic.csv ' \
                      + filepath + '/test_sets/titanic_parameterized_expectations.json'

        expected_evaluation_parameters = json.load(open('./tests/test_sets/titanic_evaluation_parameters.json'))

        result = get_system_command_result(command_str)
        json_result = json.loads(result["output"])

        self.assertEqual(json_result['evaluation_parameters'], expected_evaluation_parameters)

if __name__ == "__main__":
    unittest.main()
