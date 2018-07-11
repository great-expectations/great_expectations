import json
import pytest

import great_expectations.cli
import great_expectations.version


def test_cli_command_error(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        great_expectations.cli.dispatch([])
    out, err = capsys.readouterr()

    assert pytest_wrapped_e.type == SystemExit
    assert out == ''
    assert ('error: the following arguments are required: command' in err) or ('error: too few arguments' in err)


def test_cli_validate_help(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        great_expectations.cli.dispatch(["validate"])

    out, err = capsys.readouterr()

    assert pytest_wrapped_e.type == SystemExit
    assert out == ''
    assert ('validate: error: the following arguments are required: dataset, expectations_config_file' in err) or \
           ('error: too few arguments' in err)
    assert '[--evaluation_parameters EVALUATION_PARAMETERS]' in err
    assert '[--result_format RESULT_FORMAT]' in err
    assert '[--catch_exceptions CATCH_EXCEPTIONS]' in err
    assert '[--only_return_failures ONLY_RETURN_FAILURES]' in err
    assert '[--custom_dataset_module CUSTOM_DATASET_MODULE]' in err
    assert '[--custom_dataset_class CUSTOM_DATASET_CLASS]' in err


def test_cli_version(capsys):
    great_expectations.cli.dispatch(["version"])
    out, err = capsys.readouterr()

    assert out == great_expectations.version.__version__ + '\n'
    assert err == ''


def test_validate_basic_operation(capsys):
    with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
        great_expectations.cli.dispatch(["validate",
                                         "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_expectations.json"])

    out, err = capsys.readouterr()
    json_result = json.loads(out)
    with open('./tests/test_sets/expected_cli_results_default.json', 'r') as f:
        expected_cli_results = json.load(f)

    assert json_result == expected_cli_results


def test_validate_custom_dataset(capsys):
    with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
        great_expectations.cli.dispatch(["validate",
                                         "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_custom_expectations.json",
                                         "-f", "True",
                                         "-m", "./tests/test_fixtures/custom_dataset.py",
                                         "-c", "CustomPandasDataset"])

    out, err = capsys.readouterr()
    json_result = json.loads(out)
    del json_result["results"][0]["result"]['partial_unexpected_counts']
    with open('./tests/test_sets/expected_cli_results_custom.json', 'r') as f:
        expected_cli_results = json.load(f)

    assert json_result == expected_cli_results


def test_cli_evaluation_parameters(capsys):
    with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
        great_expectations.cli.dispatch(["validate",
                                         "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_parameterized_expectations.json",
                                         "--evaluation_parameters",
                                         "./tests/test_sets/titanic_evaluation_parameters.json",
                                         "-f", "True"])

    out, err = capsys.readouterr()
    with open('./tests/test_sets/titanic_evaluation_parameters.json', 'r') as f:
        expected_evaluation_parameters = json.load(f)

    json_result = json.loads(out)
    assert json_result['evaluation_parameters'] == expected_evaluation_parameters
