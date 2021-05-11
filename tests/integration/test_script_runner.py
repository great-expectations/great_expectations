import os
import shutil
import subprocess
import sys

import pytest

from assets.scripts.build_gallery import execute_shell_command
from great_expectations.data_context.util import file_relative_path

integration_test_matrix = [
    {
        "name": "pandas_two_batch_requests_two_validators",
        "base_dir": file_relative_path(__file__, "../../"),
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/two_batch_requests_two_validators.py",
    },
    {
        "name": "pandas_one_multi_batch_request_one_validator",
        "base_dir": file_relative_path(__file__, "../../"),
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
        "user_flow_script": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/one_multi_batch_request_one_validator.py",
    },
]


def idfn(test_configuration):
    return test_configuration.get("name")


@pytest.mark.docs
@pytest.mark.integration
@pytest.mark.parametrize("test_configuration", integration_test_matrix, ids=idfn)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python3.7")
def test_docs(test_configuration, tmp_path):
    workdir = os.getcwd()
    try:
        os.chdir(tmp_path)
        base_dir = test_configuration.get("base_dir", ".")
        # Ensure GE is installed in our environment
        ge_requirement = test_configuration.get("ge_requirement", "great_expectations")
        execute_shell_command(f"pip install {ge_requirement}")

        #
        # Build test state
        #

        # DataContext
        context_source_dir = os.path.join(
            base_dir, test_configuration.get("data_context_dir")
        )
        test_context_dir = os.path.join(tmp_path, "great_expectations")
        shutil.copytree(
            context_source_dir,
            test_context_dir,
        )

        # Test Data
        source_data_dir = os.path.join(base_dir, test_configuration.get("data_dir"))
        test_data_dir = os.path.join(tmp_path, "test_data")
        shutil.copytree(
            source_data_dir,
            test_data_dir,
        )

        # UAT Script
        script_source = os.path.join(
            test_configuration.get("base_dir"),
            test_configuration.get("user_flow_script"),
        )
        script_path = os.path.join(tmp_path, "test_script.py")
        shutil.copyfile(script_source, script_path)
        # Check initial state

        # Execute test
        res = subprocess.run(["python", script_path], capture_output=True)
        # Check final state
        expected_stderrs = test_configuration.get("expected_stderrs")
        expected_stdouts = test_configuration.get("expected_stdouts")
        expected_failure = test_configuration.get("expected_failure")
        outs = res.stdout.decode("utf-8")
        errs = res.stderr.decode("utf-8")
        print(outs)
        print(errs)

        if expected_stderrs:
            assert expected_stderrs == errs

        if expected_stdouts:
            assert expected_stdouts == outs

        if expected_failure:
            assert res.returncode != 0
        else:
            assert res.returncode == 0
    except:
        raise
    finally:
        os.chdir(workdir)
