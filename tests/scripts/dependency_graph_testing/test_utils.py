import pytest

from scripts.dependency_graph_testing.utils import (
    get_changed_source_files,
    get_changed_test_files,
)


@pytest.fixture
def diffed_files():
    # Used to mock `git diff HEAD <BRANCH> --name-only`
    diffed_files = [
        "README.md",
        ".dockerignore",
        "great_expectations/util.py",
        "tests/integration/test_script_runner.py",
        "tests/__init.py",
        "tests/test_fixtures/custom_pandas_dataset.py",
        "tests/datasource/conftest.py",
        "tests/datasource/test_datasource.py",
        "great_expectations/rule_based_profiler/README.md",
    ]
    return diffed_files


def test_get_changed_source_files(diffed_files):
    res = get_changed_source_files(files=diffed_files, source_path="great_expectations")
    assert res == ["great_expectations/util.py"]


def test_get_changed_test_files(diffed_files):
    res = get_changed_test_files(files=diffed_files, tests_path="tests")
    assert sorted(res) == [
        "tests/datasource/test_datasource.py",
        "tests/integration/test_script_runner.py",
    ]
