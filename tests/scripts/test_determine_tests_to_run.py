import pytest

from scripts.determine_tests_to_run import (
    Import,
    get_changed_source_files,
    get_changed_test_files,
    parse_imports,
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


@pytest.fixture
def parsed_imports():
    # Used to mock the result of converting AST import nodes into an intermediary format (namedtuple Import)
    filename = "great_expectations/my_fake_source_file.py"
    parsed = [
        Import(
            source=filename, module=["collections"], name=["OrderedDict"], alias=None
        ),
        Import(source=filename, module=[], name=["requests"], alias=None),
        Import(
            source=filename, module=["dateutil", "parser"], name=["parse"], alias=None
        ),
        Import(source=filename, module=["ruamel", "yaml"], name=["YAML"], alias=None),
        Import(
            source=filename, module=["ruamel", "yaml"], name=["YAMLError"], alias=None
        ),
        Import(
            source=filename,
            module=["ruamel", "yaml", "comments"],
            name=["CommentedMap"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["ruamel", "yaml", "constructor"],
            name=["DuplicateKeyError"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "checkpoint"],
            name=["Checkpoint"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "checkpoint"],
            name=["LegacyCheckpoint"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "checkpoint"],
            name=["SimpleCheckpoint"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "checkpoint", "types", "checkpoint_result"],
            name=["CheckpointResult"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "core", "expectation_suite"],
            name=["ExpectationSuite"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "core", "expectation_validation_result"],
            name=["get_metric_kwargs_id"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "core", "id_dict"],
            name=["BatchKwargs"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "core", "metric"],
            name=["ValidationMetricIdentifier"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations", "core", "run_identifier"],
            name=["RunIdentifier"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations"],
            name=["DataContext"],
            alias=None,
        ),
        Import(
            source=filename,
            module=["great_expectations"],
            name=["exceptions"],
            alias="ge_exceptions",
        ),
    ]
    return parsed


def test_get_changed_source_files(diffed_files):
    res = get_changed_source_files(files=diffed_files, source_path="great_expectations")
    assert res == ["great_expectations/util.py"]


def test_get_changed_test_files(diffed_files):
    res = get_changed_test_files(files=diffed_files, tests_path="tests")
    assert sorted(res) == [
        "tests/datasource/test_datasource.py",
        "tests/integration/test_script_runner.py",
    ]


def test_parse_imports(tmpdir, parsed_imports):
    temp_file = tmpdir.mkdir("tmp").join("ge_imports.py")
    import_statements = """
from collections import OrderedDict

import requests
from dateutil.parser import parse
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.constructor import DuplicateKeyError

from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import get_metric_kwargs_id
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
    """
    temp_file.write(import_statements)

    expected_imports = parsed_imports
    actual_imports = parse_imports(temp_file)

    assert len(actual_imports) == len(parsed_imports)
    for actual, expected in zip(actual_imports, expected_imports):
        assert (
            actual.module == expected.module
            and actual.name == expected.name
            and actual.alias == expected.alias
        )
