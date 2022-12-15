import pathlib
from typing import Set

import pytest

from scripts.public_api_report import (
    DocExampleParser,
    GXCodeParser,
    IncludeExcludeDefinition,
    Definition,
    _repo_root,
)


@pytest.fixture
def sample_docs_example_python_file_string() -> str:
    return """from scripts.sample_with_definitions import ExampleClass, example_module_level_function

ec = ExampleClass()

ec.example_method()

a = ec.example_method_with_args(some_arg=1, other_arg=2)

b = ec.example_staticmethod()

c = ec.example_classmethod()

example_module_level_function()

d = example_module_level_function()

assert d
"""


@pytest.fixture
def sample_with_definitions_python_file_string() -> str:
    return """
class ExampleClass:

    def __init__(self):
        pass

    def example_method(self):
        pass

    def example_method_with_args(self, some_arg, other_arg):
        pass

    @staticmethod
    def example_staticmethod():
        pass

    @classmethod
    def example_classmethod(cls):
        pass


def example_module_level_function():
    pass
"""


@pytest.fixture
def repo_root() -> pathlib.Path:
    return pathlib.Path("/repo_root/")


@pytest.fixture
def sample_docs_example_python_file_string_filepath(
    repo_root: pathlib.Path,
) -> pathlib.Path:
    return (
        repo_root
        / pathlib.Path(
            "tests/integration/docusaurus/sample_docs_example_python_file_string.py"
        )
    ).relative_to(repo_root)


@pytest.fixture
def sample_with_definitions_python_file_string_filepath(
    repo_root: pathlib.Path,
) -> pathlib.Path:
    return (
        repo_root
        / pathlib.Path(
            "great_expectations/sample_with_definitions_python_file_string.py"
        )
    ).relative_to(repo_root)


@pytest.fixture
def filesystem_with_samples(
    fs,
    sample_docs_example_python_file_string: str,
    sample_docs_example_python_file_string_filepath: pathlib.Path,
    sample_with_definitions_python_file_string: str,
    sample_with_definitions_python_file_string_filepath: pathlib.Path,
) -> None:
    fs.create_file(
        sample_docs_example_python_file_string_filepath,
        contents=sample_docs_example_python_file_string,
    )
    fs.create_file(
        sample_with_definitions_python_file_string_filepath,
        contents=sample_with_definitions_python_file_string,
    )


def test_fixtures_are_accessible(
    filesystem_with_samples,
    sample_docs_example_python_file_string: str,
    sample_docs_example_python_file_string_filepath: pathlib.Path,
    sample_with_definitions_python_file_string: str,
    sample_with_definitions_python_file_string_filepath: pathlib.Path,
):

    assert sample_docs_example_python_file_string_filepath == pathlib.Path(
        "tests/integration/docusaurus/sample_docs_example_python_file_string.py"
    )
    assert sample_with_definitions_python_file_string_filepath == pathlib.Path(
        "great_expectations/sample_with_definitions_python_file_string.py"
    )

    with open(sample_docs_example_python_file_string_filepath) as f:
        file_contents = f.read()
        assert file_contents == sample_docs_example_python_file_string
        assert len(file_contents) > 200

    with open(sample_with_definitions_python_file_string_filepath) as f:
        file_contents = f.read()
        assert file_contents == sample_with_definitions_python_file_string
        assert len(file_contents) > 200


class TestDocExampleParser:
    def test_instantiate(
        self,
        repo_root: pathlib.Path,
        sample_docs_example_python_file_string_filepath: pathlib.Path,
    ):
        doc_example_parser = DocExampleParser(
            repo_root=repo_root, paths={sample_docs_example_python_file_string_filepath}
        )
        assert isinstance(doc_example_parser, DocExampleParser)

    def test_retrieve_all_usages_in_files(
        self,
        filesystem_with_samples,
        repo_root: pathlib.Path,
        sample_docs_example_python_file_string_filepath: pathlib.Path,
    ):
        doc_example_parser = DocExampleParser(
            repo_root=repo_root, paths={sample_docs_example_python_file_string_filepath}
        )
        usages = doc_example_parser.retrieve_all_usages_in_files()
        assert usages == {
            "ExampleClass",
            "example_method",
            "example_method_with_args",
            "example_staticmethod",
            "example_classmethod",
            "example_module_level_function",
        }


class TestGXCodeParser:
    def test_instantiate_with_default_include_exclude(
        self,
        repo_root: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={sample_with_definitions_python_file_string_filepath},
        )
        assert isinstance(gx_code_parser, GXCodeParser)
        assert gx_code_parser.excludes
        assert gx_code_parser.includes

    def test_instantiate_with_non_default_include_exclude(
        self,
        repo_root: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={sample_with_definitions_python_file_string_filepath},
            includes=[
                IncludeExcludeDefinition(
                    reason="test", name="test_name", filepath=pathlib.Path("test_path")
                )
            ],
            excludes=[
                IncludeExcludeDefinition(
                    reason="test", name="test_name", filepath=pathlib.Path("test_path")
                )
            ],
        )
        assert isinstance(gx_code_parser, GXCodeParser)
        assert gx_code_parser.excludes
        assert gx_code_parser.includes
        assert len(gx_code_parser.excludes) == 1
        assert len(gx_code_parser.includes) == 1

    def test_get_filtered_and_included_class_method_and_function_definitions_from_files_no_include_exclude(
        self,
        filesystem_with_samples,
        repo_root: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={
                sample_with_definitions_python_file_string_filepath,
            },
            includes=[],
            excludes=[],
        )
        observed = (
            gx_code_parser.get_filtered_and_included_class_method_and_function_definitions_from_files()
        )
        assert len(observed) == 7
        assert set([d.name for d in observed]) == {
            "ExampleClass",
            "__init__",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_staticmethod",
        }
        assert set([d.filepath for d in observed]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    def test_get_filtered_and_included_class_method_and_function_definitions_from_files_exclude_by_file(
        self,
        filesystem_with_samples,
        repo_root: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={
                sample_with_definitions_python_file_string_filepath,
            },
            includes=[],
            excludes=[
                IncludeExcludeDefinition(
                    reason="test",
                    filepath=pathlib.Path(
                        "great_expectations/sample_with_definitions_python_file_string.py"
                    ),
                )
            ],
        )
        observed = (
            gx_code_parser.get_filtered_and_included_class_method_and_function_definitions_from_files()
        )
        assert len(observed) == 0
        assert set([d.name for d in observed]) == set()
        assert set([d.filepath for d in observed]) == set()

    def test_get_filtered_and_included_class_method_and_function_definitions_from_files_exclude_by_file_and_name(
        self,
        filesystem_with_samples,
        repo_root: pathlib.Path,
        sample_docs_example_python_file_string_filepath: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={
                sample_with_definitions_python_file_string_filepath,
            },
            includes=[],
            excludes=[
                IncludeExcludeDefinition(
                    reason="test",
                    name="example_method",
                    filepath=pathlib.Path(
                        "great_expectations/sample_with_definitions_python_file_string.py"
                    ),
                ),
                IncludeExcludeDefinition(
                    reason="test",
                    name="example_module_level_function",
                    filepath=pathlib.Path(
                        "great_expectations/sample_with_definitions_python_file_string.py"
                    ),
                ),
            ],
        )
        observed = (
            gx_code_parser.get_filtered_and_included_class_method_and_function_definitions_from_files()
        )
        assert len(observed) == 5
        assert set([d.name for d in observed]) == {
            "ExampleClass",
            "__init__",
            "example_classmethod",
            "example_method_with_args",
            "example_staticmethod",
        }
        assert set([d.filepath for d in observed]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    def test_get_filtered_and_included_class_method_and_function_definitions_from_files_include_by_file_and_name_already_included(
        self,
        filesystem_with_samples,
        repo_root: pathlib.Path,
        sample_docs_example_python_file_string_filepath: pathlib.Path,
        sample_with_definitions_python_file_string_filepath: pathlib.Path,
    ):
        """What does this test and why?

        This method tests that include directives that try to include already included
        definitions will not include multiple copies of the same definitions when
        not accounting for different ast definitions.
        """
        gx_code_parser = GXCodeParser(
            repo_root=repo_root,
            paths={
                sample_with_definitions_python_file_string_filepath,
            },
            includes=[
                IncludeExcludeDefinition(
                    reason="test",
                    name="example_method",
                    filepath=pathlib.Path(
                        "great_expectations/sample_with_definitions_python_file_string.py"
                    ),
                ),
                IncludeExcludeDefinition(
                    reason="test",
                    name="example_module_level_function",
                    filepath=pathlib.Path(
                        "great_expectations/sample_with_definitions_python_file_string.py"
                    ),
                ),
            ],
            excludes=[],
        )
        observed = (
            gx_code_parser.get_filtered_and_included_class_method_and_function_definitions_from_files()
        )
        # There are two extra (9 vs 7) here due to the ast_definition classes
        #  pointing to different but equivalent objects.
        assert len(observed) == 9
        assert set([d.name for d in observed]) == {
            "ExampleClass",
            "__init__",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_staticmethod",
        }
        assert set([d.filepath for d in observed]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }
