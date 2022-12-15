import pathlib

import pytest

from scripts.public_api_report import (
    DocsExampleParser,
    CodeParser,
    IncludeExcludeDefinition,
    FileContents,
    CodeReferenceFilter,
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
        
    def _example_private_method():
        pass


def example_module_level_function():
    pass
    
def _example_private_module_level_function():
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
def sample_docs_example_file_contents(
    sample_docs_example_python_file_string: str,
    sample_docs_example_python_file_string_filepath: pathlib.Path,
) -> FileContents:
    return FileContents(
        filepath=sample_docs_example_python_file_string_filepath,
        contents=sample_docs_example_python_file_string,
    )


@pytest.fixture
def sample_with_definitions_file_contents(
    sample_with_definitions_python_file_string: str,
    sample_with_definitions_python_file_string_filepath: pathlib.Path,
) -> FileContents:
    return FileContents(
        filepath=sample_with_definitions_python_file_string_filepath,
        contents=sample_with_definitions_python_file_string,
    )


@pytest.fixture
def docs_example_parser(
    sample_docs_example_file_contents: FileContents,
) -> DocsExampleParser:
    docs_example_parser = DocsExampleParser(
        file_contents={sample_docs_example_file_contents}
    )
    return docs_example_parser


class TestDocExampleParser:
    def test_instantiate(self, docs_example_parser: DocsExampleParser):
        assert isinstance(docs_example_parser, DocsExampleParser)

    def test_retrieve_all_usages_in_files(self, docs_example_parser: DocsExampleParser):

        usages = docs_example_parser.retrieve_all_usages_in_docs_example_files()
        assert usages == {
            "ExampleClass",
            "example_method",
            "example_method_with_args",
            "example_staticmethod",
            "example_classmethod",
            "example_module_level_function",
        }


@pytest.fixture
def code_parser(sample_with_definitions_file_contents: FileContents) -> CodeParser:
    code_parser = CodeParser(file_contents={sample_with_definitions_file_contents})
    return code_parser


class TestCodeParser:
    def test_instantiate(self, code_parser: CodeParser):
        assert isinstance(code_parser, CodeParser)

    def test_get_all_class_method_and_function_names(self, code_parser: CodeParser):
        names = code_parser.get_all_class_method_and_function_names()
        assert names == {
            "ExampleClass",
            "__init__",
            "_example_private_method",
            "_example_private_module_level_function",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_staticmethod",
        }

    def test_get_all_class_method_and_function_definitions(
        self, code_parser: CodeParser
    ):
        definitions = code_parser.get_all_class_method_and_function_definitions()

        assert len(definitions) == 9
        assert set([d.name for d in definitions]) == {
            "ExampleClass",
            "__init__",
            "_example_private_method",
            "_example_private_module_level_function",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_staticmethod",
        }
        assert set([d.filepath for d in definitions]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }


@pytest.fixture
def code_reference_filter(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser, code_parser=code_parser
    )


@pytest.fixture
def code_reference_filter_with_non_default_include_exclude(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
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


@pytest.fixture
def code_reference_filter_with_no_include_exclude(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        includes=[],
        excludes=[],
    )


@pytest.fixture
def code_reference_filter_with_exclude_by_file(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
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


@pytest.fixture
def code_reference_filter_with_exclude_by_file_and_name(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
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


@pytest.fixture
def code_reference_filter_with_include_by_file_and_name_already_included(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
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


@pytest.fixture
def code_reference_filter_with_include_by_file_and_name_already_excluded(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
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
        excludes=[
            IncludeExcludeDefinition(
                reason="test",
                filepath=pathlib.Path(
                    "great_expectations/sample_with_definitions_python_file_string.py"
                ),
            )
        ],
    )


class TestCodeReferenceFilter:
    def test_instantiate(self, code_reference_filter: CodeReferenceFilter):
        assert isinstance(code_reference_filter, CodeReferenceFilter)
        assert code_reference_filter.excludes
        assert code_reference_filter.includes

    def test_instantiate_with_non_default_include_exclude(
        self,
        code_reference_filter_with_non_default_include_exclude: CodeReferenceFilter,
    ):
        code_reference_filter = code_reference_filter_with_non_default_include_exclude
        assert isinstance(code_reference_filter, CodeReferenceFilter)
        assert code_reference_filter.excludes
        assert code_reference_filter.includes
        assert len(code_reference_filter.excludes) == 1
        assert len(code_reference_filter.includes) == 1

    def test_filter_definitions_no_include_exclude(
        self, code_reference_filter_with_no_include_exclude: CodeReferenceFilter
    ):
        observed = code_reference_filter_with_no_include_exclude.filter_definitions()
        assert len(observed) == 6
        assert set([d.name for d in observed]) == {
            "ExampleClass",
            # "__init__",  # Filtered private methods
            # "_example_private_method",  # Filtered private methods
            # "_example_private_module_level_function",  # Filtered private methods
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

    def test_filter_definitions_exclude_by_file(
        self, code_reference_filter_with_exclude_by_file: CodeReferenceFilter
    ):
        observed = code_reference_filter_with_exclude_by_file.filter_definitions()
        assert len(observed) == 0
        assert set([d.name for d in observed]) == set()
        assert set([d.filepath for d in observed]) == set()

    def test_filter_definitions_exclude_by_file_and_name(
        self, code_reference_filter_with_exclude_by_file_and_name: CodeReferenceFilter
    ):
        observed = (
            code_reference_filter_with_exclude_by_file_and_name.filter_definitions()
        )
        assert len(observed) == 4
        assert set([d.name for d in observed]) == {
            "ExampleClass",
            "example_classmethod",
            "example_method_with_args",
            "example_staticmethod",
        }
        assert set([d.filepath for d in observed]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    def test_filter_definitions_include_by_file_and_name_already_included(
        self,
        code_reference_filter_with_include_by_file_and_name_already_included: CodeReferenceFilter,
    ):
        """What does this test and why?

        That include directives that try to include already included definitions
        will not include multiple copies of the same definitions (when not
        accounting for different but equivalent ast definition object instances).
        """
        observed = (
            code_reference_filter_with_include_by_file_and_name_already_included.filter_definitions()
        )
        # There are two extra (8 vs 6) here due to the ast_definition classes
        #  pointing to different but equivalent objects.
        assert len(observed) == 8
        assert set([d.name for d in observed]) == {
            "ExampleClass",
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

    def test_filter_definitions_include_by_file_and_name_already_excluded(
        self,
        code_reference_filter_with_include_by_file_and_name_already_excluded: CodeReferenceFilter,
    ):
        """What does this test and why?

        Include overrides exclude.
        """
        observed = (
            code_reference_filter_with_include_by_file_and_name_already_excluded.filter_definitions()
        )
        # There are two extra (4 vs 2) here due to the ast_definition classes
        #  pointing to different but equivalent objects.
        assert len(observed) == 4
        assert set([d.name for d in observed]) == {
            "example_method",
            "example_module_level_function",
        }
        assert set([d.filepath for d in observed]) == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }


class TestPublicAPIChecker:
    def test_instantiate(self):
        raise NotImplementedError
