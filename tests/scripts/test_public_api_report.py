import ast
import pathlib
from typing import List, Union

import pytest

from docs.sphinx_api_docs_source.include_exclude_definition import (
    IncludeExcludeDefinition,
)
from docs.sphinx_api_docs_source.public_api_report import (
    CodeParser,
    CodeReferenceFilter,
    Definition,
    DocsExampleParser,
    FileContents,
    PublicAPIChecker,
    PublicAPIReport,
    _get_import_names,
    get_shortest_dotted_path,
    parse_docs_contents_for_class_names,
)


@pytest.fixture
def sample_docs_example_python_file_string() -> str:
    return """from scripts.sample_with_definitions import ExampleClass, example_module_level_function

ec = ExampleClass()

ec.example_method()

a = ec.example_method_with_args(some_arg=1, other_arg=2)

b = ec.example_staticmethod()

c = ec.example_classmethod()

ec.example_public_staticmethod()

ec.example_public_classmethod()

d = ec.example_multiple_decorator_public_method()

ec._example_private_method()

ec.example_public_api_method()

e = example_module_level_function()

_example_private_module_level_function()

example_public_api_module_level_function()

epub = ExamplePublicAPIClass()

assert d

some_yaml_contents = \"\"\"
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
\"\"\"

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

    @staticmethod
    @public_api
    def example_public_staticmethod():
        pass

    @classmethod
    @public_api
    def example_public_classmethod(cls):
        pass

    @some_other_decorator
    @public_api
    @another_decorator
    def example_multiple_decorator_public_method(self):
        pass

    def _example_private_method(self):
        pass

    @public_api
    def example_public_api_method(self):
        pass

    def example_no_usages_in_sample_docs_example_python_file_string():
        pass


def example_module_level_function():
    pass

def _example_private_module_level_function():
    pass

@public_api
def example_public_api_module_level_function():
    pass

@public_api
class ExamplePublicAPIClass:
    pass
"""


@pytest.fixture
def sample_markdown_doc_with_yaml() -> str:
    return """# Title

Content.

More content.

Some yaml:

yaml_contents = \"\"\"
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
\"\"\"

End of content.
"""


@pytest.fixture
def repo_root() -> pathlib.Path:
    return pathlib.Path("/some/absolute/path/repo_root/")


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
def sample_markdown_doc_with_yaml_file_contents(
    sample_markdown_doc_with_yaml: str,
) -> FileContents:
    return FileContents(
        filepath=pathlib.Path("some/random/filepath/markdown.md"),
        contents=sample_markdown_doc_with_yaml,
    )


@pytest.fixture
def docs_example_parser(
    sample_docs_example_file_contents: FileContents,
) -> DocsExampleParser:
    docs_example_parser = DocsExampleParser(
        file_contents={sample_docs_example_file_contents}
    )
    return docs_example_parser


@pytest.fixture
def empty_docs_example_parser(
    sample_docs_example_file_contents: FileContents,
) -> DocsExampleParser:
    docs_example_parser = DocsExampleParser(file_contents=set())
    return docs_example_parser


class TestDocExampleParser:
    @pytest.mark.unit
    def test_instantiate(self, docs_example_parser: DocsExampleParser):
        assert isinstance(docs_example_parser, DocsExampleParser)

    @pytest.mark.unit
    def test_retrieve_all_usages_in_files(self, docs_example_parser: DocsExampleParser):
        usages = docs_example_parser.get_names_from_usage_in_docs_examples()
        assert usages == {
            "ExampleClass",
            "ExamplePublicAPIClass",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_multiple_decorator_public_method",
            "example_public_api_method",
            "example_public_api_module_level_function",
            "example_public_classmethod",
            "example_public_staticmethod",
            "example_staticmethod",
            "Datasource",
            "SqlAlchemyExecutionEngine",
        }


@pytest.fixture
def code_parser(sample_with_definitions_file_contents: FileContents) -> CodeParser:
    code_parser = CodeParser(file_contents={sample_with_definitions_file_contents})
    return code_parser


class TestCodeParser:
    @pytest.mark.unit
    def test_instantiate(self, code_parser: CodeParser):
        assert isinstance(code_parser, CodeParser)

    @pytest.mark.unit
    def test_get_all_class_method_and_function_names(self, code_parser: CodeParser):
        names = code_parser.get_all_class_method_and_function_names()
        assert names == {
            "ExampleClass",
            "ExamplePublicAPIClass",
            "__init__",
            "_example_private_method",
            "_example_private_module_level_function",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_multiple_decorator_public_method",
            "example_no_usages_in_sample_docs_example_python_file_string",
            "example_public_api_method",
            "example_public_api_module_level_function",
            "example_public_classmethod",
            "example_public_staticmethod",
            "example_staticmethod",
        }

    @pytest.mark.unit
    def test_get_all_class_method_and_function_definitions(
        self, code_parser: CodeParser
    ):
        definitions = code_parser.get_all_class_method_and_function_definitions()

        assert len(definitions) == 16
        assert {d.name for d in definitions} == {
            "ExampleClass",
            "ExamplePublicAPIClass",
            "__init__",
            "_example_private_method",
            "_example_private_module_level_function",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_multiple_decorator_public_method",
            "example_no_usages_in_sample_docs_example_python_file_string",
            "example_public_api_method",
            "example_public_api_module_level_function",
            "example_public_classmethod",
            "example_public_staticmethod",
            "example_staticmethod",
        }
        assert {d.filepath for d in definitions} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }


def test_parse_docs_contents_for_class_names(
    sample_markdown_doc_with_yaml_file_contents: FileContents,
):
    assert parse_docs_contents_for_class_names(
        file_contents={sample_markdown_doc_with_yaml_file_contents}
    ) == {"Datasource", "SqlAlchemyExecutionEngine"}


def test_get_shortest_dotted_path(monkeypatch):
    """Test path traversal using an example file.

    The example is just any class that is imported in a __init__.py file in a
    parent folder. This test can be modified to use any such import e.g. if the
    example file changes, or a fixture created so the example isn't coming
    from the repo.
    """
    repo_root = pathlib.Path(__file__).parent.parent.parent
    monkeypatch.chdir(repo_root)
    filepath = pathlib.Path("great_expectations/core/expectation_suite.py")
    definition = Definition(
        name="ExpectationSuite", filepath=filepath, ast_definition=ast.ClassDef()
    )

    # This is the actual path
    assert (
        get_shortest_dotted_path(definition=definition, repo_root_path=repo_root)
        != "great_expectations.core.expectation_suite.ExpectationSuite"
    )
    # This is the shortest path
    assert (
        get_shortest_dotted_path(definition=definition, repo_root_path=repo_root)
        == "great_expectations.core.ExpectationSuite"
    )


@pytest.fixture
def various_imports() -> str:
    return """import some_module
import SomeClass
from some_module import ClassFromSomeModule
from some_other_module import ClassFromSomeOtherModule
import AliasClass as ac
import alias_module as am
from alias_module import AliasClassFromAliasModule as acfam
from a.b.c import some_method as sm
"""


@pytest.mark.unit
def test__get_import_names(various_imports: str):
    """Make sure the actual class and module names are returned."""
    tree = ast.parse(various_imports)
    import_names = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            import_names.extend(_get_import_names(node))

    assert import_names == [
        "some_module",
        "SomeClass",
        "ClassFromSomeModule",
        "ClassFromSomeOtherModule",
        "AliasClass",
        "alias_module",
        "AliasClassFromAliasModule",
        "some_method",
    ]


@pytest.fixture
def public_api_checker(
    docs_example_parser: DocsExampleParser, code_parser: CodeParser
) -> PublicAPIChecker:
    return PublicAPIChecker(code_parser=code_parser)


class TestPublicAPIChecker:
    @pytest.mark.unit
    def test_instantiate(self, public_api_checker: PublicAPIChecker):
        assert isinstance(public_api_checker, PublicAPIChecker)

    @pytest.mark.integration
    def test_get_all_public_api_definitions(self, public_api_checker: PublicAPIChecker):
        observed = public_api_checker.get_all_public_api_definitions()
        assert len(observed) == 6
        assert {d.name for d in observed} == {
            "ExamplePublicAPIClass",
            "example_multiple_decorator_public_method",
            "example_public_api_method",
            "example_public_api_module_level_function",
            "example_public_classmethod",
            "example_public_staticmethod",
        }
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    def _class_and_function_definitions(
        self, tree: ast.AST
    ) -> List[Union[ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef]]:
        """Helper function to find class and function definitions from ast tree for tests."""
        definitions = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ClassDef)  # noqa: PLR1701
                or isinstance(node, ast.FunctionDef)
                or isinstance(node, ast.AsyncFunctionDef)
            ):
                definitions.append(node)

        return definitions

    @pytest.mark.integration
    def test_is_definition_marked_public_api_yes(
        self, public_api_checker: PublicAPIChecker
    ):
        file_string = """
@public_api
def example_public_api_module_level_function():
    pass

@public_api
class ExamplePublicAPIClass:
    @public_api
    def example_public_api_method():
        pass

    @staticmethod
    @public_api
    def example_public_api_staticmethod():
        pass

    @classmethod
    @public_api
    def example_public_api_classmethod(cls):
        pass

    @some_other_decorator
    @public_api
    @another_decorator
    def example_multiple_decorator_public_api_method(self):
        pass

"""
        ast_definitions = self._class_and_function_definitions(
            tree=ast.parse(file_string)
        )
        definitions = [
            Definition(
                name="test_name",
                filepath=pathlib.Path("test_path"),
                ast_definition=ast_definition,
            )
            for ast_definition in ast_definitions
        ]
        assert all(
            public_api_checker.is_definition_marked_public_api(definition)
            for definition in definitions
        )

    @pytest.mark.integration
    def test_is_definition_marked_public_api_no(
        self, public_api_checker: PublicAPIChecker
    ):
        file_string = """
def example_module_level_function():
    pass

class ExampleClass:
    def example_method():
        pass

    @staticmethod
    def example_public_staticmethod():
        pass

    @classmethod
    def example_public_classmethod(cls):
        pass

    @some_other_decorator
    @another_decorator
    def example_multiple_decorator_public_method(self):
        pass

"""
        ast_definitions = self._class_and_function_definitions(
            tree=ast.parse(file_string)
        )
        definitions = [
            Definition(
                name="test_name",
                filepath=pathlib.Path("test_path"),
                ast_definition=ast_definition,
            )
            for ast_definition in ast_definitions
        ]
        assert not all(
            public_api_checker.is_definition_marked_public_api(definition)
            for definition in definitions
        )


@pytest.fixture
def code_reference_filter(
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
    )


@pytest.fixture
def code_reference_filter_with_non_default_include_exclude(
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
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
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
        includes=[],
        excludes=[],
    )


@pytest.fixture
def code_reference_filter_with_exclude_by_file(
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
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
def code_reference_filter_with_references_from_docs_content(
    repo_root: pathlib.Path,
    empty_docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=empty_docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
        references_from_docs_content={"ExampleClass", "ExamplePublicAPIClass"},
    )


@pytest.fixture
def code_reference_filter_with_exclude_by_file_and_name(
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
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
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
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
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
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


@pytest.fixture
def code_reference_filter_with_include_by_file_and_name_not_used_in_docs_example_exclude_file(
    repo_root: pathlib.Path,
    docs_example_parser: DocsExampleParser,
    code_parser: CodeParser,
    public_api_checker: PublicAPIChecker,
) -> CodeReferenceFilter:
    return CodeReferenceFilter(
        repo_root=repo_root,
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
        includes=[
            IncludeExcludeDefinition(
                reason="test",
                name="example_no_usages_in_sample_docs_example_python_file_string",
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
    @pytest.mark.unit
    def test_instantiate(self, code_reference_filter: CodeReferenceFilter):
        assert isinstance(code_reference_filter, CodeReferenceFilter)
        assert code_reference_filter.excludes
        assert code_reference_filter.includes

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_filter_definitions_no_include_exclude(
        self, code_reference_filter_with_no_include_exclude: CodeReferenceFilter
    ):
        observed = code_reference_filter_with_no_include_exclude.filter_definitions()
        assert len(observed) == 6
        assert {d.name for d in observed} == {
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
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    @pytest.mark.integration
    def test_filter_definitions_with_references_from_docs_content(
        self,
        code_reference_filter_with_references_from_docs_content: CodeReferenceFilter,
    ):
        observed = (
            code_reference_filter_with_references_from_docs_content.filter_definitions()
        )
        assert len(observed) == 1
        assert {d.name for d in observed} == {"ExampleClass"}
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    @pytest.mark.integration
    def test_filter_definitions_exclude_by_file(
        self, code_reference_filter_with_exclude_by_file: CodeReferenceFilter
    ):
        observed = code_reference_filter_with_exclude_by_file.filter_definitions()
        assert len(observed) == 0
        assert {d.name for d in observed} == set()
        assert {d.filepath for d in observed} == set()

    @pytest.mark.integration
    def test_filter_definitions_exclude_by_file_and_name(
        self, code_reference_filter_with_exclude_by_file_and_name: CodeReferenceFilter
    ):
        observed = (
            code_reference_filter_with_exclude_by_file_and_name.filter_definitions()
        )
        assert len(observed) == 4
        assert {d.name for d in observed} == {
            "ExampleClass",
            "example_classmethod",
            "example_method_with_args",
            "example_staticmethod",
        }
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    @pytest.mark.integration
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
        assert {d.name for d in observed} == {
            "ExampleClass",
            "example_classmethod",
            "example_method",
            "example_method_with_args",
            "example_module_level_function",
            "example_staticmethod",
        }
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    @pytest.mark.integration
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
        assert {d.name for d in observed} == {
            "example_method",
            "example_module_level_function",
        }
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }

    @pytest.mark.integration
    def test_filter_definitions_include_by_file_and_name_already_excluded_not_used_in_docs_example(
        self,
        code_reference_filter_with_include_by_file_and_name_not_used_in_docs_example_exclude_file: CodeReferenceFilter,
    ):
        """What does this test and why?

        Include overrides exclude. Method that was not included in docs examples
        is still included if manually added.
        """
        observed = (
            code_reference_filter_with_include_by_file_and_name_not_used_in_docs_example_exclude_file.filter_definitions()
        )
        assert len(observed) == 1
        assert {d.name for d in observed} == {
            "example_no_usages_in_sample_docs_example_python_file_string",
        }
        assert {d.filepath for d in observed} == {
            pathlib.Path(
                "great_expectations/sample_with_definitions_python_file_string.py"
            )
        }


@pytest.fixture
def public_api_report(
    code_reference_filter_with_no_include_exclude: CodeReferenceFilter,
    repo_root: pathlib.Path,
) -> PublicAPIReport:
    return PublicAPIReport(
        definitions=code_reference_filter_with_no_include_exclude.filter_definitions(),
        repo_root=repo_root,
    )


@pytest.fixture
def public_api_report_filter_out_file(
    code_reference_filter_with_exclude_by_file: CodeReferenceFilter,
    repo_root: pathlib.Path,
) -> PublicAPIReport:
    return PublicAPIReport(
        definitions=code_reference_filter_with_exclude_by_file.filter_definitions(),
        repo_root=repo_root,
    )


class TestPublicAPIReport:
    @pytest.mark.unit
    def test_instantiate(self, public_api_report: PublicAPIReport):
        assert isinstance(public_api_report, PublicAPIReport)

    @pytest.mark.integration
    def test_generate_printable_definitions(self, public_api_report: PublicAPIReport):
        expected: List[str] = [
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "ExampleClass",
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "example_classmethod",
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "example_method",
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "example_method_with_args",
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "example_module_level_function",
            "File: great_expectations/sample_with_definitions_python_file_string.py Name: "
            "example_staticmethod",
        ]
        observed = public_api_report.generate_printable_definitions()
        assert observed == expected

    @pytest.mark.integration
    def test_generate_printable_definitions_exclude_by_file(
        self, public_api_report_filter_out_file: PublicAPIReport
    ):
        expected: List[str] = []
        observed = public_api_report_filter_out_file.generate_printable_definitions()
        assert observed == expected


class TestIncludeExcludeDefinition:
    @pytest.mark.unit
    def test_instantiate_name_and_filepath(self):
        definition = IncludeExcludeDefinition(
            reason="reason", name="name", filepath=pathlib.Path("filepath")
        )
        assert isinstance(definition, IncludeExcludeDefinition)

    @pytest.mark.unit
    def test_instantiate_filepath_only(self):
        definition = IncludeExcludeDefinition(
            reason="reason", filepath=pathlib.Path("filepath")
        )
        assert isinstance(definition, IncludeExcludeDefinition)

    @pytest.mark.unit
    def test_instantiate_name_and_filepath_no_reason(self):
        with pytest.raises(TypeError):
            IncludeExcludeDefinition(name="name", filepath=pathlib.Path("filepath"))

    @pytest.mark.unit
    def test_instantiate_name_only(self):
        with pytest.raises(ValueError) as exc:
            IncludeExcludeDefinition(reason="reason", name="name")

        assert (
            "You must provide a filepath if also providing a name" in exc.value.args[0]
        )

    @pytest.mark.unit
    def test_instantiate_reason_only(self):
        with pytest.raises(ValueError) as exc:
            IncludeExcludeDefinition(reason="reason")

        assert (
            "You must provide at least a filepath or filepath and name"
            in exc.value.args[0]
        )
