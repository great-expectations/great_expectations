"""Report of which methods and classes are used in our public documentation.

This module contains utilities that scan our documentation examples and compare
with our codebase to determine which methods are used in examples and thus
should be considered part of our public API.

Typical usage example:

  main() method provided with typical usage.
  These utilities can also be used in tests to determine if there has been a
  change to our public API.
"""
import ast
import glob
import logging
import operator
import pathlib
from dataclasses import dataclass
from typing import List, Set, Union

import astunparse

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


# DONE  - AST walk through test scripts to find all imports of classes and methods that are GX related (can get the import location)
# DONE	- AST walk through test scripts to find all method calls (just get the names, not the location - can't import to do this since scripts have side effects)
# DONE	- AST walk through full codebase to find all classes and method names, this gets the definition location
# DONE	- Filter list of classes & methods from test scripts to only those found in the GX codebase (e.g. filter out print() or other python or 3rd party classes/methods)
# DONE  - Filter list of classes in the GX codebase to those found in the test scripts (after filtering out non GX related in test scripts)
# DONE	- Filter list of classes & methods to only those not already marked `public_api`
#   - Capture relative filepath and name instead of ast.FunctionDef, ast.ClassDef or ast.AsyncFunctionDef (eg. with Definition class)
#   - Clean up, change "test script" to "docs examples"?


class AstParser:
    def __init__(self, repo_root: pathlib.Path) -> None:
        self.repo_root = repo_root

    def print_tree(self, filepath: pathlib.Path) -> None:
        tree = self._parse_file_to_ast_tree(filepath=filepath)
        self._print_ast(tree=tree)

    def _print_ast(self, tree: ast.AST) -> None:
        """Pretty print an AST tree."""
        print(astunparse.dump(tree))

    def _parse_file_to_ast_tree(self, filepath: pathlib.Path) -> ast.AST:
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)
        return tree


class DocExampleParser:
    """Parse examples from docs to find classes, methods and functions used."""

    def __init__(self, repo_root: pathlib.Path, paths: Set[pathlib.Path]) -> None:
        self.repo_root = repo_root
        self.paths = paths

    def retrieve_all_usages_in_files(self) -> Set[str]:
        """

        Args:
            filepaths:

        Returns:

        """
        all_usages = set()
        for filepath in self.paths:
            file_usages = self.retrieve_all_usages_in_file(filepath=filepath)
            all_usages |= file_usages
        return all_usages

    def retrieve_all_usages_in_file(self, filepath: pathlib.Path) -> Set[str]:
        """Retrieve all class, method + functions used in test examples."""

        tree = self._parse_file_to_ast_tree(filepath=filepath)
        function_calls = self._list_all_function_calls(tree=tree)
        function_names = self._get_non_private_function_names(calls=function_calls)
        logger.debug(f"function_names: {function_names}")

        gx_imports = self._list_all_gx_imports(tree=tree)
        import_names = self._get_non_private_gx_import_names(imports=gx_imports)
        logger.debug(f"import_names: {import_names}")

        return function_names | import_names

    def _print_ast(self, tree: ast.AST) -> None:
        """Pretty print an AST tree."""
        print(astunparse.dump(tree))

    def _parse_file_to_ast_tree(self, filepath: pathlib.Path) -> ast.AST:
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)
        return tree

    def _list_all_gx_imports(
        self, tree: ast.AST
    ) -> List[Union[ast.Import, ast.ImportFrom]]:
        """Get all of the GX related imports in a file."""

        imports = []

        for node in ast.walk(tree):
            node_is_imported_from_gx = isinstance(
                node, ast.ImportFrom
            ) and node.module.startswith("great_expectations")
            node_is_gx_import = isinstance(node, ast.Import) and any(
                n.name.startswith("great_expectations") for n in node.names
            )
            if node_is_imported_from_gx:
                imports.append(node)
            elif node_is_gx_import:
                imports.append(node)

        return imports

    def _get_non_private_gx_import_names(
        self, imports: List[Union[ast.Import, ast.ImportFrom]]
    ) -> Set[str]:

        names = []
        for import_ in imports:
            if not isinstance(import_, (ast.Import, ast.ImportFrom)):
                raise TypeError(
                    f"`imports` should only contain ast.Import, ast.ImportFrom types, you provided {type(import_)}"
                )

            # Generally there is only 1 alias,
            # but we add all names if there are multiple aliases to be safe.
            names.extend([n.name for n in import_.names if not n.name.startswith("_")])

        return set(names)

    def _list_all_function_calls(self, tree: ast.AST) -> List[ast.Call]:
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                calls.append(node)

        return calls

    def _get_non_private_function_names(self, calls: List[ast.Call]) -> Set[str]:
        names = []
        for call in calls:
            if isinstance(call.func, ast.Attribute):
                name = call.func.attr
            elif isinstance(call.func, ast.Name):
                name = call.func.id

            if not name.startswith("_"):
                names.append(name)

        return set(names)


@dataclass(frozen=True)
class Definition:
    name: str
    filepath: pathlib.Path
    ast_definition: Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]


class GXCodeParser:
    """"""

    def __init__(self, repo_root: pathlib.Path, paths: Set[pathlib.Path]) -> None:
        """

        Args:
            repo_root:
            paths:
        """
        self.repo_root = repo_root
        self.paths = paths

    def get_all_non_private_class_method_and_function_names_from_definitions_in_files(
        self,
    ) -> Set[str]:
        all_usages = set()
        for filepath in self.paths:
            file_usages = self.get_all_non_private_class_method_and_function_names_from_definitions_in_file(
                filepath=filepath
            )
            all_usages |= file_usages
        return all_usages

    def get_all_non_private_class_method_and_function_names_from_definitions_in_file(
        self, filepath: pathlib.Path
    ) -> Set[str]:
        """

        Args:
            filepath:

        Returns:

        """
        names = self.get_all_class_method_and_function_names_from_definitions_in_file(
            filepath=filepath
        )
        return set([name for name in names if not name.startswith("_")])

    def get_all_class_method_and_function_names_from_definitions_in_file(
        self, filepath: pathlib.Path
    ) -> Set[str]:
        """

        Args:
            filepath:

        Returns:

        """
        definitions: Set[
            Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
        ] = self.get_all_class_method_and_function_definitions_from_file(
            filepath=filepath
        )

        return set([definition.name for definition in definitions])

    def get_all_class_method_and_function_definitions_from_files(
        self,
    ) -> Set[Definition]:
        all_usages: Set[Definition] = set()
        for filepath in self.paths:
            file_usages = self.get_all_class_method_and_function_definitions_from_file(
                filepath=filepath
            )
            file_usages_definitions: Set[Definition] = set(
                [
                    Definition(
                        name=usage.name,
                        filepath=filepath.relative_to(
                            self.repo_root / "great_expectations"
                        ),
                        ast_definition=usage,
                    )
                    for usage in file_usages
                ]
            )
            all_usages |= file_usages_definitions
        return all_usages

    def get_all_class_method_and_function_definitions_from_file(
        self, filepath: pathlib.Path
    ) -> Set[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]]:
        """

        Args:
            filepath:

        Returns:

        """
        tree = self._parse_file_to_ast_tree(filepath=filepath)
        all_defs: List[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]] = []
        all_defs.extend(self._list_class_definitions(tree=tree))
        all_defs.extend(self._list_function_definitions(tree=tree))

        return set(all_defs)

    def _parse_file_to_ast_tree(self, filepath: pathlib.Path) -> ast.AST:
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)
        return tree

    def _list_class_definitions(self, tree: ast.AST) -> List[ast.ClassDef]:

        class_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_defs.append(node)

        return class_defs

    def _list_function_definitions(
        self, tree: ast.AST
    ) -> List[Union[ast.FunctionDef, ast.AsyncFunctionDef]]:
        function_definitions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) or isinstance(
                node, ast.AsyncFunctionDef
            ):
                function_definitions.append(node)

        return function_definitions


class PublicAPIChecker:
    """Check if functions, methods and classes are marked part of the PublicAPI."""

    def __init__(
        self,
        repo_root: pathlib.Path,
        doc_example_parser: DocExampleParser,
        gx_code_parser: GXCodeParser,
    ) -> None:
        self.repo_root = repo_root
        self.doc_example_parser = doc_example_parser
        self.gx_code_parser = gx_code_parser

    def write_printable_definitions_to_file(
        self, filepath: pathlib.Path, definitions: Set[Definition]
    ) -> None:
        """

        Args:
            filepath:
            definitions:

        Returns:

        """

        printable_definitions = self.printable_definitions(definitions=definitions)
        with open(filepath, "w") as f:
            f.write("\n".join(printable_definitions))

    def printable_definitions(self, definitions: Set[Definition]) -> List[str]:
        """

        Args:
            definitions:

        Returns:

        """
        sorted_definitions_list = sorted(
            list(definitions), key=operator.attrgetter("filepath")
        )
        sorted_definitions_strings: List[str] = []
        for definition in sorted_definitions_list:
            sorted_definitions_strings.append(
                f"File: {str(definition.filepath)} Name: {definition.name}"
            )

        seen = set()
        sorted_definitions_strings_no_dupes = [
            d for d in sorted_definitions_strings if not (d in seen or seen.add(d))
        ]

        return sorted_definitions_strings_no_dupes

    def gx_code_definitions_appearing_in_docs_examples_and_not_marked_public_api(
        self,
    ) -> Set[Definition]:
        gx_code_definitions_appearing_in_docs_examples = (
            self.gx_code_definitions_appearing_in_docs_examples()
        )
        return set(
            [
                d
                for d in gx_code_definitions_appearing_in_docs_examples
                if not self._is_definition_marked_public_api(d.ast_definition)
            ]
        )

    def gx_code_definitions_appearing_in_docs_examples(self) -> Set[Definition]:
        """Filter out all GX classes and methods except for those used in docs examples.

        Returns:

        """
        gx_usages_in_docs_examples = self.filter_test_script_classes_and_methods()
        gx_code_definitions = (
            self.gx_code_parser.get_all_class_method_and_function_definitions_from_files()
        )
        gx_code_definitions_appearing_in_docs_examples = set(
            [d for d in gx_code_definitions if d.name in gx_usages_in_docs_examples]
        )
        return gx_code_definitions_appearing_in_docs_examples

    def filter_test_script_classes_and_methods(self) -> Set[str]:
        """Filter out non-GX usages from test scripts.

        Returns:

        """
        doc_example_usages = self.doc_example_parser.retrieve_all_usages_in_files()
        gx_code_definitions = (
            self.gx_code_parser.get_all_non_private_class_method_and_function_names_from_definitions_in_files()
        )

        doc_example_usages_of_gx_code = doc_example_usages.intersection(
            gx_code_definitions
        )
        return doc_example_usages_of_gx_code

    def _is_definition_marked_public_api(
        self, definition: Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
    ) -> bool:

        result = False
        found_decorators = self._get_decorator_names(definition=definition)

        if "public_api" in found_decorators:
            result = True

        return result

    def _get_decorator_names(
        self, definition: Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
    ) -> Set[str]:
        def flatten_attr(node):
            if isinstance(node, ast.Attribute):
                return str(flatten_attr(node.value)) + "." + node.attr
            elif isinstance(node, ast.Name):
                return str(node.id)
            else:
                pass

        found_decorators = []
        for decorator in definition.decorator_list:
            if isinstance(decorator, ast.Name):
                found_decorators.append(decorator.id)
            elif isinstance(decorator, ast.Attribute):
                found_decorators.append(flatten_attr(decorator))

        return set(found_decorators)

    def get_all_public_api_functions(self):

        filepath = (
            self.repo_root
            / "great_expectations/data_context/data_context/abstract_data_context.py"
        )
        print(self._list_public_functions_in_file(filepath=filepath))

    def _list_public_functions_in_file(self, filepath: pathlib.Path) -> List[str]:
        # TODO: Make this return function with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        def flatten_attr(node):
            if isinstance(node, ast.Attribute):
                return str(flatten_attr(node.value)) + "." + node.attr
            elif isinstance(node, ast.Name):
                return str(node.id)
            else:
                pass

        public_functions: List[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                found_decorators = []
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name):
                        found_decorators.append(decorator.id)
                    elif isinstance(decorator, ast.Attribute):
                        found_decorators.append(flatten_attr(decorator))

                if "public_api" in found_decorators:
                    public_functions.append(node.name)

        return public_functions


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).parent.parent


def _default_doc_example_paths() -> Set[pathlib.Path]:
    """Get all paths of doc examples (test scripts)."""
    base_directory = _repo_root() / "tests" / "integration" / "docusaurus"
    paths = glob.glob(f"{base_directory}/**/*.py", recursive=True)
    return set([pathlib.Path(p) for p in paths])


def _default_gx_code_paths() -> Set[pathlib.Path]:
    """All gx modules related to the main library."""
    base_directory = _repo_root() / "great_expectations"
    paths = glob.glob(f"{base_directory}/**/*.py", recursive=True)
    return set([pathlib.Path(p) for p in paths])


def main():

    doc_example_parser = DocExampleParser(
        repo_root=_repo_root(), paths=_default_doc_example_paths()
    )

    gx_code_parser = GXCodeParser(
        repo_root=_repo_root(), paths=_default_gx_code_paths()
    )

    public_api_checker = PublicAPIChecker(
        repo_root=_repo_root(),
        doc_example_parser=doc_example_parser,
        gx_code_parser=gx_code_parser,
    )

    logger.debug("Printing GX usages in test scripts")
    gx_code_definitions_appearing_in_docs_examples = (
        public_api_checker.gx_code_definitions_appearing_in_docs_examples()
    )
    printable_definitions = public_api_checker.printable_definitions(
        definitions=gx_code_definitions_appearing_in_docs_examples
    )
    for printable_definition in printable_definitions:
        logger.info(printable_definition)

    public_api_checker.write_printable_definitions_to_file(
        filepath=_repo_root() / "public_api_report.txt",
        definitions=gx_code_definitions_appearing_in_docs_examples,
    )


if __name__ == "__main__":
    main()
