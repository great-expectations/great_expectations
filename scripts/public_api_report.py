import ast
import glob
import importlib
import inspect
import logging
import pathlib
import sys
from types import ModuleType
from typing import List, Set, Union

import astunparse

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
# TODO: Change log level to Info
logger.setLevel(logging.DEBUG)


# DONE  - AST walk through test scripts to find all imports of classes and methods that are GX related (can get the import location)
# DONE	- AST walk through test scripts to find all method calls (just get the names, not the location - can't import to do this since scripts have side effects)
# DONE	- AST walk through full codebase to find all classes and method names, this gets the definition location
# DONE	- Filter list of classes & methods from test scripts to only those found in the GX codebase (e.g. filter out print() or other python or 3rd party classes/methods)
#   - Filter list of classes in the GX codebase to those found in the test scripts (after filtering out non GX related in test scripts)
# 	- Filter list of classes & methods to only those not already marked `public_api`

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
        for filepath in  self.paths:
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



    def _list_all_gx_imports(self, tree: ast.AST) -> List[Union[ast.Import, ast.ImportFrom]]:
        """Get all of the GX related imports in a file."""

        imports = []

        for node in ast.walk(tree):
            node_is_imported_from_gx = isinstance(node, ast.ImportFrom) and node.module.startswith("great_expectations")
            node_is_gx_import = isinstance(node, ast.Import) and any(n.name.startswith("great_expectations") for n in node.names)
            if node_is_imported_from_gx:
                imports.append(node)
            elif node_is_gx_import:
                imports.append(node)

        return imports


    def _get_non_private_gx_import_names(self, imports: List[Union[ast.Import, ast.ImportFrom]]) -> Set[str]:

        names = []
        for import_ in imports:
            if not isinstance(import_, (ast.Import, ast.ImportFrom)):
                raise TypeError(f"`imports` should only contain ast.Import, ast.ImportFrom types, you provided {type(import_)}")

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


    def get_all_non_private_class_method_and_function_names_from_definitions_in_files(self) -> Set[str]:
        all_usages = set()
        for filepath in self.paths:
            file_usages = self.get_all_non_private_class_method_and_function_names_from_definitions_in_file(filepath=filepath)
            all_usages |= file_usages
        return all_usages

    def get_all_non_private_class_method_and_function_names_from_definitions_in_file(self, filepath: pathlib.Path) -> Set[str]:
        """

        Args:
            filepath:

        Returns:

        """
        names = self.get_all_class_method_and_function_names_from_definitions_in_file(filepath=filepath)
        return set([name for name in names if not name.startswith("_")])


    def get_all_class_method_and_function_names_from_definitions_in_file(self, filepath: pathlib.Path) -> Set[str]:
        """

        Args:
            filepath:

        Returns:

        """
        definitions: List[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]] = self.get_all_class_method_and_function_definitions_from_file(filepath=filepath)

        return set([definition.name for definition in definitions])



    def get_all_class_method_and_function_definitions_from_file(self, filepath: pathlib.Path) -> List[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]]:
        """

        Args:
            filepath:

        Returns:

        """
        tree = self._parse_file_to_ast_tree(filepath=filepath)
        all_defs: List[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]] = []
        all_defs.extend(self._list_class_definitions(tree=tree))
        all_defs.extend(self._list_function_definitions(tree=tree))

        return all_defs


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

    def _list_function_definitions(self, tree: ast.AST) -> List[Union[ast.FunctionDef, ast.AsyncFunctionDef]]:
        function_definitions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                function_definitions.append(node)

        return function_definitions


class PublicAPIChecker:
    """Check if functions, methods and classes are marked part of the PublicAPI."""

    def __init__(self, repo_root: pathlib.Path, doc_example_parser: DocExampleParser, gx_code_parser: GXCodeParser) -> None:
        self.repo_root = repo_root
        self.doc_example_parser = doc_example_parser
        self.gx_code_parser = gx_code_parser



    def filter_test_script_classes_and_methods(self) -> Set[str]:
        """Filter out non-GX usages from test scripts.

        Returns:

        """
        doc_example_usages = self.doc_example_parser.retrieve_all_usages_in_files()
        gx_code_definitions = self.gx_code_parser.get_all_non_private_class_method_and_function_names_from_definitions_in_files()

        doc_example_usages_of_gx_code = doc_example_usages.intersection(gx_code_definitions)
        return doc_example_usages_of_gx_code


    def get_all_public_api_functions(self):

        filepath = self.repo_root / "great_expectations/data_context/data_context/abstract_data_context.py"
        print(self._list_public_functions_in_file(filepath=filepath))


    def _list_public_functions_in_file(self, filepath: pathlib.Path) -> List[str]:
        # TODO: Make this return function with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        def flatten_attr(node):
            if isinstance(node, ast.Attribute):
                return str(flatten_attr(node.value)) + '.' + node.attr
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

                # print(node.name, found_decorators)

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

    # Get code references in docs
    # repo_root = pathlib.Path(__file__).parent.parent
    # docs_dir = repo_root / "docs"
    # files = glob.glob(f"{docs_dir}/**/*.md", recursive=True)

    # report_generator = PublicAPIDocSnippetRetriever(repo_root=repo_root)
    # report_generator.generate_report(docs_code_references)



    # Parse references
    # Generate set of module level functions, classes, and methods
    # Parse full great_expectations code, find everything decorated @public_api

    # public_api_checker = PublicAPIChecker(repo_root=repo_root)
    # public_api_checker.get_all_public_api_functions()
    # Report of what is in the references and not marked @public_api
    # Report on which do not have corresponding sphinx source stub files (unless we end up autogenerating these)

    # doc_example_parser = DocExampleParser(repo_root=_repo_root(), paths=_default_doc_example_paths())
    # doc_example_parser.retrieve_definitions()
    # doc_example_parser.list_all_class_imports()

    # filename = pathlib.Path(
    #     "tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py")
    # filepath = repo_root / filename
    # print(doc_example_parser._list_all_gx_imports(filepath=filepath))

    filename = pathlib.Path(
        "scripts/sample_test_script.py")
    filepath = _repo_root() / filename
    # print(doc_example_parser._import_file_as_module(filepath=filepath))
    # print(doc_example_parser._list_classes_imported_in_file(filepath=filepath))

    doc_example_parser = DocExampleParser(repo_root=_repo_root(), paths=_default_doc_example_paths())
    # print(doc_example_parser.retrieve_all_usages_in_file(filepath=filepath))

    filename = pathlib.Path(
        "great_expectations/data_context/data_context/abstract_data_context.py")
    filepath = _repo_root() / filename
    gx_code_parser = GXCodeParser(repo_root=_repo_root(), paths=_default_gx_code_paths())
    # print(gx_code_parser.get_all_class_method_and_function_definitions_from_file(filepath=filepath))
    # print(gx_code_parser.get_all_non_private_class_method_and_function_names_from_definitions_in_file(filepath=filepath))

    ast_parser = AstParser(repo_root=_repo_root())
    # ast_parser.print_tree(filepath=filepath)

    public_api_checker = PublicAPIChecker(
        repo_root=_repo_root(),
        doc_example_parser=doc_example_parser,
        gx_code_parser=gx_code_parser
    )
    logger.debug("Printing GX usages in test scripts")
    print(public_api_checker.filter_test_script_classes_and_methods())

        # filename = "tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py"
        # # filename = "great_expectations/data_context/data_context/abstract_data_context.py"
        # filepath = self.repo_root / filename

if __name__ == "__main__":
    main()