import ast
import glob
import importlib
import inspect
import pathlib
from typing import List, Set

import astunparse


# Action: Swap the order of implementation. Start with generating the list of all methods classes etc from the files under test. Bump against list of what is decorated with @public_api. Set up linter for docstrings, focusing on public_api. Only filter with the snippet finder if the list is too messy.


# First pass - just get a set of all names. Don't worry about absolute paths yet.


class DocExampleParser:
    """Parse examples from docs to find classes, methods and functions used."""

    def __init__(self, repo_root: pathlib.Path) -> None:
        self.repo_root = repo_root

    def retrieve_definitions(self):
        """Retrieve all definitions used (class, method + function)."""
        filename = "tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py"
        # filename = "great_expectations/data_context/data_context/abstract_data_context.py"
        filepath = self.repo_root / filename


        # self._get_and_print_ast(filepath=filepath)
        # self._import_file_as_module(filepath=filepath)
        tree = self._parse_file_to_ast_tree(filepath=filepath)
        function_calls = self._list_all_function_calls(tree=tree)
        function_names = self._get_function_names(calls=function_calls)
        print(function_names)



    def _import_file_as_module(self, filepath: pathlib.Path) -> None:
        loader = importlib.machinery.SourceFileLoader('mymodule', str(filepath))
        spec = importlib.util.spec_from_loader('mymodule', loader)
        mymodule = importlib.util.module_from_spec(spec)
        dir(mymodule)

    def _get_module_name(self, filepath: pathlib.Path) -> None:
        print(inspect.getmodulename(str(filepath)))



    def _get_and_print_ast(self, filepath: pathlib.Path) -> None:
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)
        print(astunparse.dump(tree))


    def _parse_file_to_ast_tree(self, filepath: pathlib.Path) -> ast.AST:
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)
        return tree



    def _list_classes_in_file(self, filepath: pathlib.Path) -> List[ast.ClassDef]:
        # TODO: Make this return with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        class_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_defs.append(node)

        return class_defs

    def _list_module_level_functions_in_file(self, filepath: pathlib.Path) -> List[ast.FunctionDef]:
        pass

    def _list_methods_in_class(self, class_definition: ast.ClassDef) -> List[ast.FunctionDef]:
        pass

    def _list_all_function_calls(self, tree: ast.AST) -> List[ast.Call]:
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                calls.append(node)

        return calls

    def _get_function_names(self, calls: List[ast.Call]) -> Set[str]:
        names = []
        for call in calls:
            if isinstance(call.func, ast.Attribute):
                names.append(call.func.attr)
            elif isinstance(call.func, ast.Name):
                names.append(call.func.id)

        return set(names)



class PublicAPIChecker:
    """Check if functions, methods and classes are marked part of the PublicAPI."""

    def __init__(self, repo_root: pathlib.Path) -> None:
        self.repo_root = repo_root


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



def main():

    # Get code references
    repo_root = pathlib.Path(__file__).parent.parent
    docs_dir = repo_root / "docs"
    files = glob.glob(f"{docs_dir}/**/*.md", recursive=True)

    # report_generator = PublicAPIDocSnippetRetriever(repo_root=repo_root)
    # report_generator.generate_report(docs_code_references)



    # Parse references
    # Generate set of module level functions, classes, and methods
    # Parse full great_expectations code, find everything decorated @public_api

    # public_api_checker = PublicAPIChecker(repo_root=repo_root)
    # public_api_checker.get_all_public_api_functions()
    # Report of what is in the references and not marked @public_api
    # Report on which do not have corresponding sphinx source stub files (unless we end up autogenerating these)

    doc_example_parser = DocExampleParser(repo_root=repo_root)
    doc_example_parser.retrieve_definitions()






if __name__ == "__main__":
    main()