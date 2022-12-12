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



# Action: Swap the order of implementation. Start with generating the list of all methods classes etc from the files under test. Bump against list of what is decorated with @public_api. Set up linter for docstrings, focusing on public_api. Only filter with the snippet finder if the list is too messy.


# First pass - just get a set of all names. Don't worry about absolute paths yet.

# Idea for generating list from test scripts -
# 1. Parse test scripts with AST and add any class imports
# 2. Import the test file as a module. Inspect items imported to determine where they
#    were imported from. (maybe using co_filename attribute?)
# 3. Parse great_expectations and create list of all classes, methods and functions. Note which are marked @public_api. Note the import path.


class DocExampleParser:
    """Parse examples from docs to find classes, methods and functions used."""

    def __init__(self, repo_root: pathlib.Path, paths: List[pathlib.Path]) -> None:
        self.repo_root = repo_root
        self.paths = paths

    def retrieve_classes_methods_and_functions_imported(self):
        """Retrieve all class, method + functions imported in test examples."""

        # For each path, import and get classes, methods and functions.
        for path in self.paths:
            logger.debug(f"Retrieving imports from path: {path}")
            imported_module = self._import_file_as_module(filepath=path)
            classes = self._list_classes_imported_in_module(module=imported_module)
            methods = self._list_methods_imported_in_module(module=imported_module)
            functions = self._list_functions_imported_in_module(module=imported_module)

            print("Classes:", classes)
            print("Methods:", methods)
            print("Functions:", functions)





    def retrieve_definitions(self):
        """Retrieve all class, method + functions used in test examples."""
        filename = "tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py"
        # filename = "great_expectations/data_context/data_context/abstract_data_context.py"
        filepath = self.repo_root / filename


        # self._get_and_print_ast(filepath=filepath)
        # self._import_file_as_module(filepath=filepath)
        tree = self._parse_file_to_ast_tree(filepath=filepath)
        function_calls = self._list_all_function_calls(tree=tree)
        function_names = self._get_function_names(calls=function_calls)
        print(function_names)


    def list_all_class_imports(self):
        raise NotImplementedError
        # filename = pathlib.Path("tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py")
        # filepath = self.repo_root / filename
        # tree = self._parse_file_to_ast_tree(filepath=filepath)
        # print(astunparse.dump(tree))
        #
        # imports = []
        #
        # for node in ast.walk(tree):
        #     if isinstance(node, ast.ImportFrom):
        #         pass
        #     elif isinstance(node, ast.Import):
        #         imports.append(node)
        #
        # return imports


    def _import_file_as_module(self, filepath: pathlib.Path) -> ModuleType:

        # TODO: Clean up and only include necessary steps below:
        loader = importlib.machinery.SourceFileLoader('mymodule', str(filepath))
        spec = importlib.util.spec_from_loader('mymodule', loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules["mymodule"] = module
        spec.loader.exec_module(module)
        members = inspect.getmembers(module)
        return module




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



    def _list_classes_defined_in_file(self, filepath: pathlib.Path) -> List[ast.ClassDef]:
        # TODO: Make this return with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        class_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_defs.append(node)

        return class_defs


    def _list_classes_imported_in_module(self, module: ModuleType) -> str: # TODO: Placeholder type
        # TODO: Docstring
        return inspect.getmembers(module, inspect.isclass)


    def _list_methods_imported_in_module(self, module: ModuleType) -> str: # TODO: Placeholder type
        # TODO: Docstring
        return inspect.getmembers(module, inspect.ismethod)


    def _list_functions_imported_in_module(self, module: ModuleType) -> str: # TODO: Placeholder type
        # TODO: Docstring
        return inspect.getmembers(module, inspect.isfunction)



    def _list_classes_imported_in_file(self, filepath: pathlib.Path) -> List[ast.ClassDef]:
        # TODO: Make this return with dotted path, not just str
        module = self._import_file_as_module(filepath=filepath)
        classes = inspect.getmembers(module, inspect.isclass)
        return classes



    def _list_all_gx_imports_in_file(self, filepath: pathlib.Path) -> List[Union[ast.Import, ast.ImportFrom]]:
        """Get all of the GX related imports in a file."""

        tree = self._parse_file_to_ast_tree(filepath=filepath)


        imports = []

        for node in ast.walk(tree):
            node_is_imported_from_gx = isinstance(node, ast.ImportFrom) and node.module.startswith("great_expectations")
            node_is_gx_import = isinstance(node, ast.Import) and any(n.name.startswith("great_expectations") for n in node.names)
            if node_is_imported_from_gx:
                imports.append(node)
            elif node_is_gx_import:
                imports.append(node)

        return imports


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



def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).parent.parent

def _default_paths() -> List[pathlib.Path]:
    """Get all paths of test scripts."""
    docs_dir = _repo_root() / "tests" / "integration" / "docusaurus"
    paths = glob.glob(f"{docs_dir}/**/*.py", recursive=True)



    # TODO: Check all files, not just sample
    # filename = pathlib.Path(
    #     "scripts/sample_test_script.py")
    # filepath = _repo_root() / filename
    # paths = [filepath]
    paths = paths[:5]

    return [pathlib.Path(p) for p in paths]


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

    # doc_example_parser = DocExampleParser(repo_root=_repo_root(), paths=_default_paths())
    # doc_example_parser.retrieve_definitions()
    # doc_example_parser.list_all_class_imports()

    # filename = pathlib.Path(
    #     "tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py")
    # filepath = repo_root / filename
    # print(doc_example_parser._list_all_gx_imports_in_file(filepath=filepath))

    # filename = pathlib.Path(
    #     "scripts/sample_test_script.py")
    # filepath = _repo_root() / filename
    # print(doc_example_parser._import_file_as_module(filepath=filepath))
    # print(doc_example_parser._list_classes_imported_in_file(filepath=filepath))

    doc_example_parser = DocExampleParser(repo_root=_repo_root(), paths=_default_paths())
    doc_example_parser.retrieve_classes_methods_and_functions_imported()



if __name__ == "__main__":
    main()