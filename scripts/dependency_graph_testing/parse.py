import ast
import difflib
import glob
import logging
import os
from collections import namedtuple
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)
Import = namedtuple("Import", ["source", "module", "name", "alias"])


def get_dependency_graphs(
    root_dir: str, tests_dir: str
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Wrapper method around all parsing logic/behavior.

    Output is the two dependency graphs (source and tests) necessary for graph
    traversal and test file determination.
    """
    declaration_map = parse_declaration_nodes(root_dir)
    fixture_map = parse_pytest_fixtures(tests_dir, declaration_map)
    source_dependency_graph = parse_import_nodes(root_dir, declaration_map)
    tests_dependency_graph = parse_tests_dependencies(
        tests_dir, root_dir, declaration_map, fixture_map
    )
    return source_dependency_graph, tests_dependency_graph


def parse_declaration_nodes(root_dir: str) -> Dict[str, List[str]]:
    """
    Traverses a given directory using AST to determine function and class declarations.

    Output is a mapping between symbols and the files that use them. This is used
    to help pinpoint where exactly a given symbol originates from when imported somewhere
    in the codebase.

    i.e. 'DataContext': 'great_expectations/data_context/data_context.py'
    """
    declaration_map = {}
    for path in glob.glob(f"{root_dir}/**/*.py", recursive=True):
        _parse_declaration_nodes(path, declaration_map)
    return declaration_map


def _parse_declaration_nodes(
    filepath: str, declaration_map: Dict[str, List[str]]
) -> None:
    with open(filepath) as f:
        root = ast.parse(f.read(), filepath)

    declaration_nodes = []
    for node in root.body:
        try:
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                declaration_nodes.append(node.name)
        except Exception as e:
            logger.info(f"Something went wrong when parsing declaration nodes: {e}")

    for name in declaration_nodes:
        if name not in declaration_map:
            declaration_map[name] = []
        declaration_map[name].append(filepath)


def parse_import_nodes(
    root_dir: str, declaration_map: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """
    Traverses a given directory using AST to determine relative imports.

    Output is a dependency graph that associates a given file/module with
    all of the files/modules that use it as a dependency.

    Parses both:
      * from great_expectations.x.y.z import ...
      * from great_expectation import ...
    """
    dependency_graph = {}
    for path in glob.glob(f"{root_dir}/**/*.py", recursive=True):
        file_imports = _parse_import_nodes(path, root_dir, declaration_map)
        _update_dict(dependency_graph, file_imports)
    return dependency_graph


def _parse_import_nodes(
    filepath: str,
    root_dir: str,
    declaration_map: Dict[str, List[str]],
    reverse_edges: bool = False,
) -> Dict[str, List[str]]:
    with open(filepath) as f:
        root = ast.parse(f.read(), filepath)

    imports = []
    try:
        for node in root.body:
            if isinstance(node, ast.Import):
                module = []
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                module = node.module.split(".")
            else:
                continue

            for n in node.names:
                import_ = Import(filepath, module, n.name.split("."), n.asname)
                imports.append(import_)
    except Exception as e:
        logger.info(f"Something went wrong when parsing import nodes: {e}")

    paths = set()
    for import_ in imports:
        partial_path = "/".join(m for m in import_.module)
        if (
            not import_.module
            or not import_.name
            or not partial_path.startswith(root_dir)
        ):
            continue
        key = import_.name[0]
        candidates = declaration_map.get(key, [])
        if len(candidates) == 0:
            logger.info(f"No suitable matches found between {filepath} and {key}")
            continue
        elif len(candidates) == 1:
            paths.add(candidates[0])
        else:
            # If we're not sure of the origin, use the stringified import statement and make an educated guess
            closest = difflib.get_close_matches(partial_path, candidates)[0]
            logger.info(
                f"Ambiguous import of {key} in {filepath} - selected {closest} out of {candidates}"
            )
            paths.add(closest)

    if reverse_edges:
        return {path: [filepath] for path in paths}
    return {filepath: sorted(paths)}


def parse_pytest_fixtures(
    tests_dir: str, declaration_map: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """
    Traverses a given test directory using AST to determine the components that make up
    pytest fixtures.

    Output is a map between a fixture name and the various files/modules that use it as
    a dependency.

    i.e. 'basic_datasource': ['great_expectations/data_context/util.py']
    The 'basic_datasource' fixture utilizes 'instantiate_class_from_config', which
    comes from 'great_expectations/data_context/util.py'
    """
    fixture_map = {}
    for path in glob.glob(f"{tests_dir}/**/*.py", recursive=True):
        if os.path.basename(path) == "conftest.py":
            file_fixtures = _parse_pytest_fixtures(path, declaration_map)
            _update_dict(fixture_map, file_fixtures)
    return fixture_map


def _parse_pytest_fixtures(
    filepath: str, declaration_map: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    with open(filepath) as f:
        root = ast.parse(f.read(), filepath)

    fixture_nodes = []
    for node in root.body:
        try:
            if isinstance(node, ast.FunctionDef):
                for d in node.decorator_list:
                    if isinstance(d, ast.Attribute) and d.attr == "fixture":
                        fixture_nodes.append(node)
                    elif isinstance(d, ast.Call) and d.func.attr == "fixture":
                        fixture_nodes.append(node)
        except Exception as e:
            logger.info(f"Something went wrong when parsing fixtures: {e}")

    # Parse the body of each fixture and find symbols.
    # If that symbol is something that was declared in the source files (class or function),
    # create an association between the fixture and the file that the symbol was declared in.
    fixture_map = {}
    for node in fixture_nodes:
        for child in ast.walk(node):
            for symbol in child.__dict__.values():
                if isinstance(symbol, str) and symbol in declaration_map:
                    candidates = declaration_map[symbol]
                    fixture_map[node.name] = []
                    for candidate in candidates:
                        fixture_map[node.name].append(candidate)

    return fixture_map


def parse_tests_dependencies(
    tests_dir: str,
    root_dir: str,
    declaration_map: Dict[str, List[str]],
    fixture_map: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """
    Traverses a given test directory using AST to determine what sources files
    our test files are importing.

    Output is a dependency graph that associates a given test file with all of the
    source file dependencies it has.

    i.e. `tests/data_context/test_data_context.py`: ['great_expectations/data_context/data_context.py']
    The test file imports symbols declared in the source file (thus resulting in an association)
    """
    tests_dependency_graph = {}
    for path in glob.glob(f"{tests_dir}/**/*.py", recursive=True):
        if os.path.basename(path).startswith("test_"):
            file_graph = _parse_tests_dependencies(
                path, root_dir, declaration_map, fixture_map
            )
            _update_dict(tests_dependency_graph, file_graph)

    return tests_dependency_graph


def _parse_tests_dependencies(
    filepath: str,
    root_dir: str,
    declaration_map: Dict[str, List[str]],
    fixture_map: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    with open(filepath) as f:
        root = ast.parse(f.read(), filepath)

    file_imports = _parse_import_nodes(
        filepath, root_dir, declaration_map, reverse_edges=True
    )
    for node in root.body:
        if isinstance(node, ast.FunctionDef):
            for symbol in node.args.args:
                arg = symbol.arg
                # If the pytest function argument is a fixture, add that fixture's dependencies
                for test_dep in fixture_map.get(arg, []):
                    if filepath not in file_imports:
                        file_imports[filepath] = []
                    if test_dep not in file_imports[filepath]:
                        file_imports[filepath].append(test_dep)

    return file_imports


def _update_dict(
    to_update: Dict[str, List[str]], update_with: Dict[str, List[str]]
) -> None:
    for key, value in to_update.items():
        items = update_with.get(key, [])
        for item in items:
            if item not in value:
                value.append(item)

    for key, value in update_with.items():
        if key not in to_update:
            to_update[key] = value
