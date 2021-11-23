import ast
import glob
import os
from pprint import pprint
from typing import Dict, List


def parse_imports(path: str) -> List[str]:
    """Traverses a file using AST and determines relative imports (from within GE)"""
    with open(path) as f:
        imports = set()
        root: ast.Module = ast.parse(f.read())

        for node in ast.walk(root):
            # ast.Import is only used for external deps
            if not isinstance(node, ast.ImportFrom):
                continue

            # Only consider imports relevant to GE (note that "import great_expectations as ge" is discarded)
            if (
                isinstance(node.module, str)
                and "great_expectations" in node.module
                and node.module.count(".") > 0  # Excludes `import great_expectations`
            ):
                imports.add(node.module)

    return sorted(imports)


def get_import_paths(imports: List[str]) -> List[str]:
    """Takes a list of imports and determines the relative path to each source file or module"""
    paths = []

    for imp in imports:
        # AST nodes are formatted as "great_expectations.module.file"
        path = imp.replace(".", "/")

        if os.path.isfile(f"{path}.py"):
            paths.append(f"{path}.py")
        # AST node points to a module so we add ALL files in that directory
        elif os.path.isdir(path):
            for file in glob.glob(f"{path}/**/*.py", recursive=True):
                paths.append(file)

    return paths


def create_dependency_graph(
    directory: str, bidirectional: bool
) -> Dict[str, List[str]]:
    """Traverse a given directory, parse all imports, and create a DAG linking source files to dependencies"""
    files = glob.glob(f"{directory}/**/*.py")

    graph = {}
    for file in files:
        imports = parse_imports(file)
        paths = get_import_paths(imports)

        if bidirectional and file not in graph:
            graph[file] = set()

        for path in paths:
            if path not in graph:
                graph[path] = set()
            graph[path].add(file)
            if bidirectional:
                graph[file].add(path)

    for k, v in graph.items():
        graph[k] = sorted(v)
    return graph


def traverse_graph(root: str, graph: Dict[str, List[str]], depth: int) -> List[str]:
    """Perform an iterative, DFS-based traversal of a given dependency graph"""
    stack = [(root, depth)]
    seen = set()
    res = set()

    while stack:
        node, d = stack.pop()
        if node in seen or d <= 0 or not node.startswith("great_expectations"):
            continue
        seen.add(node)
        res.add(node)
        for child in graph.get(node, []):
            stack.append((child, d - 1))

    return sorted(res)


def determine_relevant_source_files(changed_files: List[str], depth: int) -> List[str]:
    """Perform graph traversal on all changed files to determine which source files are possibly influenced in the commit"""
    ge_graph = create_dependency_graph("great_expectations", bidirectional=True)
    res = set()
    for file in changed_files:
        deps = traverse_graph(file, ge_graph, depth)
        res.update(deps)
    return sorted(res)


def determine_files_to_test(source_files: List[str]) -> List[str]:
    """Perform graph traversal on all source files to determine which test files need to be run"""
    tests_graph = create_dependency_graph("tests", bidirectional=False)
    res = set()
    for file in source_files:
        for test in tests_graph.get(file, []):
            test_filename = test.split("/")[-1]
            if test_filename.startswith("test_"):
                res.add(test)
    return sorted(res)


if __name__ == "__main__":
    changed_files = [
        "great_expectations/render/renderer/site_builder.py",
    ]
    source_files = determine_relevant_source_files(changed_files, 2)
    files_to_test = determine_files_to_test(source_files)

    for file in files_to_test:
        print(file)
