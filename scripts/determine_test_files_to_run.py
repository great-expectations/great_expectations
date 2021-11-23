"""
Usage: `python determine_test_files_to_run.py`

This script is used in our Azure pipeline (azure-pipelines.yml) to determine which test files to run in CI/CD.
Rather than test all tests each time, the test files that are selected are based on which source files that have changed;
the specific method in which this is done is explained in detail below.

The script takes the following steps:
    1. Determine which files have changed in the last commit (when compared to `develop`)
    2. For each changed file, find which files it depends on and which files depend on it (i.e. "relevant source files")
    3. For each relevant source file, determine the corresponding test file.

By determining which files are related to which other files, we're able to create a directed, acyclic graph from our codebase.

Let's look at the following example:
  ```
  # foo.py
  from bar import bar
  from baz import baz
  ```
The module `foo` depends on `bar` and `baz` so the following links are created in the dependency graph:
  `foo` <--> `bar`
  `foo` <--> `baz`
Now that we know that `foo`, `bar`, and `baz` are strongly coupled, we want our testing strategy to reflect that.

Upon creating a valid graph, we can use standard graph traversal algorithms to test modules that are dependent or relevant to
the changed file. To determine which tests to run, we create yet another graph; this one parses our test suite and determines
which source files are associated with a given test file. Once we have all our relevant source files and our mapping between
source file and test file, we can simple feed in our files to determine which tests need to be run in a given CI/CD cycle.
  ```
  # test_qux.py
  from foo import foo
  ```
The `test_qux.py` file directly links to `foo` (which is associated with `bar` and `baz`). A change in one will cause `test_qux.py` to run.

While this script does not provide as much coverage as a traditional test run, the fact that it traverses GE's internal dependency
graph layer by layer to determine the most relevant files allows us to maintain high coverage (all while improving performance).

If a key file like `data_context.py` is changed, the vast majority of the test suite will run because the corresponding node in
the dependency graph has so many ingoing and outgoing connections. This treatment of "high traffic" areas allow us to keep a watchful eye
over the most important and used parts of our codebase.

"""
import ast
import glob
import os
import subprocess
from typing import Dict, List


def get_changed_files() -> List[str]:
    """git diff HEAD origin/develop --name-only"""
    process = subprocess.run(
        ["git", "diff", "HEAD", "origin/develop", "--name-only"], stdout=subprocess.PIPE
    )
    files = [f.decode("utf-8") for f in process.stdout.splitlines()]
    return files


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


def main() -> None:
    changed_files = get_changed_files()
    source_files = determine_relevant_source_files(changed_files, depth=2)
    files_to_test = determine_files_to_test(source_files)
    for file in files_to_test:
        print(file)


if __name__ == "__main__":
    main()
