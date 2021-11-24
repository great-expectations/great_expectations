"""
Usage: `python determine_test_files_to_run.py`

This script is used in our Azure pipeline (azure-pipelines.yml) to determine which test files to run in CI/CD.
Rather than test all tests each time, the test files that are selected are based on which source files that have changed;
the specific method in which this is done is explained in detail below.

The script takes the following steps:
    1. Determine which files have changed in the last commit (when compared to `develop`)
    2. For each changed file, find which files depend on it (i.e. "relevant source files")
    3. For each relevant source file, determine the associated test files and run them.

By determining which files are related to which other files, we're able to create a directed, acyclic graph from our codebase.

Let's look at the following example:
  ```
  # great_expectations/data_context/data_context.py
  from great_expectations.checkpoint import Checkpoint
  from great_expectations.core.batch import Batch
  ```
The module `data_context` depends on `checkpoint` and `batch` so the following links are created in the dependency graph:
  `checkpoint` --> `data_context`
  `batch` --> `data_context`
Now that we know that `data_context`, `checkpoint`, and `batch` are strongly coupled, we want our testing strategy to reflect that.

Upon creating a valid graph, we can use standard graph traversal algorithms to test modules that are dependent or relevant to
the changed file. To determine which tests to run, we create yet another graph; this one parses our test suite and determines
which source files are associated with a given test file. Once we have all our relevant source files and our mapping between
source file and test file, we can simple feed in our files to determine which tests need to be run in a given CI/CD cycle.
  ```
  # test_pandas_datasource.py
  from great_expectations.core.batch import Batch
  ```
The `test_pandas_datasource.py` file directly links to `batch` (which is associated with `data_context`). These linkages determine what tests are selected.
The specificity of the algorithm can be tweaked through the `depth` argument (how many layers out do you want to traverse).

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
from collections import namedtuple
from typing import Dict, List

Import = namedtuple("Import", ["source", "module", "name", "alias"])


def get_changed_files() -> List[str]:
    """Standard git diff against `develop` to determine list of changed files"""
    process = subprocess.run(
        ["git", "diff", "HEAD", "origin/develop", "--name-only"], stdout=subprocess.PIPE
    )
    files = [f.decode("utf-8") for f in process.stdout.splitlines()]
    return [f for f in files if f.startswith("great_expectations")]


def parse_imports(path: str) -> List[Import]:
    """Traverses a file using AST and determines relative imports (from within GE)"""
    imports = []
    with open(path) as f:
        root = ast.parse(f.read(), path)

    for node in ast.walk(root):
        if isinstance(node, ast.Import):
            module = []
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            module = node.module.split(".")
        else:
            continue

        for n in node.names:
            imp = Import(path, module, n.name.split("."), n.asname)
            imports.append(imp)

    return imports


def get_import_paths(imports: List[Import]) -> List[str]:
    """Takes a list of imports and determines the relative path to each source file or module"""
    paths = []
    for imp in imports:
        if "great_expectations" not in imp.module:
            continue
        path = ""
        if len(imp.module) == 1:
            if "DataContext" in imp.name:
                path = "great_expectations/data_context/data_context"
            elif "exceptions" in imp.name:
                path = "great_expectations/exceptions/exceptions"
        else:
            path = "/".join(x for x in imp.module)

        if os.path.isfile(f"{path}.py"):
            paths.append(f"{path}.py")
        # AST node points to a module so we add ALL files in that directory
        elif os.path.isdir(path):
            for file in glob.glob(f"{path}/**/*.py", recursive=True):
                paths.append(file)

    return paths


def create_dependency_graph(directory: str) -> Dict[str, List[str]]:
    """Traverse a given directory, parse all imports, and create a DAG linking source files to dependencies"""
    graph = {}
    for file in glob.glob(f"{directory}/**/*.py", recursive=True):
        imports = parse_imports(str(file))
        paths = get_import_paths(imports)

        for path in paths:
            if path not in graph:
                graph[path] = set()
            graph[path].add(file)

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
        # If we've hit a cycle, traversed past our stated depth, or touched a file that isn't GE, throw away the node
        if node in seen or d <= 0 or not node.startswith("great_expectations"):
            continue
        seen.add(node)
        res.add(node)
        for child in graph.get(node, []):
            stack.append((child, d - 1))

    return sorted(res)


def determine_relevant_source_files(changed_files: List[str], depth: int) -> List[str]:
    """Perform graph traversal on all changed files to determine which source files are possibly influenced by the commit"""
    ge_graph = create_dependency_graph("great_expectations")
    res = set()
    for file in changed_files:
        deps = traverse_graph(file, ge_graph, depth)
        res.update(deps)
    return sorted(res)


def determine_files_to_test(source_files: List[str]) -> List[str]:
    """Perform graph traversal on all source files to determine which test files need to be run"""
    tests_graph = create_dependency_graph("tests")
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
