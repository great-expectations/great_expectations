"""
Usage: `python determine_tests_to_run.py --depth <DEPTH>`
Output: A list of '\n' delimited file paths that represent relevant test files to run
        (using `xargs`, we can feed in this list to `pytest` in our Azure config)

This script is used in our Azure pipeline (azure-pipelines-dependency-graph-testing.yml) to determine which test files to run in CI/CD.
Rather than test all tests each time, the test files that are selected are based on which source files have changed;
the specific method in which this is done is explained in detail below.

The script takes the following steps:
    1. Determine which files have changed in the last commit (when compared to `origin/develop`)
    2. For each changed file, find which files depend on it (i.e. "relevant source files")
    3. For each relevant source file, determine the associated test files and run them.

By determining which files are related to which other files, we're able to create a directed graph from our codebase.

Let's look at the following example:
  ```
  # great_expectations/data_context/data_context.py
  from great_expectations.checkpoint import Checkpoint
  from great_expectations.core.batch import Batch
  ```
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
import argparse
import ast
import glob
import os
import subprocess
import sys
from collections import namedtuple
from typing import Dict, List, Tuple

Import = namedtuple("Import", ["source", "module", "name", "alias"])


def get_changed_files(branch: str) -> Tuple[List[str], List[str]]:
    """
    Standard git diff against a given branch to determine list of changed files.

    Ensure that any changed tests are picked up and run (regardless of the file changes in 'great_expectations/')
    """
    process = subprocess.run(
        ["git", "diff", "HEAD", branch, "--name-only"], stdout=subprocess.PIPE
    )
    files = [f.decode("utf-8") for f in process.stdout.splitlines()]
    test_files = [f for f in files if f.startswith("tests")]
    source_files = [f for f in files if f.startswith("great_expectations")]
    return source_files, test_files


def parse_imports(path: str) -> List[Import]:
    """
    Traverses a file using AST and determines relative imports (from within GE)

    Parses both:
      * from great_expectations.x.y.z import ...
      * from great_expectation import ...
    """
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
    """
    Takes a list of Imports and determines the relative path to each source file or module.

    Imports of type:
      * `from great_expectations.x.y.z import ...` originate from `great_expectations/x/y/z.py`.
      * `from great_expectations import ...` must be evaluated on a case by case basis.
         Thankfully, there are only two edge cases we need to consider (DataContext and exceptions).

    If it is the case that an Import is pointing to a module or directory, we play it safe
    and add all files from that directory to ensure a high level of coverage.
    """
    paths = []
    for imp in imports:
        if "great_expectations" not in imp.module:
            continue
        path: str
        if len(imp.module) == 1:
            name = imp.name[0]
            # `from great_expectations import DataContext`
            if name == "DataContext":
                path = "great_expectations/data_context/data_context"
            # `from great_expectations import exceptions as ge_exceptions`
            elif name == "exceptions":
                path = "great_expectations/exceptions/exceptions"
            else:
                continue
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
    """
    Traverse a given directory, parse all imports, and create a directed graph linking source files to dependencies.

    The output dictionary has the following structure:
      * key: the dependency or import in the current file
      * val: the path of the current file

    Example:
      ```
      # great_expectations/data_context/data_context.py
      from great_expectations.checkpoint import Checkpoint
      from great_expectations.core.batch import Batch
      ```
    The following edges are added to the graph:
      `checkpoint` --> `data_context`
      `batch` --> `data_context`

    This allows us to traverse from a changed file to all files that use the origin file as a dependency.
    If we ever see a change in a given module, we immediately know which files are related and possibly impacted by the change.
    """
    graph = {}
    for file in glob.glob(f"{directory}/**/*.py", recursive=True):
        imports = parse_imports(str(file))
        paths = get_import_paths(imports)

        for path in paths:
            if path not in graph:
                graph[path] = set()
            graph[path].add(file)

    for key, value in graph.items():
        graph[key] = sorted(value)

    return graph


def traverse_graph(root: str, graph: Dict[str, List[str]], depth: int) -> List[str]:
    """
    Perform an iterative, DFS-based traversal of a given dependency graph.

    The provided `depth` arg determines how many layers you want to traverse. The smaller the number, the
    more relevant the matches (but the less overall coverage you obtain).

    The output is a series of files that are determined to be relevant to the root file.
    """
    stack = [(root, depth)]
    seen = set()

    while stack:
        node, d = stack.pop()
        # If we've hit a cycle, traversed past our stated depth, or touched a file that isn't GE, throw away the node
        if node in seen or d <= 0 or not node.startswith("great_expectations"):
            continue
        seen.add(node)
        for child in graph.get(node, []):
            stack.append((child, d - 1))

    return sorted(seen)


def determine_relevant_source_files(
    ge_dependency_graph: Dict[str, List[str]], changed_files: List[str], depth: int
) -> List[str]:
    """
    Perform graph traversal on all changed files to determine which source files are possibly influenced by the commit.
    Using a dependency graph from the `great_expectations/` directory, we can perform graph traversal for all changed files.
    """
    res = set()
    for file in changed_files:
        deps = traverse_graph(file, ge_dependency_graph, depth)
        res.update(deps)
    return sorted(res)


def determine_files_to_test(
    tests_dependency_graph: Dict[str, List[str]],
    source_files: List[str],
    changed_test_files: List[str],
) -> List[str]:
    """
    Perform graph traversal on all source files to determine which test files need to be run.

    Use a dependency graph of our `tests/` directory, we're able to map relevant source file to all tests that use
    that file as an import.
    """
    # Ensure we include test files that were caught by `get_changed_files()`
    res = {file for file in changed_test_files}
    for file in source_files:
        for test in tests_dependency_graph.get(file, []):
            # Some basic filtering is necessary to remove things like conftest.py
            test_filename = test.split("/")[-1]
            if test_filename.startswith("test_"):
                res.add(test)
    return sorted(res)


def _get_user_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        help="The relative path to your source files",
        default="great_expectations",
    )
    parser.add_argument(
        "--tests", help="The relative path to your tests", default="tests"
    )
    parser.add_argument(
        "--depth", help="Maximum depth reached in graph traversal", default=3, type=int
    )
    parser.add_argument(
        "--ignore",
        help="Exclude files that start with a given path prefix",
        default=[],
        nargs="+",
    )
    parser.add_argument(
        "--filter",
        help="Filter test runs by a given path prefix",
    )
    parser.add_argument(
        "--branch",
        help="The specific branch to diff against",
        default="origin/develop",
        type=str,
    )
    parsed = parser.parse_args()
    return parsed


def main():
    user_args = _get_user_args()
    changed_source_files, changed_test_files = get_changed_files(user_args.branch)

    ge_dependency_graph = create_dependency_graph(user_args.source)
    relevant_files = determine_relevant_source_files(
        ge_dependency_graph, changed_source_files, depth=user_args.depth
    )

    # TODO(cdkini): Parsing of conftest.py will need to eventually be added to this step to raise accuracy
    tests_dependency_graph = create_dependency_graph(user_args.tests)
    test_candidates = determine_files_to_test(
        tests_dependency_graph, relevant_files, changed_test_files
    )

    files_to_test = []
    for file in test_candidates:
        # Throw out files that are in our ignore list
        if any(file.startswith(path) for path in user_args.ignore):
            continue
        # Throw out files that aren't explicitly part of a filter (if supplied)
        if user_args.filter and not file.startswith(user_args.filter):
            continue
        files_to_test.append(file)

    if len(files_to_test) == 0:
        # Return code is important as it let's us know whether or not to pipe to pytest
        sys.exit(1)

    # If we successfully get here, the return code is 0 and the output files are tested
    print("\n".join(files_to_test))


if __name__ == "__main__":
    main()
