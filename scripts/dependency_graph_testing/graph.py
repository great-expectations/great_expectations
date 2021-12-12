import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


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


def determine_test_candidates(
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


def determine_tests_to_run(
    test_candidates: List[str], ignore_paths: List[str], filter_: Optional[str]
) -> List[str]:
    """
    Applies a number of filters to a list of test candidates in order to determine
    the final output to be fed to pytest.
    """
    files_to_test = []
    for file in test_candidates:
        # Throw out files that are in our ignore list
        if any(file.startswith(path) for path in ignore_paths):
            logger.info(f"Skipped '{file}' due to --ignore flag")
            continue
        # Throw out files that aren't explicitly part of a filter (if supplied)
        if filter_ and not file.startswith(filter_):
            logger.info(f"Skipped '{file}' due to --filter flag")
            continue
        files_to_test.append(file)
    return files_to_test
