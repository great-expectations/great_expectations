import argparse
import subprocess
import sys
from typing import List, Tuple

from graph import (
    determine_relevant_source_files,
    determine_test_candidates,
    determine_tests_to_run,
)
from parse import get_dependency_graphs


def get_changed_files(
    branch: str, source_path: str, tests_path: str
) -> Tuple[List[str], List[str]]:
    """
    Standard git diff against a given branch to determine list of changed files.

    Ensure that any changed tests are picked up and run (regardless of the file changes in 'great_expectations/')
    """
    process = subprocess.run(
        ["git", "diff", "HEAD", branch, "--name-only"], stdout=subprocess.PIPE
    )
    files = [f.decode("utf-8") for f in process.stdout.splitlines()]
    source_files = get_changed_source_files(files, source_path)
    test_files = get_changed_test_files(files, tests_path)
    return source_files, test_files


def get_changed_source_files(files: List[str], source_path: str) -> List[str]:
    valid_src = lambda f: f.startswith(source_path) and f.endswith(".py")
    return [f for f in files if valid_src(f)]


def get_changed_test_files(files: List[str], tests_path: str) -> List[str]:
    valid_test = (
        lambda f: f.startswith(tests_path)
        and f.endswith(".py")
        and f.split("/")[-1].startswith("test_")
    )
    return [f for f in files if valid_test(f)]


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
    # Parse user args (or supply defaults if omitted)
    user_args = _get_user_args()

    # Use a git diff to check which files have changed
    changed_source_files, changed_test_files = get_changed_files(
        user_args.branch, user_args.source, user_args.tests
    )

    # Generate dependency graphs through AST parsing
    source_dependency_graph, tests_dependency_graph = get_dependency_graphs(
        user_args.source, user_args.tests
    )

    # Perform a graph traversal to determine relevant source files from our diffed files
    relevant_files = determine_relevant_source_files(
        source_dependency_graph, changed_source_files, user_args.depth
    )

    # Perform an additional graph traversal to select tests to run (based on relevant source files)
    test_candidates = determine_test_candidates(
        tests_dependency_graph, relevant_files, changed_test_files
    )

    # Filter test candidates based on user-provided args
    files_to_test = determine_tests_to_run(
        test_candidates, user_args.ignore, user_args.filter
    )

    # Return code is important as it let's us know whether or not to pipe to pytest
    if len(files_to_test) == 0:
        sys.exit(1)

    # If we successfully get here, the return code is 0 and the output files are tested
    print("\n".join(files_to_test))


if __name__ == "__main__":
    main()
