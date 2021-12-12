import argparse
import subprocess
from typing import List, Tuple


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
    source_files = _get_changed_source_files(files, source_path)
    test_files = _get_changed_test_files(files, tests_path)
    return source_files, test_files


def _get_changed_source_files(files: List[str], source_path: str) -> List[str]:
    valid_src = lambda f: f.startswith(source_path) and f.endswith(".py")
    return [f for f in files if valid_src(f)]


def _get_changed_test_files(files: List[str], tests_path: str) -> List[str]:
    valid_test = (
        lambda f: f.startswith(tests_path)
        and f.endswith(".py")
        and f.split("/")[-1].startswith("test_")
    )
    return [f for f in files if valid_test(f)]


def get_user_args() -> argparse.Namespace:
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
