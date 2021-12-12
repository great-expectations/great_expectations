import sys

from graph import (
    determine_relevant_source_files,
    determine_test_candidates,
    determine_tests_to_run,
)
from parse import get_dependency_graphs
from utils import get_changed_files, get_user_args


def main():
    # Parse user args (or supply defaults if omitted)
    user_args = get_user_args()

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
