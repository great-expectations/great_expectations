import glob
import re
from typing import List, Pattern, Tuple, cast

import pytest
from packaging import version

from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def regex_for_deprecation_comments() -> Pattern:
    pattern: Pattern = re.compile(r"deprecated-v(.+)")
    return pattern


@pytest.fixture
def files_with_deprecation_warnings() -> List[str]:
    files: List[str] = glob.glob("great_expectations/**/*.py", recursive=True)
    return files


def test_deprecation_warnings_are_accompanied_by_appropriate_comment(
    regex_for_deprecation_comments: Pattern,
    files_with_deprecation_warnings: List[str],
):
    """
    What does this test do and why?

    For every invokation of 'DeprecationWarning', there must be a corresponding
    comment with the following format: 'deprecated-v<MAJOR>.<MINOR>.<PATCH>'.

    This test is meant to capture instances where one or the other is missing.
    """
    for file in files_with_deprecation_warnings:
        with open(file) as f:
            contents = f.read()

        matches: List[str] = regex_for_deprecation_comments.findall(contents)
        warning_count: int = contents.count("DeprecationWarning")
        assert (
            len(matches) == warning_count
        ), f"Either a 'deprecated-v...' comment or 'DeprecationWarning' call is missing from {file}"


def test_deprecation_warnings_have_been_removed_after_two_minor_versions(
    regex_for_deprecation_comments: Pattern,
    files_with_deprecation_warnings: List[str],
):
    """
    What does this test do and why?

    To ensure that we're appropriately deprecating, we want to test that we're fully
    removing warnings (and the code they correspond to) after two minor versions have passed.
    """
    deployment_version_path: str = file_relative_path(
        __file__, "../great_expectations/deployment_version"
    )
    current_version: str
    with open(deployment_version_path) as f:
        current_version = f.read().strip()

    current_parsed_version: version.Version = cast(
        version.Version, version.parse(current_version)
    )
    current_minor_version: int = current_parsed_version.minor

    unneeded_deprecation_warnings: List[Tuple[str, str]] = []
    for file in files_with_deprecation_warnings:
        with open(file) as f:
            contents = f.read()

        matches: List[str] = regex_for_deprecation_comments.findall(contents)
        for match in matches:
            parsed_version: version.Version = cast(
                version.Version, version.parse(match)
            )
            minor_version: int = parsed_version.minor
            if current_minor_version - minor_version > 2:
                unneeded_deprecation_warning: Tuple[str, str] = (file, match)
                unneeded_deprecation_warnings.append(unneeded_deprecation_warning)

    if unneeded_deprecation_warnings:
        print(
            "\nThe following deprecation warnings must be cleared per the code style guide:"
        )
        for file, version_ in unneeded_deprecation_warnings:
            print(f"{file} - v{version_}")

    # Chetan - 20220316 - Note that this will break as soon as v0.16.0 lands;
    # this should be cleaned up and made 0 at that point.
    assert len(unneeded_deprecation_warnings) <= 35
