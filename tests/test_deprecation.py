import glob
import re
from typing import List, Tuple

import versioneer


def test_deprecation_warnings_are_accompanied_by_appropriate_comment():
    pattern: re.Pattern = re.compile(r"deprecated-v(\d*)\.(\d*)\.(\d*)")
    files: List[str] = glob.glob("great_expectations/**/*.py", recursive=True)

    # Filter out parts of the codebase that aren't written by the GE team
    files = list(filter(lambda f: "marshmallow__shade" not in f, files))

    for file in files:
        with open(file) as f:
            contents = f.read()

        matches: List[Tuple[str, str, str]] = pattern.findall(contents)
        warning_count: int = contents.count("DeprecationWarning")
        assert (
            len(matches) == warning_count
        ), f"Either a 'deprecated-v...' comment or 'DeprecationWarning' call is missing from {file}"


def test_deprecation_warnings_have_been_removed_after_two_minor_versions():
    """
    What does this test do and why?

    To ensure that we're appropriately deprecating, we want to test that we're fully
    removing warnings (and the code they correspond to) after two minor versions have passed.
    """
    current_version: str = versioneer.get_version()
    current_minor_version: int = int(current_version.split(".")[1])

    pattern: re.Pattern = re.compile(r"deprecated-v(\d*)\.(\d*)\.(\d*)")
    files: List[str] = glob.glob("great_expectations/**/*.py", recursive=True)

    unneeded_deprecation_warnings: List[Tuple[str, str]] = []
    for file in files:
        with open(file) as f:
            contents = f.read()

        matches: List[Tuple[str, str, str]] = pattern.findall(contents)
        for match in matches:
            minor_version: int = int(match[1])
            if current_minor_version - minor_version > 2:
                unneeded_deprecation_warning: Tuple[str, str] = (
                    file,
                    ".".join(m for m in match),
                )
                unneeded_deprecation_warnings.append(unneeded_deprecation_warning)

    if unneeded_deprecation_warnings:
        print(
            "\nThe following deprecation warnings must be cleared per the code style guide:"
        )
        for file, version in unneeded_deprecation_warnings:
            print(f"{file} - v{version}")

    # Chetan - 20220315 - This should be 0 once we've cleared deprecation warnings in v0.16.0.
    assert len(unneeded_deprecation_warnings) == 29
