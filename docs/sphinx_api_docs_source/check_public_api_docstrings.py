"""Utilities to lint public API docstrings.

Public API docstrings are those marked with the @public_api decorator.

Typical usage example:

  main() method provided with typical usage.
"""
from __future__ import annotations

import datetime
import logging
import pathlib
import re
import subprocess
from dataclasses import dataclass
from typing import List, Set, Tuple

from .public_api_report import (
    CodeParser,
    Definition,
    FileContents,
    PublicAPIChecker,
    _default_code_absolute_paths,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True)
class DocstringError:
    """Error in a docstring with info to locate the error.

    Args:
        name: name of the class, method, function - empty if module
        filepath_relative_to_repo_root: location of error
        error: stripped string of error
        raw_error: raw string of error
        line_number: line number of error within file

    """

    name: str
    filepath_relative_to_repo_root: pathlib.Path
    error: str
    raw_error: str
    line_number: int

    def __str__(self):
        return self.raw_error


def parse_ruff_errors(raw_errors: List[str]) -> List[DocstringError]:
    """Parse raw string output of ruff to DocstringError."""

    docstring_errors: List[DocstringError] = []
    pattern = re.compile(r"^D\d{3}")
    for raw_error in raw_errors:
        if not raw_error:
            continue

        split_error = raw_error.split(":")
        if len(split_error) >= 4:
            path = split_error[0]
            line_number = int(split_error[1])
            error_code_and_error = split_error[3].strip()

            error_code_match = re.search(pattern, error_code_and_error)
            if error_code_match:
                error_code = error_code_match.group()
            else:
                error_code = ""
            name = re.sub(pattern, "", error_code_and_error).strip()

            docstring_errors.append(
                DocstringError(
                    name=name,
                    filepath_relative_to_repo_root=_repo_relative_filepath(
                        pathlib.Path(path)
                    ),
                    error=error_code,
                    raw_error=raw_error,
                    line_number=line_number,
                )
            )

    return docstring_errors


def _repo_root() -> pathlib.Path:
    repo_root_path = pathlib.Path(__file__).parents[2]
    return repo_root_path


def _repo_relative_filepath(filepath: pathlib.Path) -> pathlib.Path:
    if filepath.is_absolute():
        return filepath.relative_to(_repo_root())
    else:
        return filepath


def run_ruff(paths: List[pathlib.Path]) -> List[str]:
    """Run ruff to identify issues with docstrings."""

    _log_with_timestamp("Running ruff")
    # --select D option to enable pydocstyle errors in ruff
    # https://github.com/charliermarsh/ruff#pydocstyle-d
    cmds = ["ruff", "--select", "D"] + [str(p) for p in paths]
    raw_results: subprocess.CompletedProcess = subprocess.run(
        cmds,
        capture_output=True,
        text=True,
    )

    # Check to make sure `ruff` actually ran
    err: str = raw_results.stderr
    if err:
        raise ValueError(err)

    _log_with_timestamp("Finished running ruff")
    return raw_results.stdout.split("\n")


def _log_with_timestamp(content: str) -> None:
    """Log content with timestamp appended."""
    timestamp = datetime.datetime.now()
    timestamp_str = timestamp.strftime("%H:%M:%S")
    logger.debug(f"{content} Timestamp: {timestamp_str}")


def _get_docstring_errors(
    select_paths: list[pathlib.Path] | None = None,
) -> List[DocstringError]:
    """Get all docstring errors."""
    if select_paths:
        filepaths_containing_public_api_entities = [p.resolve() for p in select_paths]
    else:
        filepaths_containing_public_api_entities = [
            pathlib.Path(d.filepath).resolve() for d in get_public_api_definitions()
        ]

    ruff_raw_errors = run_ruff(paths=filepaths_containing_public_api_entities)
    parsed_ruff_errors = parse_ruff_errors(ruff_raw_errors)
    return parsed_ruff_errors


def get_public_api_definitions() -> Set[Definition]:
    """Get entities marked with the @public_api decorator."""
    code_file_contents = FileContents.create_from_local_files(
        _default_code_absolute_paths()
    )

    code_parser = CodeParser(file_contents=code_file_contents)

    public_api_checker = PublicAPIChecker(code_parser=code_parser)

    return public_api_checker.get_all_public_api_definitions()


def get_public_api_module_level_function_definitions() -> Set[Definition]:
    """Get module level functions marked with the @public_api decorator."""
    code_file_contents = FileContents.create_from_local_files(
        _default_code_absolute_paths()
    )

    code_parser = CodeParser(file_contents=code_file_contents)

    public_api_checker = PublicAPIChecker(code_parser=code_parser)

    return public_api_checker.get_module_level_function_public_api_definitions()


def _public_api_docstring_errors(
    select_paths: list[pathlib.Path] | None = None,
) -> Set[DocstringError]:
    """Get all docstring errors for entities marked with the @public_api decorator."""

    _log_with_timestamp("Getting public api definitions.")
    public_api_definitions = get_public_api_definitions()
    public_api_definition_tuples: Set[Tuple[str, str]] = {
        (str(_repo_relative_filepath(d.filepath)), d.name)
        for d in public_api_definitions
    }

    _log_with_timestamp("Getting docstring errors.")
    public_api_docstring_errors: List[DocstringError] = []
    docstring_errors = _get_docstring_errors(select_paths=select_paths)

    _log_with_timestamp("Getting docstring errors applicable to public api.")
    for docstring_error in docstring_errors:
        docstring_error_tuple: Tuple[str, str] = (
            str(docstring_error.filepath_relative_to_repo_root),
            docstring_error.name,
        )
        if docstring_error_tuple in public_api_definition_tuples:
            public_api_docstring_errors.append(docstring_error)

    return set(public_api_docstring_errors)


def main(
    select_paths: list[pathlib.Path] | None = None,
):
    logger.info(
        "Generating list of public API docstring errors. This may take a minute."
    )
    errors = _public_api_docstring_errors(select_paths=select_paths)

    _log_with_timestamp("Finished evaluating public docstrings.")

    if not errors:
        logger.info("There are no public API docstring errors.")
    else:
        errors_str = (
            f"\n----- {len(errors)} errors found -----\n"
            + "\n".join([e.raw_error for e in errors])
            + "\n----- END -----\n"
        )
        logger.error(errors_str)

    assert len(errors) == 0, f"There are {len(errors)} docstring errors to address."


if __name__ == "__main__":
    main()
