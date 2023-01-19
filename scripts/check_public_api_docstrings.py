"""Utilities to lint public API docstrings.

Public API docstrings are those marked with the @public_api decorator.

Typical usage example:

  main() method provided with typical usage.
"""
import logging
import pathlib
import re
import subprocess
from dataclasses import dataclass
from typing import List, Set, Tuple

from scripts.public_api_report import (
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


def parse_pydocstyle_errors(raw_errors: List[str]) -> List[DocstringError]:
    """Parse raw string output of pydocstyle to DocstringError."""

    docstring_errors: List[DocstringError] = []
    for idx, raw_error in enumerate(raw_errors):
        if not raw_error:
            break
        if raw_error.startswith("/"):
            raw_error_path = raw_error
            raw_error_error = raw_errors[idx + 1]

            docstring_errors.append(
                DocstringError(
                    name=_parse_name(raw_error_path=raw_error_path),
                    filepath_relative_to_repo_root=_repo_relative_filepath(
                        pathlib.Path(_parse_filepath(raw_error_path=raw_error_path))
                    ),
                    error=raw_error_error.strip(),
                    raw_error="\n".join([raw_error_path, raw_error_error]),
                    line_number=_parse_line_number(raw_error_path),
                )
            )

    return docstring_errors


def parse_darglint_errors(raw_errors: List[str]) -> List[DocstringError]:
    """Parse raw string output of darglint to DocstringError."""

    docstring_errors: List[DocstringError] = []
    for raw_error in raw_errors:
        if not raw_error:
            continue

        split_error = raw_error.split(":")
        path = split_error[0]
        name = split_error[1]
        line_number = int(split_error[2])
        error_code = split_error[3]

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
    return pathlib.Path(__file__).parent.parent


def _repo_relative_filepath(filepath: pathlib.Path) -> pathlib.Path:
    if filepath.is_absolute():
        return filepath.relative_to(_repo_root())
    else:
        return filepath


def _parse_filepath(raw_error_path: str) -> str:
    """Parse the filepath from the first part of an error pair."""
    match = re.search(r"(?:/[^/]+)+?/\w+\.\w+", raw_error_path)
    if match:
        filepath: str = match.group(0)
    else:
        raise ValueError(f"No filepath found in error: {raw_error_path}.")
    return filepath


def _parse_line_number(raw_error_path: str) -> int:
    """Parse the line number from the first part of an error pair."""
    raw_line_num = raw_error_path.split(":")[1]
    match = re.search(r"\d+", raw_line_num)
    if match:
        line_num = int(match.group(0))
    else:
        raise ValueError(f"No line number found in error: {raw_error_path}.")
    return line_num


def _parse_name(raw_error_path: str) -> str:
    match = re.search(r"`(.*?)`", raw_error_path)
    name = ""
    if match:
        name = match.group(0).replace("`", "")
    return name


def run_pydocstyle(paths: List[pathlib.Path]) -> List[str]:
    """Run pydocstyle to identify issues with docstrings."""

    logger.debug("Running pydocstyle")
    cmds = ["pydocstyle"] + [str(p) for p in paths]
    raw_results: subprocess.CompletedProcess = subprocess.run(
        cmds,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    # Check to make sure `pydocstyle` actually ran
    err: str = raw_results.stderr
    if err:
        raise ValueError(err)

    logger.debug("Finished running pydocstyle")
    return raw_results.stdout.split("\n")


def _get_docstring_errors() -> List[DocstringError]:
    """Get all docstring errors."""

    filepaths_containing_public_api_entities = [
        pathlib.Path(d.filepath).resolve() for d in get_public_api_definitions()
    ]
    pydocstyle_raw_errors = run_pydocstyle(
        paths=filepaths_containing_public_api_entities
    )
    darglint_raw_errors = run_darglint(paths=filepaths_containing_public_api_entities)

    parsed_pydocstyle_errors = parse_pydocstyle_errors(raw_errors=pydocstyle_raw_errors)
    parsed_darglint_errors = parse_darglint_errors(raw_errors=darglint_raw_errors)

    return parsed_pydocstyle_errors + parsed_darglint_errors


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


def _public_api_docstring_errors() -> Set[DocstringError]:
    """Get all docstring errors for entities marked with the @public_api decorator."""

    logger.debug("Getting public api definitions.")
    public_api_definitions = get_public_api_definitions()
    public_api_definition_tuples: Set[Tuple[str, str]] = {
        (str(_repo_relative_filepath(d.filepath)), d.name)
        for d in public_api_definitions
    }

    logger.debug("Getting docstring errors.")
    public_api_docstring_errors: List[DocstringError] = []
    docstring_errors = _get_docstring_errors()

    logger.debug("Getting docstring errors applicable to public api.")
    for docstring_error in docstring_errors:
        docstring_error_tuple: Tuple[str, str] = (
            str(docstring_error.filepath_relative_to_repo_root),
            docstring_error.name,
        )
        if docstring_error_tuple in public_api_definition_tuples:
            public_api_docstring_errors.append(docstring_error)

    return set(public_api_docstring_errors)


def run_darglint(paths: List[pathlib.Path]) -> List[str]:
    """Run darglint to identify issues with docstrings."""

    logger.debug("Running darglint")
    cmds = ["darglint"] + [str(p) for p in paths]
    raw_results: subprocess.CompletedProcess = subprocess.run(
        cmds,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    # Check to make sure `darglint` actually ran
    err: str = raw_results.stderr
    if err:
        raise ValueError(err)

    logger.debug("Finished running darglint")

    return raw_results.stdout.split("\n")


def main():
    logger.info(
        "Generating list of public API docstring errors. This may take a few minutes."
    )
    errors = _public_api_docstring_errors()

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
