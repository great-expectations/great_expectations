import logging
import pathlib
import re
import subprocess
from dataclasses import dataclass
from typing import List, Set, Tuple

from scripts.public_api_report import (
    FileContents,
    _default_code_absolute_paths,
    CodeParser,
    PublicAPIChecker,
    Definition,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class DocstringError:
    """Error in a docstring with info to locate the error.

    Args:
        name: name of the class, method, function - empty if module
        filepath_relative_to_repo_root: location of error
        error: stripped string of error
        raw_error_first_line: raw string of error first line
        raw_error_second_line: raw string of error second line
        line_number: line number of error within file

    """

    name: str
    filepath_relative_to_repo_root: pathlib.Path
    error: str
    raw_error_first_line: str
    raw_error_second_line: str
    line_number: int


def parse_to_docstring_errors(raw_errors: List[str]) -> List[DocstringError]:
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
                    raw_error_first_line=raw_error_path,
                    raw_error_second_line=raw_error_error,
                    line_number=_parse_line_number(raw_error_path),
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


def run_pydocstyle(directory: pathlib.Path) -> List[str]:
    """Run pydocstyle to identify issues with docstrings."""

    raw_results: subprocess.CompletedProcess = subprocess.run(
        [
            "pydocstyle",
            directory,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    # Check to make sure `pydocstyle` actually ran
    err: str = raw_results.stderr
    if err:
        raise ValueError(err)

    return raw_results.stdout.split("\n")


def _get_docstring_errors() -> List[DocstringError]:
    """Get all docstring errors."""
    repo_root = pathlib.Path(__file__).parent.parent
    pydocstyle_dir = repo_root / "great_expectations/"
    pydocstyle_dir_abs = pydocstyle_dir.absolute()
    raw_errors = run_pydocstyle(pydocstyle_dir_abs)

    return parse_to_docstring_errors(raw_errors=raw_errors)


def _get_public_api_definitions() -> Set[Definition]:
    """Get entities marked with the @public_api decorator."""
    code_file_contents = FileContents.create_from_local_files(
        _default_code_absolute_paths()
    )

    code_parser = CodeParser(file_contents=code_file_contents)

    public_api_checker = PublicAPIChecker(code_parser=code_parser)

    return public_api_checker.get_all_public_api_definitions()


def _public_api_docstring_errors() -> Set[DocstringError]:
    """Get all docstring errors for entities marked with the @public_api decorator."""

    public_api_definitions = _get_public_api_definitions()
    public_api_definition_tuples: Set[Tuple[str, str]] = {
        (str(_repo_relative_filepath(d.filepath)), d.name)
        for d in public_api_definitions
    }

    public_api_docstring_errors: List[DocstringError] = []
    for docstring_error in _get_docstring_errors():
        docstring_error_tuple: Tuple[str, str] = (
            str(docstring_error.filepath_relative_to_repo_root),
            docstring_error.name,
        )
        if docstring_error_tuple in public_api_definition_tuples:
            public_api_docstring_errors.append(docstring_error)

    return set(public_api_docstring_errors)


def main():
    logger.info("Generating list of public API docstring errors.")
    errors = _public_api_docstring_errors()

    for error in errors:
        logger.error(error)


if __name__ == "__main__":
    main()
