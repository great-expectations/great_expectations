"""
Need to replace any instance of

`import great_expectations` or `from great_expectations` with
`import great_expectations_v0` or `from great_expectations_v0` respectively.

"""

from __future__ import annotations

import pathlib
import re
from typing import Final, Pattern

IMPORT_PATTERN: Final[Pattern] = re.compile(r"import great_expectations")
FROM_PATTERN: Final[Pattern] = re.compile(r"from great_expectations")

CORE_DIRECTORY: Final[pathlib.Path] = pathlib.Path("great_expectations").resolve(
    strict=True
)

UNTOUCED_FILES: Final[list[pathlib.Path]] = []


def replace(file_path: pathlib.Path) -> None:
    with open(file_path, "r") as file:
        contents = file.read()
        new_contents = IMPORT_PATTERN.sub("import great_expectations_v0", contents)
        new_contents = FROM_PATTERN.sub("from great_expectations_v0", new_contents)
        if contents == new_contents:
            UNTOUCED_FILES.append(file_path)
            return
    print(f"{file_path.relative_to(CORE_DIRECTORY.parent)} updated")
    with open(file_path, "w") as file:
        file.write(new_contents)


def iterate_files(file_dir: pathlib.Path) -> None:
    for file in file_dir.iterdir():
        if file.is_dir():
            iterate_files(file)
        elif file.is_file() and file.suffix in (".py", ".pyi"):
            replace(file)


if __name__ == "__main__":
    iterate_files(CORE_DIRECTORY)
    print(f"\n Untouched files: {len(UNTOUCED_FILES)}")
