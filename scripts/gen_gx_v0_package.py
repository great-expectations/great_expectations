"""
Need to replace any instance of

`import great_expectations` or `from great_expectations` with
`import great_expectations_v0` or `from great_expectations_v0` respectively.

"""

import pathlib
import re
from typing import Final, Pattern

IMPORT_PATTERN: Final[Pattern] = re.compile(r"import great_expectations")
FROM_PATTERN: Final[Pattern] = re.compile(r"from great_expectations")

CORE_DIRECTORY: Final[pathlib.Path] = pathlib.Path("great_expectations").resolve(
    strict=True
)


def replace(file_path: pathlib.Path) -> None:
    with open(file_path, "r") as file:
        contents = file.read()
        new_contents = IMPORT_PATTERN.sub("import great_expectations_v0", contents)
        new_contents = FROM_PATTERN.sub("from great_expectations_v0", new_contents)
        if contents == new_contents:
            return
    print(f"{file_path} updated")
    with open(file_path, "w") as file:
        file.write(new_contents)


def iterate_files(file_dir: pathlib.Path) -> None:
    for file in file_dir.iterdir():
        if file.is_dir():
            iterate_files(file)
        elif file.is_file() and file.suffix in (".py", ".pyi"):
            print(f"Replacing in file: {file}")
            replace(file)


if __name__ == "__main__":
    iterate_files(CORE_DIRECTORY)
