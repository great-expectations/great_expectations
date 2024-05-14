"""
Generate a new package `great_expectations_v0` from the existing `great_expectations` package.
This is used to create a mirror of the existing package with the name `great_expectations_v0` so users can
install the old version of the package alongside a v1 installation.

"""

from __future__ import annotations

import pathlib
import re
import shutil
from typing import Final, Pattern

IMPORT_PATTERN: Final[Pattern] = re.compile(r"import great_expectations")
FROM_PATTERN: Final[Pattern] = re.compile(r"from great_expectations")
MODULE_STRING: Final[Pattern] = re.compile(r"\"great_expectations\.")

CORE_DIRECTORY: Final[pathlib.Path] = pathlib.Path("great_expectations").resolve(
    strict=True
)
NEW_PACKAGE_DIR: Final[pathlib.Path] = CORE_DIRECTORY.with_name("great_expectations_v0")

DIST_DIR: Final[pathlib.Path] = pathlib.Path("dist")

UNTOUCED_FILES: int = 0
UPDATED_FILES: int = 0


def replace(file_path: pathlib.Path) -> None:
    global UNTOUCED_FILES, UPDATED_FILES  # noqa: PLW0603
    with open(file_path) as file:
        contents = file.read()
        new_contents = IMPORT_PATTERN.sub("import great_expectations_v0", contents)
        new_contents = FROM_PATTERN.sub("from great_expectations_v0", new_contents)
        new_contents = MODULE_STRING.sub('"great_expectations_v0.', new_contents)
        if contents == new_contents:
            UNTOUCED_FILES += 1
            return
    print(f"{file_path.relative_to(CORE_DIRECTORY.parent)} updated")
    with open(file_path, "w") as file:
        file.write(new_contents)
        UPDATED_FILES += 1


def iterate_files(file_dir: pathlib.Path) -> None:
    print("📁 Creating new package with updated `great_expectations_v0` references\n")
    for file in file_dir.iterdir():
        if file.is_dir():
            iterate_files(file)
        elif file.is_file() and file.suffix in (".py", ".pyi"):
            replace(file)


def cleanup_dist_dir() -> None:
    """Delete the dist (distribution) directory if it exists."""
    if DIST_DIR.exists():
        print(f"🗑️  Removing {DIST_DIR}\n")
        shutil.rmtree(DIST_DIR, ignore_errors=True)


if __name__ == "__main__":
    cleanup_dist_dir()
    if NEW_PACKAGE_DIR.exists():
        print(f"❌ {NEW_PACKAGE_DIR} already exists. Removing...")
        NEW_PACKAGE_DIR.rmdir()
        print(f"✅ Removed existing {NEW_PACKAGE_DIR}\n")

    iterate_files(CORE_DIRECTORY)
    print(f"\n Untouched files: {UNTOUCED_FILES}\n Updated files: {UPDATED_FILES}\n")

    new_core_dir = CORE_DIRECTORY.rename(NEW_PACKAGE_DIR.name)
    assert (
        new_core_dir.exists() and not CORE_DIRECTORY.exists()
    ), "Directory rename failed"
    print(f"✅ Directory renamed to {new_core_dir}\n")
