"""Prepare prior docs versions of GX for inclusion into the latest docs under the version dropdown.

There are changes to paths that need to be made to prior versions of docs.
"""
from __future__ import annotations

import glob
import pathlib
import re


def _docs_dir() -> pathlib.Path:
    """Base directory for docs (contains docusaurus folder)."""
    return pathlib.Path().absolute()


def change_paths_for_docs_file_references():
    """Change file= style references to use versioned_docs paths.

    This is used in v0.14 docs like v0.14.13 since we moved to using named
    snippets only for v0.15.50 and later.
    """
    path = _docs_dir() / "docusaurus/versioned_docs/version-0.14.13/"
    files = glob.glob(f"{path}/**/*.md", recursive=True)
    pattern = re.compile(r"((.*)(file *= *)((../)*))(.*)")
    path_to_insert = "versioned_code/version-0.14.13/"

    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = re.sub(pattern, rf"\1{path_to_insert}\6", contents)
            f.seek(0)
            f.truncate()
            f.write(contents)
        print(f"processed {file_path}")


def _paths_to_versioned_docs() -> list[pathlib.Path]:
    data_path = _docs_dir() / "docusaurus/versioned_docs"
    paths = [f for f in data_path.iterdir() if f.is_dir() and "0.14.13" not in str(f)]
    return paths


def _paths_to_versioned_code() -> list[pathlib.Path]:
    data_path = _docs_dir() / "docusaurus/versioned_code"
    paths = [f for f in data_path.iterdir() if f.is_dir() and "0.14.13" not in str(f)]
    return paths


def prepend_version_info_to_name_for_snippet_by_name_references():
    """Prepend version info e.g. name="snippet_name" -> name="version-0.15.50 snippet_name" """

    pattern = re.compile(r"((.*)(name *= *\"))(.*)")
    paths = _paths_to_versioned_docs() + _paths_to_versioned_code()

    for path in paths:
        version = path.name
        files = []
        for extension in (".md", ".mdx", ".py", ".yml", ".yaml"):
            files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
        for file_path in files:
            with open(file_path, "r+") as f:
                contents = f.read()
                contents = re.sub(pattern, rf"\1{version} \4", contents)
                f.seek(0)
                f.truncate()
                f.write(contents)
            print(f"processed {file_path}")


def prepend_version_info_to_name_for_href_absolute_links():
    """Prepend version info to absolute links: /docs/... becomes /docs/{version}/..."""

    href_pattern = re.compile(r"(?P<href>href=[\"\']/docs/)(?P<link>\S*[\"\'])")
    version_from_path_name_pattern = re.compile(
        r"(?P<version>\d{1,2}\.\d{1,2}\.\d{1,2})"
    )
    paths = _paths_to_versioned_docs() + _paths_to_versioned_code()

    for path in paths:
        version = path.name
        version_only = version_from_path_name_pattern.search(version).group(0)
        if not version_only:
            raise ValueError("Path does not contain a version number")

        files = []
        for extension in (".md", ".mdx"):
            files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
        for file_path in files:
            with open(file_path, "r+") as f:
                contents = f.read()
                # href="/docs/link" -> href="/docs/0.14.13/link"
                contents = re.sub(
                    href_pattern, rf"\g<href>{version_only}/\g<link>", contents
                )
                f.seek(0)
                f.truncate()
                f.write(contents)
            print(f"processed {file_path}")


if __name__ == "__main__":
    change_paths_for_docs_file_references()
    prepend_version_info_to_name_for_snippet_by_name_references()
    prepend_version_info_to_name_for_href_absolute_links()
