"""Prepare prior docs versions of GX for inclusion into the latest docs under the version dropdown.

There are changes to paths that need to be made to prior versions of docs.
"""
from __future__ import annotations

import glob
import pathlib
import re


def _repo_root() -> pathlib.Path:
    """Get the path from the repo root for this file."""
    print("print(__file__)")
    print(__file__)
    print("print(pathlib.Path(__file__).parents)")
    print(pathlib.Path(__file__).parents)
    print("print(list(pathlib.Path(__file__).parents))")
    print(list(pathlib.Path(__file__).parents))

    all_dirs = [p for p in pathlib.Path(__file__).parent.glob("**/*") if p.is_dir()]
    print("all_dirs")
    print(all_dirs)
    print("all_dirs")

    repo_root_path = list(pathlib.Path(__file__).parents)[1]
    print(repo_root_path)
    return repo_root_path


def change_paths_for_docs_file_references():
    """Change file= style references to use versioned_docs.

    This is used in v0.14 docs like v0.14.13 since we moved to using named
    snippets only for v0.15.50 and later.
    """
    path = _repo_root() / "docs/docusaurus/versioned_docs/version-0.14.13/"
    files = glob.glob(f"{path}/**/*.md", recursive=True)
    pattern = re.compile(r"((.*)(file=)((../)*))(.*)")
    path_to_insert = "versioned_code/version-0.14.13/"

    # TODO: Make this idempotent for development

    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = re.sub(pattern, rf"\1{path_to_insert}\6", contents)
            f.seek(0)
            f.truncate()
            f.write(contents)
        print(f"processed {file_path}")


def _paths_to_versioned_docs() -> list[pathlib.Path]:
    data_path = _repo_root() / "docs/docusaurus/versioned_docs"
    return [f for f in data_path.iterdir() if f.is_dir() and "0.14.13" not in str(f)]


def _paths_to_versioned_code() -> list[pathlib.Path]:
    data_path = _repo_root() / "docs/docusaurus/versioned_code"
    return [f for f in data_path.iterdir() if f.is_dir() and "0.14.13" not in str(f)]


def prepend_version_info_to_name_for_snippet_by_name_references_markdown():
    """Prepend version info e.g. name="snippet_name" -> name="version-0.15.50 snippet_name" """

    pattern = re.compile(r"((.*)(name=\"))(.*)")
    paths = _paths_to_versioned_docs() + _paths_to_versioned_code()

    # TODO: Make this idempotent for development

    for path in paths:
        version = path.name
        files = []
        for extension in (".md", ".mdx", ".py"):
            files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
        for file_path in files:
            with open(file_path, "r+") as f:
                contents = f.read()
                contents = re.sub(pattern, rf"\1{version}\4", contents)
                f.seek(0)
                f.truncate()
                f.write(contents)
            print(f"processed {file_path}")


if __name__ == "__main__":
    # change_paths_for_docs_file_references()
    # prepend_version_info_to_name_for_snippet_by_name_references_markdown()
    _repo_root()
