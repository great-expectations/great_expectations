"""Prepare prior docs versions of GX for inclusion into the latest docs under the version dropdown. 

There are changes to paths that need to be made to prior versions of docs.
"""
import glob
import pathlib
import re


def _repo_root() -> pathlib.Path:
    """Get the path from the repo root for this file."""
    repo_root_path = pathlib.Path(__file__).parents[1]
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

    # TODO: Make this idempotent

    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = re.sub(pattern, rf"\1{path_to_insert}\6", contents)
            f.seek(0)
            f.truncate()
            f.write(contents)
        print(f"processed {file_path}")


if __name__ == "__main__":
    change_paths_for_docs_file_references()
