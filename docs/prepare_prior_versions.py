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
    """Change file= style references, mainly in v0.14 docs like v0.14.13"""
    path = _repo_root() / "docs/docusaurus/versioned_docs/version-0.14.13/"
    files = glob.glob(f"{path}/**/*.md", recursive=True)
    # pattern = re.compile()
    for file_path in files:
        # TODO: Run on all files not just test one
        if "how_to_use_great_expectations_in_databricks_copy.md" in str(file_path):
            with open(file_path, "r+") as f:
                contents = f.read()
                # TODO: Use regex here instead
                contents = contents.replace("file=", "test_change_paths=")
                f.seek(0)
                f.truncate()
                f.write(contents)
            print(f"processed {file_path}")


if __name__ == "__main__":
    change_paths_for_docs_file_references()
