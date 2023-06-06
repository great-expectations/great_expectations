"""
Purpose: To ensure that no docs snippets use the file and line number convention,
only the named snippets convention.
"""
import pathlib
import re
import shutil
import subprocess
import sys
from typing import List

ITEMS_IGNORED_FROM_LINE_NUMBER_SNIPPET_CHECKER = {
    "docs/prepare_to_build_docs.sh",
    "docs/prepare_prior_versions.py",
}
EXCLUDED_FILENAMES_PATTERN = re.compile(r"node_modules", re.IGNORECASE)


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def run_grep(target_dir: pathlib.Path) -> List[str]:
    try:
        res = subprocess.run(
            [
                "grep",
                "--recursive",
                "--files-with-matches",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"file=",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e
    return res.stdout.splitlines()


def main() -> None:
    check_dependencies("grep")
    project_root = pathlib.Path(__file__).parent.parent.parent
    docs_dir = project_root / "docs"
    assert docs_dir.exists()
    grep_output = run_grep(docs_dir)
    grep_output = list(
        filter(
            lambda filename: EXCLUDED_FILENAMES_PATTERN.match(filename),
            grep_output,
        )
    )
    excluded_documents = {
        project_root / file_path
        for file_path in ITEMS_IGNORED_FROM_LINE_NUMBER_SNIPPET_CHECKER
    }
    new_violations = set(grep_output).difference(excluded_documents)
    if new_violations:
        print(
            f"[ERROR] Found {len(new_violations)} snippets using file and line number syntax.  Please use named snippet syntax:"
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
