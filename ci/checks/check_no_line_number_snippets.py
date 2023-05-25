"""
Purpose: To ensure that no docs snippets use the file and line number convention,
only the named snippets convention.
"""
import pathlib
import shutil
import subprocess
import sys
from typing import List

ITEMS_IGNORED_FROM_LINE_NUMBER_SNIPPET_CHECKER = {
    "docs/prepare_prior_versions.py",
    "docs/prepare_to_build_docs.sh",
}


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def run_grep(target_dir: pathlib.Path) -> List[str]:
    try:
        res_positive = subprocess.run(
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
        res_negative = subprocess.run(
            [
                "grep",
                "--recursive",
                "--files-with-matches",
                "--ignore-case",
                "--regexp",
                r"node_modules",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res = list(
            set(res_positive.stdout.splitlines()).difference(
                set(res_negative.stdout.splitlines())
            )
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e
    return res


def main() -> None:
    check_dependencies("grep")
    docs_dir = pathlib.Path(__file__).parent.parent.parent / "docs"
    assert docs_dir.exists()
    grep_output = run_grep(docs_dir)
    new_violations = set(grep_output).difference(
        ITEMS_IGNORED_FROM_LINE_NUMBER_SNIPPET_CHECKER
    )
    if new_violations:
        print(
            f"[ERROR] Found {len(new_violations)} snippets using file and line number syntax.  Please use named snippet syntax:"
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
