"""
Purpose: To ensure that no docs snippets use the file and line number convention,
only the named snippets convention.
"""
import pathlib
import shutil
import subprocess
import sys
from typing import List


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def run_grep(target_dir: pathlib.Path) -> List[str]:
    try:
        res = subprocess.run(
            [
                "grep",
                "-rnw",
                "-e",
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
    docs_dir = pathlib.Path(__file__).parent.parent.parent / "docs"
    assert docs_dir.exists()
    grep_output = run_grep(docs_dir)
    if grep_output:
        print(
            f"[ERROR] Found {len(grep_output)} snippets using file and line number syntax, please use named snippet syntax:"
        )
        for line in grep_output:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
