"""
Purpose: To ensure that no stray snippet opening/closing tags are present in our production docs

In short, this script creates a temporary Docusaurus build and utilizes grep to parse for stray tags.
"""

import shutil
import subprocess
import sys
import tempfile
from typing import List


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def run_docusaurus_build(target_dir: str) -> None:
    # https://docusaurus.io/docs/cli#docusaurus-build-sitedir
    subprocess.call(
        [
            "yarn",
            "build",
            "--out-dir",
            target_dir,
            "--no-minify",  # Aids with debugging errors
        ],
    )


def run_grep(target_dir: str) -> List[str]:
    try:
        res = subprocess.run(
            [
                "grep",
                "-C",  # Add surrounding context
                "3",
                "-Er",  # Enable regex and directory search
                r"<\/?snippet>",
                target_dir,
            ],
            capture_output=True,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        )
    return res.stdout.splitlines()


def main() -> None:
    check_dependencies("yarn", "grep")
    with tempfile.TemporaryDirectory() as tmp_dir:
        run_docusaurus_build(tmp_dir)
        grep_output = run_grep(tmp_dir)
        if grep_output:
            print("[ERROR] Found snippets in the docs build:")
            for line in grep_output:
                print(line)
            sys.exit(1)


if __name__ == "__main__":
    main()
