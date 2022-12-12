"""
Purpose: To ensure that no stray snippet opening/closing tags are present in our production docs

In short, this script creates a temporary Docusaurus build and utilizes grep to parse for stray tags.
"""

import shutil
import subprocess
import sys
from typing import List


def check_dependencies(*deps: str) -> None:
    for dep in deps:
        if not shutil.which(dep):
            raise Exception(f"Must have `{dep}` installed in PATH to run {__file__}")


def run_docusaurus_build() -> None:
    subprocess.call(
        [
            "yarn",
            "build",
        ],
    )


def run_grep(target_dir: str) -> List[str]:
    try:
        out = subprocess.check_output(
            [
                "grep",
                "-r",
                "snippet>",
                target_dir,
            ],
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        )
    return out.splitlines()


def main() -> None:
    check_dependencies("yarn", "grep")
    run_docusaurus_build()
    grep_output = run_grep("build")
    if grep_output:
        print("[ERROR] Found snippets in the docs build:")
        for line in grep_output:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
