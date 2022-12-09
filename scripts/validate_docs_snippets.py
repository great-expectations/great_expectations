import subprocess
import sys
import tempfile
from typing import List


def run_docusaurus_build(tmp_dir_name: str) -> None:
    # https://docusaurus.io/docs/cli#docusaurus-build-sitedir
    subprocess.call(
        [
            "yarn",
            "build",
            "--out-dir",
            tmp_dir_name,
        ],
    )


def run_grep(target_dir: str) -> List[str]:
    out = subprocess.check_output(
        [
            "grep",
            "-Enr",
            r"<\/?snippet>",
            target_dir,
        ],
        universal_newlines=True,
    )
    return out.splitlines()


def main() -> None:
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
