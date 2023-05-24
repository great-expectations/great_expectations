"""
Purpose: To ensure that no docs snippets use the old "Mark Down" style of including Python code snippets.

The old "Mark Down" style of including Python code snippets has the following form:

```python Python code
import great_expectations as gx

context = gx.get_context()
```

It can also be expressed in the following form:

```python title="Python code"
import great_expectations as gx

context = gx.get_context()
```

However, the new style of including Python code snippets refers to the Python module, containing the test, as follows:

```python name="tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context"
```

whereby "tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context", which
is the Python module, containing the integration test in the present example, would contain the following tagged code:

# Python
# <snippet name="tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

Adherence to this pattern is assertained by the present checker module.
"""

import pathlib
import shutil
import subprocess
import sys
from typing import List


ITEMS_IGNORED_FROM_NAME_TAG_SNIPPET_CHECKER = []


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
                "--line-number",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"```python ",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_negative = subprocess.run(
            [
                "grep",
                "--recursive",
                "--line-number",
                "--ignore-case",
                "--invert-match",
                "--regexp",
                r"python name=",
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
        ITEMS_IGNORED_FROM_NAME_TAG_SNIPPET_CHECKER
    )
    if new_violations:
        print(
            f"[ERROR] Found {len(grep_output)} snippets using file and line number syntax, please use named snippet syntax:"
        )
        for line in new_violations:
            print(line)
        sys.exit(1)

    unnecessary_exclusions = set(
        ITEMS_IGNORED_FROM_NAME_TAG_SNIPPET_CHECKER
    ).difference(grep_output)
    if unnecessary_exclusions:
        print(
            f"[ERROR] Found {len(unnecessary_exclusions)} snippets unnecessarily placed on exclusion list.  Please update exclusion list:"
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
