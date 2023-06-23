"""
Purpose: To ensure that all named snippets are referenced in the docs.

Python code snippets refers to the Python module, containing the test, as follows:

```python name="tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context"
```

whereby "tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context", which
is the Python module, containing the integration test in the present example, would contain the following tagged code:

# Python
# <snippet name="tests/integration/docusaurus/general_directory/specic_directory/how_to_do_my_operation.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

Find all named snippets and ensure that they are referenced in the docs using the above syntax.
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
        res_snippets = subprocess.run(
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--no-filename",
                "--ignore-case",
                "--word-regexp",
                "--regexp",
                r"^# <snippet .*name=.*>",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_snippet_names = subprocess.run(
            ["sed", 's/.*name="//; s/">//; s/version-[0-9\\.]* //'],
            text=True,
            input=res_snippets.stdout,
            capture_output=True,
        )

        res_snippet_usages = subprocess.run(
            [
                "grep",
                "--recursive",
                "--binary-files=without-match",
                "--no-filename",
                "--ignore-case",
                "-E",
                "--regexp",
                r"```(python|yaml).*name=",
                str(target_dir),
            ],
            text=True,
            capture_output=True,
        )
        res_snippet_used_names = subprocess.run(
            ["sed", 's/.*="//; s/".*//; s/version-[0-9\\.]* //'],
            text=True,
            input=res_snippet_usages.stdout,
            capture_output=True,
        )
        unused_snippet_names = sorted(
            list(
                set(res_snippet_names.stdout.splitlines()).difference(
                    set(res_snippet_used_names.stdout.splitlines())
                )
            )
        )
        return unused_snippet_names
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {e.cmd} returned with error (code {e.returncode}): {e.output}"
        ) from e


def main() -> None:
    check_dependencies("grep")
    check_dependencies("sed")
    project_root = pathlib.Path(__file__).parent.parent.parent
    docs_dir = project_root / "docs"
    assert docs_dir.exists()
    new_violations = run_grep(docs_dir)
    if new_violations:
        print(
            f"[ERROR] Found {len(new_violations)} snippets which are not used within a doc file."
        )
        for line in new_violations:
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()
