"""
Usage: `python trace_docs_deps.py [DOCS_DIR]`

This script is used in our Azure Docs Integration pipeline (azure-pipelines-docs-integration.yml) to determine whether
a change has been made in the `great_expectations/` directory that change impacts `docs/` and the snippets therein.

The script takes the following steps:
    1. Parses all markdown files in `docs/`, using regex to find any Docusaurus links (i.e. ```python file=...#L10-20)
    2. Goes to each linked file and uses AST to parse imports used there
    3. Filters for only relative imports and determines the paths to those files

The resulting output list is all of the dependencies `docs/` has on the primary `great_expectations/` directory.
If a change is identified in any of these files during the pipeline runtime, we know that a docs dependency has possibly
been impacted and the pipeline should run to ensure adequate test coverage.

Please note that this solution has some inherit disclaimers one should be familiar with. As there is no actual execution
of source code (but rather parsing of the source file's AST), it is hard to pinpoint the location of a given import with
absolute certainty.  Utilizing the `__file__` attr would provide the exact path of an import but without that, we need
to assume that an `ast.Import` node corresponds to an accurate path in our codebase (ex: `great_expectations.core.batch`
results in great_expectations/core/batch.py).

As long as imports correspond to the relative path of modules, this method will continue to work as intended. Deviations
here could result in certain imports not being picked up by the script, reducing the accuracy and frequency of the
Azure Docs Integration pipeline's triggers. As GE has maintained this consistency throughout its development, this
script and the methods used herein are deemed to be appropriate.

"""

import ast
import glob
import os
import re
import sys
from typing import List, Set


def find_docusaurus_refs(dir: str) -> List[str]:
    """Finds any Docusaurus links within a target directory (i.e. ```python file=...#L10-20)"""
    linked_files: Set[str] = set()
    pattern: str = (
        r"\`\`\`[a-zA-Z]+ file"  # Format of internal links used by Docusaurus
    )

    for doc in glob.glob(f"{dir}/**/*.md", recursive=True):
        for line in open(doc):
            if re.search(pattern, line):
                file: str = _parse_file_from_docusaurus_link(line)
                path: str = os.path.join(os.path.dirname(doc), file)
                linked_files.add(path)

    return [file for file in linked_files]


def _parse_file_from_docusaurus_link(line: str) -> str:
    pattern: str = "=(.+?)#"  # Parse just the path from the Docusaurus link
    search: re.Match[str] = re.search(pattern, line)
    return search.group(1)


def get_local_imports(files: List[str]) -> List[str]:
    """Parses a list of files to determine local imports; external dependencies are discarded"""
    imports: Set[str] = set()

    for file in files:
        with open(file) as f:
            root: ast.Module = ast.parse(f.read(), file)

        for node in ast.walk(root):
            # ast.Import is only used for external deps
            if not isinstance(node, ast.ImportFrom):
                continue

            # Only consider imports relevant to GE (note that "import great_expectations as ge" is discarded)
            if (
                isinstance(node.module, str)
                and "great_expectations" in node.module
                and node.module.count(".") > 0
            ):
                imports.add(node.module)

    return [imp for imp in imports]


def get_import_paths(imports: List[str]) -> List[str]:
    """Takes a list of imports and determines the relative path to each source file or module"""
    paths: List[str] = []

    for imp in imports:
        path: str = imp.replace(
            ".", "/"
        )  # AST nodes are formatted as great_expectations.module.file
        _update_paths(paths, path)

    return paths


def _update_paths(paths: List[str], path: str) -> None:
    if os.path.isfile(f"{path}.py"):
        paths.append(f"{path}.py")
    elif os.path.isdir(path):
        for file in glob.glob(f"{path}/**/*.py", recursive=True):
            paths.append(file)


if __name__ == "__main__":
    files: List[str] = find_docusaurus_refs(sys.argv[1])
    imports: List[str] = get_local_imports(files)
    paths: List[str] = get_import_paths(imports)
    for path in paths:
        print(path)
