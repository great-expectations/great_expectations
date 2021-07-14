"""
Usage: `./trace_docs_deps [DOCS_DIR]`

This script is used in our Azure Docs Integration pipeline (azure-pipelines-docs-integration.yml) to determine whether
a change has been made in the `great_expectations/` directory that change impacts `docs/` and the snippets therein.

The script takes the following steps:
    1. Parses all markdown files in `docs/`, using regex to find any Docusaurus links (i.e. ```python file=...#L10-20)
    2. Goes to each linked file and uses AST to parse imports used there
    3. Filters for only relative imports and determines the paths to those files

The resulting output list is all of the dependencies `docs/` has on the primary `great_expectations/` directory.
If a change is identified in any of these files during the pipeline runtime, we know that a docs dependency has possibly
been impacted and the pipeline should run to ensure adequate test coverage.

"""

import ast
import glob
import os
import re
import sys
from typing import List, Set


def find_docusaurus_refs(dir: str) -> List[str]:
    linked_files: Set[str] = set()
    pattern: str = r"\`\`\`[a-zA-Z]+ file"

    for doc in glob.glob(f"{dir}/**/*.md", recursive=True):
        for line in open(doc):
            if re.search(pattern, line):
                path: str = get_relative_path(line, doc)
                linked_files.add(path)

    return [file for file in linked_files]


def get_relative_path(line: str, doc: str) -> str:
    pattern: str = "=(.+?)#"
    search: re.Match[str] = re.search(pattern, line)
    path: str = search.group(1)

    nesting: int = doc.count("/")
    parts: List[str] = path.split("/")
    return "/".join(part for part in parts[nesting:])


def get_imports(files: List[str]) -> List[str]:
    imports: Set[str] = set()

    for file in files:
        with open(file) as fh:
            root: ast.Module = ast.parse(fh.read(), file)
        for node in ast.walk(root):
            if not isinstance(node, ast.ImportFrom):
                continue

            if isinstance(node.module, str):
                imports.add(node.module)

    return [imp for imp in imports]


def get_paths(imports: List[str]) -> List[str]:
    paths: List[str] = []

    for imp in imports:
        if "great_expectations" not in imp or imp.count(".") == 0:
            continue

        path: str = imp.replace(".", "/")
        if os.path.isfile(f"{path}.py"):
            paths.append(f"{path}.py")
        elif os.path.isdir(path):
            for file in glob.glob(f"{path}/**/*.py", recursive=True):
                paths.append(file)

    return paths


if __name__ == "__main__":
    files: List[str] = find_docusaurus_refs(sys.argv[1])
    imports: List[str] = get_imports(files)
    paths: List[str] = get_paths(imports)
    for path in paths:
        print(path)
