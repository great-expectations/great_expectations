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
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Set, Union


def parse_definition_nodes_from_source_code(directory: str) -> Dict[str, Set[str]]:
    """Utility to parse all class/function definitions from a given codebase

    Args:
        source_files: A list of files from the codebase

    Returns:
        A mapping between class/function definition and the origin of that symbol.
        Using this, one can immediately tell where to look when encountering a class instance or method invocation.

    """
    definition_map: Dict[str, Set[str]] = {}
    for file in glob.glob(f"{directory}/**/*.py", recursive=True):
        file_definition_map = _parse_definition_nodes_from_file(file)
        _update_dict(definition_map, file_definition_map)
    return definition_map


def _parse_definition_nodes_from_file(file: str) -> Dict[str, Set[str]]:
    with open(file) as f:
        root = ast.parse(f.read(), file)

    definition_nodes = []
    for node in root.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            definition_nodes.append(node.name)

    file_definition_map: DefaultDict[str, Set[str]] = defaultdict(set)
    for name in definition_nodes:
        file_definition_map[name].add(file)

    return file_definition_map


def _update_dict(A: Dict[str, Set[str]], B: Dict[str, Set[str]]) -> None:
    for key, val in A.items():
        if key in B:
            A[key] = val.union(B[key])

    for key, val in B.items():
        if key not in A:
            A[key] = {v for v in val}


def find_docusaurus_refs(directory: str) -> List[str]:
    """Finds any Docusaurus links within a target directory (i.e. ```python file=...#L10-20)

    Args:
        directory: The directory that contains your Docusaurus files (docs/)

    Returns:
        A list of test files that are referenced within docs under test

    """
    linked_files: Set[str] = set()
    pattern: str = (
        r"\`\`\`[a-zA-Z]+ file"  # Format of internal links used by Docusaurus
    )

    for doc in glob.glob(f"{directory}/**/*.md", recursive=True):
        with open(doc) as f:
            lines = f.readlines()

        for line in lines:
            if re.search(pattern, line):
                file = _parse_file_from_docusaurus_link(line)
                if not file:
                    continue
                path: str = os.path.join(os.path.dirname(doc), file)
                # only interested in looking at .py files for now (excludes .yml files)
                if path[-3:] == ".py":
                    linked_files.add(path)

    return [file for file in linked_files]


def _parse_file_from_docusaurus_link(line: str) -> Optional[str]:
    pattern: str = "=(.+?)#"  # Parse just the path from the Docusaurus link
    search: Union[re.Match[str], None] = re.search(pattern, line)
    if search:
        return search.group(1)
    return None


def determine_relevant_source_files(
    files: List[str], definition_map: Dict[str, Set[str]]
) -> List[str]:
    """Uses AST to parse all symbols from an input list of files and maps them to their origins

    Args:
        files: List of files to evaluate with AST
        definition_map: An association between symbol and the origin of that symbol in the source code

    Returns:
        List of source files that are relevant to the Docusaurus docs

    """
    relevant_source_files = set()
    for file in files:
        symbols = _retrieve_symbols_from_file(file)
        for symbol in symbols:
            paths = definition_map.get(symbol, set())
            relevant_source_files.update(paths)

    return sorted(relevant_source_files)


def _retrieve_symbols_from_file(file: str) -> Set[str]:
    with open(file) as f:
        root = ast.parse(f.read(), file)

    symbols = set()
    for node in ast.walk(root):
        if isinstance(node, ast.Name):
            symbols.add(node.id)

    return symbols


if __name__ == "__main__":
    definition_map = parse_definition_nodes_from_source_code("great_expectations")
    files_referenced_in_docs = find_docusaurus_refs("docs")
    paths = determine_relevant_source_files(files_referenced_in_docs, definition_map)
    for path in paths:
        print(path)
