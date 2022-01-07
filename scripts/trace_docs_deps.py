"""
Usage: `python trace_docs_deps.py`

This script is used in our Azure Docs Integration pipeline (azure-pipelines-docs-integration.yml) to determine whether
a change has been made in the `great_expectations/` directory that change impacts `docs/` and the snippets therein.

The script takes the following steps:
    1. Uses AST to parse the source code in `great_expectations/`; the result is a mapping between function/class definition and the origin file of that symbol
    2. Parses all markdown files in `docs/`, using regex to find any Docusaurus links (i.e. ```python file=...#L10-20)
    3. Evaluates each linked file using AST and leverages the definition map from step #1 to determine which source files are relevant to docs under test

The resulting output list is all of the dependencies `docs/` has on the primary `great_expectations/` directory.
If a change is identified in any of these files during the pipeline runtime, we know that a docs dependency has possibly
been impacted and the pipeline should run to ensure adequate test coverage.

"""

import ast
import glob
import logging
import os
import re
from collections import defaultdict
from typing import DefaultDict, Dict, List, Set

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
        file_definition_map = parse_definition_nodes_from_file(file)
        _update_dict(definition_map, file_definition_map)
    return definition_map


def parse_definition_nodes_from_file(file: str) -> Dict[str, Set[str]]:
    """See `parse_definition_nodes_from_source_code`"""
    with open(file) as f:
        root = ast.parse(f.read(), file)

    logger.debug(f"Parsing {file} for function/class definnitions")

    # Parse all 'def ...' and 'class ...' statements in the source code
    definition_nodes = []
    for node in root.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            name = node.name
            definition_nodes.append(name)
            logger.debug(f"Found symbol {name}")

    # Associate the function/class name with the file it comes from
    file_definition_map: DefaultDict[str, Set[str]] = defaultdict(set)
    for name in definition_nodes:
        file_definition_map[name].add(file)
    logger.debug(f"Added {len(definition_nodes)} definitions to map")

    return file_definition_map


def _update_dict(A: Dict[str, Set[str]], B: Dict[str, Set[str]]) -> None:
    for key, val in A.items():
        if key in B:
            A[key] = val.union(B[key])

    for key, val in B.items():
        if key not in A:
            A[key] = {v for v in val}


def find_docusaurus_refs_in_docs(directory: str) -> List[str]:
    """Finds any Docusaurus links within a target directory (i.e. ```python file=...#L10-20)

    Args:
        directory: The directory that contains your Docusaurus files (docs/)

    Returns:
        A list of test files that are referenced within docs under test

    """
    linked_files: Set[str] = set()

    for doc in glob.glob(f"{directory}/**/*.md", recursive=True):
        file_refs = find_docusaurus_refs_in_file(doc)
        linked_files.update(file_refs)

    return sorted(linked_files)


def find_docusaurus_refs_in_file(file: str) -> Set[str]:
    """See `find_docusaurus_refs_in_docs`"""
    with open(file) as f:
        contents = f.read()

    logger.debug(f"Reviewing {file} for Docusaurus links")

    file_refs: Set[str] = set()

    # Format of internal links used by Docusaurus
    r = re.compile(r"```python file=([\.\/l\w]+)")
    matches = r.findall(contents)
    if not matches:
        logger.info(f"Could not find any Docusaurs links in {file}")
        return file_refs

    for match in matches:
        path: str = os.path.join(os.path.dirname(file), match)
        # only interested in looking at .py files for now (excludes .yml files)
        if path[-3:] == ".py":
            file_refs.add(path)
        else:
            logger.info(f"Excluding {path} due to not being a .py file")

    return file_refs


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
        symbols = retrieve_symbols_from_file(file)
        for symbol in symbols:
            paths = definition_map.get(symbol, set())
            relevant_source_files.update(paths)

    return sorted(relevant_source_files)


def retrieve_symbols_from_file(file: str) -> Set[str]:
    """See `retrieve_symbols_from_file`"""
    with open(file) as f:
        root = ast.parse(f.read(), file)

    symbols = set()
    for node in ast.walk(root):
        # If there is a function/constructor call, make sure we pick it up
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute):
                symbols.add(func.attr)
                logger.debug(f"Identified symbol {func.attr}")
            elif isinstance(func, ast.Name):
                symbols.add(func.id)
                logger.debug(f"Identified symbol {func.id}")

    logger.debug(f"parsed {len(symbols)} symbols from {file}")
    return symbols


def main() -> None:
    definition_map = parse_definition_nodes_from_source_code("great_expectations")
    files_referenced_in_docs = find_docusaurus_refs_in_docs("docs")
    paths = determine_relevant_source_files(files_referenced_in_docs, definition_map)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
