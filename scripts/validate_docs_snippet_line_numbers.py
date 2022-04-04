import glob
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


OPENING_TAG: str = "<snippet>"
CLOSING_TAG: str = "</snippet>"
DOCUSAURUS_REGEX: re.Pattern = re.compile(r"```python file=(.+)#L(\d*)(?:-L)?(\d*)")


@dataclass
class DocusaurusRef:
    docs_file: str
    py_file: str
    line_numbers: Tuple[int, int]

    @classmethod
    def from_regex_parsed_values(
        cls, file: str, values: Tuple[str, str, str]
    ) -> "DocusaurusRef":
        path, start, end = values

        start_line: int = int(start)
        end_line: int = int(end) if end else start_line
        line_numbers: Tuple[int, int] = start_line, end_line

        relative_path: str = os.path.join(os.path.dirname(file), path)
        cleaned_path: str = os.path.normpath(relative_path)

        return cls(docs_file=file, py_file=cleaned_path, line_numbers=line_numbers)


def collect_docusaurus_refs(directory: str) -> Dict[str, List[DocusaurusRef]]:
    """Iterate through a directory and parses all embedded Docusaurus refs to Python files.

    Args:
        directory: The dir to recursively search through.

    Returns:
        A map between Python files and the docs that reference them.
    """
    docusaurus_refs: Dict[str, List[DocusaurusRef]] = defaultdict(list)
    for file in glob.glob(f"{directory}/**/*.md", recursive=True):
        logger.debug("Checking %s for Docusaurus links", file)
        _collect_docusaurus_refs(file=file, reference_map=docusaurus_refs)

    return docusaurus_refs


def _collect_docusaurus_refs(
    file: str, reference_map: Dict[str, List[DocusaurusRef]]
) -> None:
    with open(file) as f:
        contents = f.read()

    references: List[Tuple[str, str, str]] = DOCUSAURUS_REGEX.findall(contents)

    for reference in references:
        docusaurus_ref = DocusaurusRef.from_regex_parsed_values(
            file=file, values=reference
        )
        reference_map[docusaurus_ref.py_file].append(docusaurus_ref)
        logger.debug(
            "Create association between %s and the docs that use it",
            docusaurus_ref.py_file,
        )


def evaluate_snippet_validity(
    docusaurus_refs: Dict[str, List[DocusaurusRef]]
) -> List[DocusaurusRef]:
    """Given a series of references, reviews the referenced files and ensures that
    opening and closing tags directly surround the snippet.

    Ex: ```python file=...#L2

        1 <snippet>
        2
        3 context = DataContext()
        4 </snippet>

    Note that the tags do not need to be immediately before and after the content - as long
    as the lines in between are whitespace, the algorithm will not flag it as an issue.

    Args:
        docusaurus_refs: A map between Python files and the docs that reference them.

    Returns:
        A list of broken references.
    """
    all_broken_refs: List[DocusaurusRef] = []
    for docs_file, ref_list in docusaurus_refs.items():
        logger.debug(
            "Checking %s for snippet validity",
            docs_file,
        )
        file_broken_refs: List[DocusaurusRef] = _evaluate_snippet_validity(
            docs_file, ref_list
        )
        all_broken_refs.extend(file_broken_refs)

    return all_broken_refs


def _evaluate_snippet_validity(
    file: str, ref_list: List[DocusaurusRef]
) -> List[DocusaurusRef]:
    with open(file) as f:
        file_contents: List[str] = f.readlines()

    broken_refs: List[DocusaurusRef] = []
    for ref in ref_list:
        if not _validate_docusaurus_ref(ref, file_contents):
            broken_refs.append(ref)

    return broken_refs


def _validate_docusaurus_ref(
    docusaurus_ref: DocusaurusRef, file_contents: List[str]
) -> bool:
    start_line, end_line = docusaurus_ref.line_numbers
    if start_line > len(file_contents) or end_line > len(file_contents):
        logger.warning(
            "Invalid snippet due to out-of-bounds reference: %s", docusaurus_ref
        )
        return False

    return _validate_opening_tag(start_line, file_contents) and _validate_closing_tag(
        end_line, file_contents
    )


def _validate_opening_tag(start_line: int, file_contents: List[str]) -> bool:
    ptr: int = start_line - 1

    line: str
    while ptr >= 0:
        line = file_contents[ptr - 1]
        if OPENING_TAG in line:
            return True
        elif not line.isspace():
            return False
        ptr -= 1

    return False


def _validate_closing_tag(end_line: int, file_contents: List[str]) -> bool:
    ptr: int = end_line + 1

    line: str
    while ptr < len(file_contents):
        line = file_contents[ptr - 1]
        if CLOSING_TAG in line:
            return True
        elif not line.isspace():
            return False
        ptr += 1

    return False


def print_diagnostic_report(broken_refs: List[DocusaurusRef]) -> None:
    """Prints results to console for the user to review.

    Args:
        broken_refs: A list of broken refernces (as parsed and determined by prior stages).
    """
    refs_by_source_doc: Dict[str, List[DocusaurusRef]] = defaultdict(list)
    for ref in broken_refs:
        refs_by_source_doc[ref.docs_file].append(ref)

    for docs_file, refs in refs_by_source_doc.items():
        print(f"\n{docs_file}:")
        for ref in refs:
            start, end = ref.line_numbers
            print(f"  {ref.py_file}: L{start}-L{end}")

    print(
        f"\n[ERROR] {len(broken_refs)} snippet issue(s) found in {len(refs_by_source_doc)} docs file(s)!"
    )


if __name__ == "__main__":
    docusaurus_refs: Dict[str, List[DocusaurusRef]] = collect_docusaurus_refs("docs")
    broken_refs: List[DocusaurusRef] = evaluate_snippet_validity(docusaurus_refs)
    if broken_refs:
        print_diagnostic_report(broken_refs)
    else:
        print("[SUCCESS] All snippets are valid and referenced properly!")

    # Chetan - 20220316 - While this number should be 0, getting the number of warnings down takes time
    # and effort. In the meanwhile, we want to set an upper bound on warnings to ensure we're not introducing
    # further regressions. As snippets are validated, developers should update this number.
    broken_ref_threshold: int = 494
    broken_ref_count: int = len(broken_refs)
    assert (
        broken_ref_count <= broken_ref_threshold
    ), f"""A broken snippet reference was introduced; please resolve the matter before merging.
                We expect there to be {broken_ref_threshold} or fewer broken references (actual: {broken_ref_count})"""
