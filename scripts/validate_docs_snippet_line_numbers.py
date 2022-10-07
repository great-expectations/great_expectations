import enum
import glob
import pprint
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

# This should be reduced as snippets are added/fixed
VIOLATION_THRESHOLD = 523

r = re.compile(r"```\w+ file=(.+)")


class Status(enum.Enum):
    MISSING_BOTH = 0
    MISSING_OPENING = 1
    MISSING_CLOSING = 2
    COMPLETE = 3


@dataclass
class Reference:
    raw: str
    origin_path: str
    origin_line: int
    target_path: str
    target_lines: Optional[Tuple[int, int]]


@dataclass
class Result:
    ref: Reference
    status: Status

    def to_dict(self) -> dict:
        data = self.__dict__
        data["ref"] = data["ref"].__dict__
        data["status"] = data["status"].name
        return data


def collect_references(files: List[str]) -> List[Reference]:
    all_refs = []
    for file in files:
        file_refs = _collect_references(file)
        all_refs.extend(file_refs)

    return all_refs


def _collect_references(file: str) -> List[Reference]:
    with open(file) as f:
        lines = f.readlines()

    refs = []
    for i, line in enumerate(lines):
        match = r.match(line.strip())
        if not match:
            continue

        ref = _parse_reference(match=match, file=file, line=i + 1)
        refs.append(ref)

    return refs


def _parse_reference(match: re.Match, file: str, line: int) -> Reference:
    # Chetan - 20221007 - This parsing logic could probably be cleaned up with a regex
    # and pathlib/os but since this is not prod code, I'll leave cleanup as a nice-to-have
    raw_path = match.groups()[0].strip()
    parts = raw_path.split("#")

    target_path = parts[0]
    while target_path.startswith("../"):
        target_path = target_path[3:]

    target_lines: Optional[Tuple[int, int]]
    if len(parts) == 1:
        target_lines = None
    else:
        line_nos = parts[1].split("-")

        start = int(line_nos[0][1:])
        end: int
        if len(line_nos) == 1:
            end = start
        else:
            end = int(line_nos[1][1:])
        target_lines = (start, end)

    return Reference(
        raw=raw_path,
        origin_path=file,
        origin_line=line,
        target_path=target_path,
        target_lines=target_lines,
    )


def determine_results(refs: List[Reference]) -> List[Result]:
    all_results = []
    for ref in refs:
        result = _determine_result(ref)
        all_results.append(result)

    return all_results


def _determine_result(ref: Reference) -> Result:
    if ref.target_lines is None:
        return Result(
            ref=ref,
            status=Status.COMPLETE,
        )

    with open(ref.target_path) as f:
        lines = f.readlines()

    start, end = ref.target_lines

    try:
        open_tag = lines[start - 2]
        valid_open = "<snippet" in open_tag
    except IndexError:
        valid_open = False

    try:
        close_tag = lines[end]
        valid_close = "snippet>" in close_tag
    except IndexError:
        valid_close = False

    status: Status
    if valid_open and valid_close:
        status = Status.COMPLETE
    elif valid_open:
        status = Status.MISSING_CLOSING
    elif valid_close:
        status = Status.MISSING_OPENING
    else:
        status = Status.MISSING_BOTH

    return Result(
        ref=ref,
        status=status,
    )


def evaluate_results(results: List[Result]) -> None:
    summary = {}

    for res in results:
        key = res.status.name
        val = res.to_dict()
        if key not in summary:
            summary[key] = []
        summary[key].append(val)

    pprint.pprint(summary)

    print("\n[SUMMARY]")
    for key, val in summary.items():
        print(f"* {key}: {len(val)}")

    violations = len(results) - len(summary[Status.COMPLETE.name])
    assert (
        violations <= VIOLATION_THRESHOLD
    ), f"Expected {VIOLATION_THRESHOLD} or fewer snippet violations, got {violations}"

    # There should only be COMPLETE (valid snippets) or MISSING_BOTH (snippets that haven't recieved surrounding tags yet)
    # The presence of MISSING_OPENING or MISSING_CLOSING signifies a misaligned line number reference
    assert (
        summary.get(Status.MISSING_OPENING.name, 0) == 0
    ), "Found a snippet without an opening snippet tag"
    assert (
        summary.get(Status.MISSING_CLOSING.name, 0) == 0
    ), "Found a snippet without an closing snippet tag"


def main() -> None:
    files = glob.glob("docs/**/*.md", recursive=True)
    refs = collect_references(files)
    results = determine_results(refs)

    evaluate_results(results)


if __name__ == "__main__":
    main()
