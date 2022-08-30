import ast
import glob
import re
from collections import namedtuple
from typing import Dict, List

DURATION_PATTERN = r"(\d*\.\d*)s\s*(\w*)\s*([\w\/\.]*)::(\w*)"
THRESHOLD = 1.0
MARKER = "@pytest.mark.slow"


DurationEntry = namedtuple("DurationEntry", ["duration", "stage", "file", "test"])


def parse_durations_file(file: str) -> List[DurationEntry]:
    with open(file) as f:
        contents = f.read()

    r = re.compile(DURATION_PATTERN)

    durations_list = []

    matches = r.findall(contents)
    for match in matches:
        assert len(match) == 4
        duration, stage, file, test = match
        entry = DurationEntry(float(duration), stage, file, test)
        durations_list.append(entry)

    return durations_list


def aggregate_durations(durations: List[DurationEntry]) -> Dict[str, Dict[str, float]]:
    durations_dict = {}
    for entry in durations:
        file = entry.file
        if file not in durations_dict:
            durations_dict[file] = {}

        test = entry.test
        duration = entry.duration
        if test not in durations_dict[file]:
            durations_dict[file][test] = 0

        durations_dict[file][test] += duration

    return durations_dict


def gather_test_files(pattern: str) -> List[str]:
    files = glob.glob(pattern, recursive=True)
    return files


def parse_files(files: List[str]) -> Dict[str, List[ast.FunctionDef]]:
    res = {}
    for file in files:
        nodes = _parse_file(file)
        if nodes:
            res[file] = nodes

    return res


def _parse_file(file: str) -> List[ast.FunctionDef]:
    with open(file) as f:
        root = ast.parse(f.read(), file)

    result_nodes = []

    all_funcs = list(filter(lambda n: isinstance(n, ast.FunctionDef), root.body))
    for func in all_funcs:
        if _check_for_unmarked(func):
            result_nodes.append(func)

    return result_nodes


def _check_for_unmarked(func: ast.FunctionDef) -> bool:
    name = func.name
    if not name.startswith("test_"):
        return False

    # decorators = func.decorator_list
    # if decorators:
    #     return False

    return True


def insert_marks(
    node_dict: Dict[str, List[ast.FunctionDef]],
    durations_dict: Dict[str, Dict[str, float]],
) -> None:
    for file, node_list in node_dict.items():
        for node in node_list:
            test_name = node.name

            duration = durations_dict.get(file, {}).get(test_name)
            if not duration or duration < THRESHOLD:
                continue  # Skip tests that aren't run or have 0.00 duration

            contains_pytest_import = False

            # Find where to insert mark
            with open(file) as f:
                contents = f.readlines()

                idx = -1
                for i, line in enumerate(contents):
                    if contains_pytest_import is False and "import pytest" in line:
                        contains_pytest_import = True
                    if line.startswith(f"def {test_name}("):
                        idx = i - 1
                        break

                # Update contents with mark
                if idx > 0 and MARKER not in contents[idx]:
                    print(test_name, duration)
                    contents[idx] += f"{MARKER} # {duration}\n"

            if not contains_pytest_import:
                contents.insert(0, "import pytest\n")

            # Insert updated contents
            with open(file, "w") as f:
                f.writelines(contents)


def main() -> None:
    durations_list = parse_durations_file("temp")
    durations_dict = aggregate_durations(durations_list)

    files = gather_test_files("tests/**/*.py")
    node_dict = parse_files(files)

    insert_marks(node_dict, durations_dict)


if __name__ == "__main__":
    main()
