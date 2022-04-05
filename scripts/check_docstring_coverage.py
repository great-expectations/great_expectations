import ast
import glob
import subprocess
from collections import defaultdict
from typing import Dict, List, Tuple, cast

Diagnostics = Dict[str, List[Tuple[ast.FunctionDef, bool]]]

DOCSTRING_ERROR_THRESHOLD = (
    1087  # This number is to be reduced as we document more public functions!
)


def get_changed_files() -> List[str]:
    git_diff: subprocess.CompletedProcess = subprocess.run(
        ["git", "diff", "origin/develop", "--name-only"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return [f for f in git_diff.stdout.split()]


def collect_functions(directory_path: str) -> Dict[str, List[ast.FunctionDef]]:
    all_funcs: Dict[str, List[ast.FunctionDef]] = {}

    file_paths: List[str] = _gather_source_files(directory_path)
    for file_path in file_paths:
        all_funcs[file_path] = _collect_functions(file_path)

    return all_funcs


def _gather_source_files(directory_path: str) -> List[str]:
    return glob.glob(f"{directory_path}/**/*.py", recursive=True)


def _collect_functions(file_path: str) -> List[ast.FunctionDef]:
    with open(file_path) as f:
        root: ast.Module = ast.parse(f.read())

    return cast(
        List[ast.FunctionDef],
        list(filter(lambda n: isinstance(n, ast.FunctionDef), ast.walk(root))),
    )


def gather_docstring_diagnostics(
    all_funcs: Dict[str, List[ast.FunctionDef]]
) -> Diagnostics:
    diagnostics: Diagnostics = defaultdict(list)
    for file, func_list in all_funcs.items():
        public_funcs: List[ast.FunctionDef] = list(
            filter(
                lambda f: _function_filter(f),
                func_list,
            )
        )
        for func in public_funcs:
            result: Tuple[ast.FunctionDef, bool] = (func, bool(ast.get_docstring(func)))
            diagnostics[file].append(result)

    return diagnostics


def _function_filter(func: ast.FunctionDef) -> bool:
    # Private and dunder funcs/methods
    if func.name.startswith("_"):
        return False
    # Getters and setters
    for decorator in func.decorator_list:
        if (isinstance(decorator, ast.Name) and decorator.id == "property") or (
            isinstance(decorator, ast.Attribute) and decorator.attr == "setter"
        ):
            return False
    return True


def review_diagnostics(diagnostics: Diagnostics, changed_files: List[str]) -> None:
    total_passed: int = 0
    total_funcs: int = 0

    relevant_diagnostics: Dict[str, List[ast.FunctionDef]] = defaultdict(list)

    for file, diagnostics_list in diagnostics.items():
        relevant_file: bool = file in changed_files
        for func, success in diagnostics_list:
            if success:
                total_passed += 1
            elif not success and relevant_file:
                relevant_diagnostics[file].append(func)
            total_funcs += 1

    print(
        f"[SUMMARY] {total_passed} of {total_funcs} public functions ({100 * total_passed / total_funcs:.2f}%) have docstrings!"
    )

    if relevant_diagnostics:
        print(
            "\nHere are violations of the style guide that are relevant to the files changed in your PR:"
        )

        for file, func_list in relevant_diagnostics.items():
            print(f"  {file}:")
            for func in func_list:
                print(f"    L{func.lineno}:{func.name}")

    total_failed: int = total_funcs - total_passed
    assert (
        total_failed <= DOCSTRING_ERROR_THRESHOLD
    ), f"""A public function without a docstring was introduced; please resolve the matter before merging.
                We expect there to be {total_failed} or fewer violations of the style guide (actual: {total_failed})"""


if __name__ == "__main__":
    changed_files: List[str] = get_changed_files()
    all_funcs: Dict[str, List[ast.FunctionDef]] = collect_functions(
        "great_expectations"
    )
    docstring_diagnostics: Diagnostics = gather_docstring_diagnostics(all_funcs)
    review_diagnostics(docstring_diagnostics, changed_files)
