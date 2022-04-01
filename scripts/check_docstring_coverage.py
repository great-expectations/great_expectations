import ast
import glob
from collections import defaultdict
from typing import Dict, List, Tuple, cast

Diagnostics = Dict[str, List[Tuple[ast.FunctionDef, bool]]]


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
            filter(lambda f: not f.name.startswith("_"), func_list)
        )
        for func in public_funcs:
            if _is_getter_or_setter(func):
                continue
            result: Tuple[ast.FunctionDef, bool] = (func, bool(ast.get_docstring(func)))
            diagnostics[file].append(result)

    return diagnostics


def _is_getter_or_setter(func: ast.FunctionDef) -> bool:
    for decorator in func.decorator_list:
        if (
            (isinstance(decorator, ast.Name) and decorator.id == "property")
            or isinstance(decorator, ast.Attribute)
            and decorator.attr == "setter"
        ):
            return True
    return False


def render_diagnostics(diagnostics: Diagnostics) -> None:
    total_count = 0
    success_count = 0

    for file, diagnostics_list in diagnostics.items():
        failures = list(filter(lambda d: d[1] is False, diagnostics_list))
        failures.sort(key=lambda d: d[0].lineno)

        total_count += len(diagnostics_list)
        success_count += len(diagnostics_list) - len(failures)

        if failures:
            print(f"{file}:")
            for func, _ in failures:
                print(f"   L{func.lineno} - {func.name}")
            print()

    print(
        f"RESULT: {100 * success_count / total_count:.2f}% of public functions have docstrings!"
    )


if __name__ == "__main__":
    all_funcs: Dict[str, List[ast.FunctionDef]] = collect_functions(
        "great_expectations"
    )
    docstring_diagnostics: Diagnostics = gather_docstring_diagnostics(all_funcs)
    render_diagnostics(docstring_diagnostics)
