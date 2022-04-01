import ast
import glob
import logging
from collections import defaultdict
from typing import Dict, List, Tuple, cast

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

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
    directory_results = {}

    for file, diagnostics_list in diagnostics.items():

        base_directory: str = _get_base_directory(file)
        if base_directory not in directory_results:
            directory_results[base_directory] = [0, 0]

        for func, success in diagnostics_list:
            if success:
                directory_results[base_directory][0] += 1
            else:
                logger.info(f"{file} - L{func.lineno}:{func.name}")

            directory_results[base_directory][1] += 1

    repo_passed: int = 0
    repo_total: int = 0

    for directory, results in directory_results.items():
        passed, total = results
        repo_passed += passed
        repo_total += total

        ratio: str = f"{passed}/{total}"
        print(f"{directory: <50} {ratio: <10} ({100 * passed/total:.2f}%)")

    print(
        f"\nRESULT: {100 * repo_passed / repo_total:.2f}% of public functions have docstrings!"
    )


def _get_base_directory(path: str):
    parts = path.split("/")
    if parts[1].endswith(".py"):
        return parts[0]
    else:
        return "/".join(p for p in parts[:2])


if __name__ == "__main__":
    all_funcs: Dict[str, List[ast.FunctionDef]] = collect_functions(
        "great_expectations"
    )
    docstring_diagnostics: Diagnostics = gather_docstring_diagnostics(all_funcs)
    render_diagnostics(docstring_diagnostics)
