# DON'T MERGE THIS
import ast
import glob
import os
import pprint
from typing import Any, Dict, Set

import pkg_resources


def parse_requirements() -> Dict[str, str]:
    relevant_reqs = {}

    requirements_files = glob.glob("requirements*.txt")
    for requirements_file in requirements_files:
        if "contrib" in requirements_file:
            continue
        with open(requirements_file) as f:
            contents = f.read()

        try:
            requirements = list(pkg_resources.parse_requirements(contents))
        except:
            continue

        for requirement in requirements:
            relevant_reqs[requirement.name] = requirements_file

    return relevant_reqs


def parse_imports(relevant_reqs: Dict[str, str]) -> Dict[str, Set[str]]:
    files = glob.glob("./**/*.py", recursive=True)
    import_map = {}

    for file in files:
        with open(file) as f:
            root = ast.parse(f.read())

        for node in ast.walk(root):
            if isinstance(node, ast.Import):
                identifier = node.names[0].name
            elif isinstance(node, ast.ImportFrom):
                identifier = node.module
            else:
                continue

            identifier = identifier.split(".")[0]

            if identifier not in relevant_reqs:
                continue

            if identifier not in import_map:
                import_map[identifier] = set()
            import_map[identifier].add(file)

    return import_map


def find_least_common_paths(
    import_map: Dict[str, Set[str]], relevant_reqs: Dict[str, str]
) -> Dict[str, Any]:
    results = {}
    for name, files in import_map.items():
        values = {}
        files = sorted(files)
        values["files"] = files
        common_path = os.path.commonpath(files) or "."
        values["common_path"] = common_path
        values["requirements_file"] = relevant_reqs[name]
        results[name] = values

    return results


def main() -> None:
    relevant_reqs = parse_requirements()
    import_map = parse_imports(relevant_reqs)
    results = find_least_common_paths(
        import_map=import_map, relevant_reqs=relevant_reqs
    )

    pprint.pprint(results)


if __name__ == "__main__":
    main()
