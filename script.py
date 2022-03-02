from typing import List
import glob
import ast

def parse_tests(tests_dir: str):
    for path in glob.glob(f"{tests_dir}/**/*.py", recursive=True):
        filename = path.split("/")[-1]
        if not filename.startswith("test_"):
            continue
        nodes = _parse_tests(path)
        for node in nodes:
            breakpoint()
            print(filename, node.args)


def _parse_tests(file: str) -> List[ast.FunctionDef]:
    with open(file) as f:
        root = ast.parse(f.read(), file)

    test_nodes: List[ast.FunctionDef] = list(
        filter(
            lambda node: isinstance(node, ast.FunctionDef)
            and node.name.startswith("test_"),
            root.body,
        )
    )

    return test_nodes

parse_tests("tests")
