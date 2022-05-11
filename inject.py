"""
1. Run this script to modify great_expectations/
2. Resolve any conflicts but DO NOT run `black` or other formatters.
  - Should be minimal (if any)
3. Run `pytest` (with `-s` if required)
"""

import ast
import glob
import sys

import astunparse  # Necessary since this was introduced in stdlib in 3.9

SNIPPET = """
import inspect
__frame = inspect.currentframe()
__file = __frame.f_code.co_filename
__func = __frame.f_code.co_name
for k, v in __frame.f_locals.items():
    if any(var in k for var in ('self', 'cls', '__frame', '__file', '__func', 'inspect')):
        continue
    print(f'META:{__file}:{__func}:{k} - {v.__class__.__name__}')
"""


def insert_snippets(directory: str) -> None:
    # 1. Collect files
    files = glob.glob(f"{directory}/**/*.py", recursive=True)
    files = list(filter(lambda f: "marshmallow" not in f, files))

    for file in files:
        # 2. Collect functions
        with open(file) as f:
            root: ast.Module = ast.parse(f.read())
            funcs = list(
                filter(lambda n: isinstance(n, ast.FunctionDef), ast.walk(root))
            )

        # 3. Inject snippet into functions
        for func in funcs:
            node = ast.parse(SNIPPET)
            updated_body = node.body + func.body
            func.body = updated_body
            ast.fix_missing_locations(func)

        # 4. Write updated functions based to disk
        with open(file, "w") as f:
            f.write(astunparse.unparse(root))


if __name__ == "__main__":
    insert_snippets(sys.argv[1])
