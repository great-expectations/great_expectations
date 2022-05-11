"""
1. Run this script to modify great_expectations/
2. Run `black` to check for any bad injections
  - Resolve as needed (should be very minimal)
3. Run `pytest`
"""


import ast
import glob

import astunparse  # Necessary since this was introduced in stdlib in 3.9

SNIPPET = """
import inspect
__frame = inspect.currentframe()
__file = __frame.f_code.co_filename
__func = __frame.f_code.co_name
for k, v in __frame.f_locals.items():
    if any(var in k for var in ('__frame', '__file', '__func')):
        continue
    print(f'<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}')
"""


# 1. Collect files
files = glob.glob("great_expectations/**/*.py", recursive=True)

for file in files:
    # 2. Collect functions
    with open(file) as f:
        root: ast.Module = ast.parse(f.read())
        funcs = list(filter(lambda n: isinstance(n, ast.FunctionDef), ast.walk(root)))

    # 3. Inject snippet into functions
    for func in funcs:
        node = ast.parse(SNIPPET)
        updated_body = node.body + func.body
        func.body = updated_body
        ast.fix_missing_locations(func)

    # 4. Write updated functions based to disk
    with open(file, "w") as f:
        f.write(astunparse.unparse(root))
