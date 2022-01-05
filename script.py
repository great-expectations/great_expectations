# Get the longest functions by line count

import ast
import glob


def get_n_longest_funcs(dir, n):
    function_definitions = []
    for path in glob.glob(f"{dir}/**/*.py", recursive=True):
        with open(path) as f:
            root = ast.parse(f.read(), path)

        for node in ast.walk(root):
            if isinstance(node, ast.FunctionDef):
                length = node.end_lineno - node.lineno
                definition = (node.name, length, path)
                function_definitions.append(definition)

    function_definitions.sort(key=lambda x: x[1], reverse=True)
    return function_definitions[:n]


print("===== Great Expectations =====")
for func in get_n_longest_funcs("great_expectations", 20):
    print(func)

print("\n===== Tests =====")
for func in get_n_longest_funcs("tests", 20):
    print(func)
