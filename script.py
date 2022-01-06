import ast
import glob
import re


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


def parse_test_results():
    with open("test_performance.txt") as f:
        contents = f.read()

    print(contents.split(" ").count("call"))


if __name__ == "__main__":
    # print("========== Great Expectations ==========")
    # for func in get_n_longest_funcs("great_expectations", 20):
    #     print(func)

    # print("\n========== Tests ==========")
    # for func in get_n_longest_funcs("tests", 20):
    #     print(func)

    parse_test_results()
