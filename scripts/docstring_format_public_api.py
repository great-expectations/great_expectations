# Trying to use docformatter only on methods etc marked with @public_api decorator
import pathlib
import tokenize



def _repo_root():
    return pathlib.Path(__file__).parent.parent

with open(_repo_root() / "great_expectations/data_context/data_context/abstract_data_context.py") as f:
    for token_type, token_string, start, end, line in tokenize.generate_tokens(f.readline):

        if token_type == tokenize.OP:
            if token_string == "@":
                print(token_type, token_string)

        # if token_type == tokenize.AT:
        #     print(token_type)


