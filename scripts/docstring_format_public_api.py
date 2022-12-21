# Trying to use docformatter only on methods etc marked with @public_api decorator
import pathlib
import sys
import tokenize
from dataclasses import dataclass

import astor
import black
import docformatter
from docformatter import Configurater

from scripts.public_api_report import FileContents, CodeParser, PublicAPIChecker


def _repo_root():
    return pathlib.Path(__file__).parent.parent

filepath = _repo_root() / "great_expectations/data_context/data_context/abstract_data_context.py"

# with open(filepath) as f:
#     for token_type, token_string, start, end, line in tokenize.generate_tokens(f.readline):
#
#         if token_type == tokenize.OP:
#             if token_string == "@":
#                 print(token_type, token_string, start, end, line)

file_contents = FileContents.create_from_local_files(filepaths={filepath})

file_contents_str = next(iter(file_contents)).contents

code_parser = CodeParser(file_contents=file_contents)

docs_example_parser = None

public_api_checker = PublicAPIChecker(
    docs_example_parser=docs_example_parser, code_parser=code_parser
)

definitions = public_api_checker.get_all_public_api_definitions()

for definition in definitions:
    # print(definition)

    astor_source_reconstruction = astor.to_source(definition.ast_definition)
    # print(astor_source_reconstruction)


    black_mode = black.mode.Mode()
    formatted_definition_str = black.format_str(src_contents=astor_source_reconstruction, mode=black_mode)

    # print(formatted_definition_str)

# print(file_contents_str)

config = Configurater(args=["some_file.py", "docstring_format_public_api.py"])
# config = Configurater(args=[])
config.do_parse_arguments()

formatter = docformatter.Formatter(
    args=config.args,
    stdout=sys.stdout,
    stderror=sys.stderr,
    stdin=sys.stdin,
)
print(formatter._format_code(source=formatted_definition_str))