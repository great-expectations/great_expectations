import ast
import glob
import pathlib
from dataclasses import dataclass
from typing import Tuple, List

import astunparse

from scripts.validate_docs_snippet_line_numbers import collect_references, Reference


@dataclass
class CodeSnippet:
    reference: Reference
    code: Tuple[str]


class PublicAPIDocSnippetRetriever:

    def __init__(self, repo_root: pathlib.Path) -> None:
        self.repo_root = repo_root

    def generate_report(self, docs_code_references: List[Reference]):
        code_snippets = self._create_code_snippets_from_references(docs_code_references=docs_code_references)
        # print(code_snippets)
        snippet_num = 3

        print("".join(code_snippets[snippet_num].code))
        print(code_snippets[snippet_num].reference)
        parsed_example = self._parse_code_snippet_to_ast(code_snippet=code_snippets[snippet_num])

        print(astunparse.dump(parsed_example))



    def _create_code_snippets_from_references(self, docs_code_references: List[Reference]) -> List[CodeSnippet]:
        """Load file to create CodeSnippet from code Reference.

        Args:
            docs_code_references: References to code snippets.

        Returns:
            List of CodeSnippets which each contain the actual code from the reference.
        """
        code_snippets: List[CodeSnippet] = []
        for docs_code_reference in docs_code_references:
            should_parse_reference: bool = (docs_code_reference.target_path.endswith(".py")) and docs_code_reference.target_lines

            if should_parse_reference:
                with open(self.repo_root / docs_code_reference.target_path) as f:
                    file_lines: List[str] = f.readlines()

                assert docs_code_reference.target_lines
                start, end = docs_code_reference.target_lines
                start -= 1  # Docusaurus snippets are 1 indexed
                # lines = tuple(file_lines[start:end])
                # TODO: Revert to snippet, not whole file
                lines = tuple(file_lines)

                code_snippets.append(CodeSnippet(reference=docs_code_reference, code=lines))

        return code_snippets

    def _parse_code_snippet_to_ast(self, code_snippet: CodeSnippet) -> ast.AST:
        code_snippet_str = "\n".join(code_snippet.code)
        tree = ast.parse(code_snippet_str)
        return tree


class PublicAPIDocParser:
    """Parse examples from docs to find classes, methods and functions used."""

    def __init__(self):
        pass

    def retrieve_definitions(self):
        """Retrieve all definitions used (class, method + function)."""
        pass


class PublicAPIChecker:
    """Check if functions, methods and classes are marked part of the PublicAPI."""

    def __init__(self, repo_root: pathlib.Path) -> None:
        self.repo_root = repo_root


    def get_all_public_api_functions(self):

        filepath = self.repo_root / "great_expectations/data_context/data_context/abstract_data_context.py"
        # print(self._list_public_functions_in_file(filepath=filepath))
        class_defs = self._list_classes_in_file(filepath=filepath)

        for class_def in class_defs:
            print(astunparse.dump(class_def))



    def _list_classes_in_file(self, filepath: pathlib.Path):  # TODO: Return type
        # TODO: Make this return with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        class_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_defs.append(node)

        return class_defs

    def _list_public_functions_in_file(self, filepath: pathlib.Path) -> List[str]:
        # TODO: Make this return function with dotted path, not just str
        with open(self.repo_root / filepath) as f:
            file_contents: str = f.read()

        tree = ast.parse(file_contents)

        def flatten_attr(node):
            if isinstance(node, ast.Attribute):
                return str(flatten_attr(node.value)) + '.' + node.attr
            elif isinstance(node, ast.Name):
                return str(node.id)
            else:
                pass

        public_functions: List[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                found_decorators = []
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name):
                        found_decorators.append(decorator.id)
                    elif isinstance(decorator, ast.Attribute):
                        found_decorators.append(flatten_attr(decorator))

                # print(node.name, found_decorators)

                if "public_api" in found_decorators:
                    public_functions.append(node.name)

        return public_functions



def main():

    # Get code references
    repo_root = pathlib.Path(__file__).parent.parent
    docs_dir = repo_root / "docs"
    files = glob.glob(f"{docs_dir}/**/*.md", recursive=True)
    docs_code_references = collect_references(files)

    # report_generator = PublicAPIDocSnippetRetriever(repo_root=repo_root)
    # report_generator.generate_report(docs_code_references)



    # Parse references
    # Generate set of module level functions, classes, and methods
    # Parse full great_expectations code, find everything decorated @public_api

    public_api_checker = PublicAPIChecker(repo_root=repo_root)
    public_api_checker.get_all_public_api_functions()
    # Report of what is in the references and not marked @public_api
    # Report on which do not have corresponding sphinx source stub files (unless we end up autogenerating these)







if __name__ == "__main__":
    main()