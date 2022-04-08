import ast
import glob
import subprocess
from collections import defaultdict
from typing import Dict, List, Tuple, cast, Union
from os.path import basename
from pathlib import Path
import re


def _gather_source_files(directory_path: str) -> List[str]:
    return glob.glob(f"{directory_path}/**/*.py", recursive=True)


def _filter_source_files(file_paths: List[str]) -> List[str]:
    return [_ for _ in file_paths if not basename(_).startswith("_")]


def get_relevant_source_files(directory_path: str) -> List[str]:
    return _filter_source_files(_gather_source_files(directory_path))


def _get_ast_module(source_file_path: str) -> ast.Module:
    with open(source_file_path) as f:
        root: ast.Module = ast.parse(f.read())
    return root


def _filter_ast_module(ast_module: ast.Module) -> List[Union[ast.ClassDef, ast.FunctionDef]]:
    return [_ for _ in ast_module.body if isinstance(_, ast.ClassDef) or isinstance(_, ast.FunctionDef)]


def get_top_level_classes_and_methods(source_file_path: str) -> List[Union[ast.ClassDef, ast.FunctionDef]]:
    return _filter_ast_module(_get_ast_module(source_file_path))


def get_or_create_doc_folder(source_file_path):
    new_folder = Path(source_file_path.replace("../", "../docs/api_docs/"))
    new_folder = new_folder.with_suffix('')
    new_folder.mkdir(parents=True, exist_ok=True)
    return new_folder


def get_doc_file_path(doc_folder: Path, ast_obj: Union[ast.FunctionDef, ast.ClassDef]):
    file_path = doc_folder.joinpath(f"{ast_obj.name}__auto_api").with_suffix('.md')
    print(file_path)
    return file_path


def parse_tuple(ast_obj: ast.Tuple):
    tuple_items = []
    for item in ast_obj.elts:
        if isinstance(item, ast.Subscript):
            tuple_items.append(parse_subscript(item))
        elif isinstance(item, ast.Name):
            tuple_items.append(item.id)
        elif isinstance(item, ast.Attribute):
            tuple_items.append(item.attr)
        elif isinstance(item, ast.Constant):
            tuple_items.append(str(item.value))
        elif isinstance(item, ast.Tuple):
            tuple_items.append(parse_tuple(item))
        elif isinstance(item, ast.List):
            tuple_items.append(parse_tuple(item))
        else:
            print(f"{ast_obj.slice.value}")
            raise Exception
    out_str = ', '.join(tuple_items)
    return out_str


def parse_subscript(ast_obj: ast.Subscript, input_str: str = "") -> str:
    if isinstance(ast_obj.value, ast.Attribute):
        out_str = f"{input_str}{ast_obj.value.attr}["
    else:
        out_str = f"{input_str}{ast_obj.value.id}["
    if isinstance(ast_obj.slice.value, ast.Subscript):
        out_str = f"{parse_subscript(ast_obj.slice.value, out_str)}"
    elif isinstance(ast_obj.slice.value, ast.Tuple):
        out_str = f"{out_str}{parse_tuple(ast_obj.slice.value)}"
    elif isinstance(ast_obj.slice.value, ast.Name):
        out_str = f"{out_str}{ast_obj.slice.value.id}"
    elif isinstance(ast_obj.slice.value, ast.Attribute):
        out_str = f"{out_str}{ast_obj.slice.value.attr}"
    elif isinstance(ast_obj.slice.value, ast.Constant):
        out_str = f"{out_str}{ast_obj.slice.value.value}"
    else:
        out_str = f"{out_str}{ast_obj.slice.value}"
        print(f"{ast_obj.slice.value}")
        raise Exception
    out_str = f"{out_str}]"
    return out_str

def generate_function_documentation(ast_obj):
    api_doc = []
    api_doc.append(f"## Method: {ast_obj.name}")
    api_doc.append("")
    api_doc.append("### Callable")
    api_doc.append(f"**{ast_obj.name}**({', '.join([arg.arg for arg in ast_obj.args.args])})")
    api_doc[-1] = api_doc[-1].replace("(self,", "(*self*,")
    if ast_obj.args.args:
        api_doc.append('')
        api_doc.append('### Arguments')
        api_doc.append('')
        api_doc.append("Argument | Annotation | Type")
        api_doc.append("---|---|---")
        for arg in ast_obj.args.args:
            arg_name = arg.arg
            if arg_name == 'self':
                arg_name = '*self*'
            arg_annotation = arg.annotation
            arg_type = arg.type_comment
            if arg_annotation:
                if isinstance(arg_annotation, ast.Attribute):
                    arg_annotation = f"{arg_annotation.value.id}.{arg_annotation.attr}"
                elif isinstance(arg_annotation, ast.Name):
                    arg_annotation = arg_annotation.id
                elif isinstance(arg_annotation, ast.Subscript):
                    arg_test = parse_subscript(arg_annotation)
                    arg_annotation = parse_subscript(arg_annotation)
                elif isinstance(arg_annotation, str):
                    arg_annotation = arg_annotation
            else:
                arg_annotation = ""
            if arg_type:
                pass
            else:
                arg_type = ""
            api_doc.append(f'{arg_name}|{arg_annotation}|{arg_type}')
    api_doc.append('')
    api_doc.append('### Docstring')
    docstring = ast.get_docstring(ast_obj)
    if docstring:
        api_doc.append('')
        api_doc.append(format_docstring_to_markdown(docstring))
    else:
        api_doc.append(f"*This method does not currently have a docstring.*")
    return("\n".join(api_doc))


def _function_filter(func: ast.FunctionDef) -> bool:
    # Private and dunder funcs/methods
    if func.name.startswith("_"):
        return False
    # Getters and setters
    for decorator in func.decorator_list:
        if (isinstance(decorator, ast.Name) and decorator.id == "property") or (
            isinstance(decorator, ast.Attribute) and decorator.attr == "setter"
        ):
            return False
    return True


def write_doc_file(file_path: Path, ast_obj: Union[ast.FunctionDef, ast.ClassDef]):
    api_doc = ["---"]
    if isinstance(ast_obj, ast.FunctionDef):
        if not _function_filter(ast_obj):
            return
        else:
            if ast_obj.args.defaults:
                pass
            api_doc.append(f"title: 'Method: {ast_obj.name}'")
            doc_type = 'method'
            api_doc.append("---")
            api_doc.append("")
            api_doc.append(generate_function_documentation(ast_obj))
    else:
        api_doc.append(f"title: 'Class: {ast_obj.name}'")
        doc_type = 'class'
        api_doc.append("---")
        api_doc.append("")
        api_doc.append("## Overview")
        public_methods = [element for element in ast_obj.body if
                          isinstance(element, ast.FunctionDef) and _function_filter(element)]
        if public_methods:
            api_doc.append("")
            api_doc.append("### Methods")
            api_doc.append("")
        for public_method in public_methods:
            if len(public_method.args.args) > 1:
                api_doc.append(f"- [{public_method.name}(...)](#method-{public_method.name.lower()})")
            else:
                api_doc.append(f"- [{public_method.name}()](#method-{public_method.name.lower()})")

        api_doc.append("")
        api_doc.append("### Docstring")
        docstring = ast.get_docstring(ast_obj)
        if docstring:
            api_doc.append("")
            api_doc.append(format_docstring_to_markdown(docstring))
        else:
            api_doc.append(f"*This class does not currently have a docstring.*")

        for public_method in public_methods:
            api_doc.append("")
            api_doc.append(generate_function_documentation(public_method))
        # for body_element in ast_obj.body:
        #     if isinstance(body_element, ast.FunctionDef):
        #         if _function_filter(body_element):
        #             if len(body_element.args.args) > 1:
        #                 api_doc.append(f"[{body_element.name}(...)](#{body_element.name})")
        #             else:
        #                 api_doc.append(f"[{body_element.name}()](#{body_element.name})")
        # for body_element in ast_obj.body:
        #     if isinstance(body_element, ast.FunctionDef):
        #         if _function_filter(body_element):
        #             api_doc.append("")
        #             api_doc.append(generate_function_documentation(body_element))
    with open(file_path, 'w') as f:
        f.write("\n".join(api_doc))


def format_docstring_to_markdown(docstr: str) -> str:
    """
    Add markdown formatting to a provided docstring

    Args:
        docstr: the original docstring that needs to be converted to markdown.

    Returns:
        str of Docstring formatted as markdown

    """
    r = re.compile(r"\s\s+", re.MULTILINE)
    clean_docstr_list = []
    prev_line = None
    in_code_block = False
    in_param = False
    first_code_indentation = None

    # Parse each line to determine if it needs formatting
    for original_line in docstr.split("\n"):
        # Remove excess spaces from lines formed by concatenated docstring lines.
        line = r.sub(" ", original_line)
        line = line.replace("import", "`import`")
        # In some old docstrings, this indicates the start of an example block.
        if line.strip() == "::":
            in_code_block = True
            clean_docstr_list.append("```")

        # All of our parameter/arg/etc lists start after a line ending in ':'.
        elif line.strip().endswith(":"):
            in_param = True
            # This adds a blank line before the header if one doesn't already exist.
            if prev_line != "":
                clean_docstr_list.append("")
            # Turn the line into an H4 header
            clean_docstr_list.append(f"#### {line.strip()}")
        elif line.strip() == "" and prev_line != "::":
            # All of our parameter groups end with a line break, but we don't want to exit a parameter block due to a
            # line break in a code block.  However, some code blocks start with a blank first line, so we want to make
            # sure we aren't immediately exiting the code block (hence the test for '::' on the previous line.
            in_param = False
            # Add the markdown indicator to close a code block, since we aren't in one now.
            if in_code_block:
                clean_docstr_list.append("```")
            in_code_block = False
            first_code_indentation = None
            clean_docstr_list.append(line)
        else:
            if in_code_block:
                # Determine the number of spaces indenting the first line of code so they can be removed from all lines
                # in the code block without wrecking the hierarchical indentation levels of future lines.
                if first_code_indentation == None and line.strip() != "":
                    first_code_indentation = len(
                        re.match(r"\s*", original_line, re.UNICODE).group(0)
                    )
                if line.strip() == "" and prev_line == "::":
                    # If the first line of the code block is a blank one, just skip it.
                    pass
                else:
                    # Append the line of code, minus the extra indentation from being written in an indented docstring.
                    clean_docstr_list.append(original_line[first_code_indentation:])
            elif ":" in line.replace(":ref:", "") and in_param:
                # This indicates a parameter. arg. or other definition.
                clean_docstr_list.append(f"- {line.strip()}")
            else:
                # This indicates a regular line of text.
                clean_docstr_list.append(f"{line.strip()}")
        prev_line = line.strip()
        if clean_docstr_list[-1].startswith("- "):
            pass
        else:
            doc_parts = clean_docstr_list[-1].replace("https:", "https").split(":")
            if len(doc_parts) == 2 and doc_parts[1]:
                doc_parts = clean_docstr_list[-1].split(":", 1)
                clean_docstr_list[-1] = f"- **{doc_parts[0]}:** {doc_parts[1]} "
            else:
                pass
    clean_docstr = "\n".join(clean_docstr_list)
    clean_docstr = clean_docstr.replace("<", r"\<")
    clean_docstr = clean_docstr.replace(">", r"\>")
    return clean_docstr


if __name__ == "__main__":
    def main_run():
        for relevant_source_file in get_relevant_source_files("../great_expectations"):
            print(relevant_source_file)
            print(get_top_level_classes_and_methods(relevant_source_file))
            folder_path = get_or_create_doc_folder(relevant_source_file)
            for ast_obj in get_top_level_classes_and_methods(relevant_source_file):
                write_doc_file(get_doc_file_path(folder_path, ast_obj), ast_obj)
    main_run()