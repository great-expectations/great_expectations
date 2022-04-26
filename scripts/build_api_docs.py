import pydoc
import re
import ast
import glob
from pathlib import Path
from typing import Dict, List, Union
import importlib
import inspect


PUBLIC_API_WHITELISTED = "--Public API Whitelisted--"


def _gather_source_files(directory_path: str) -> List[Path]:
    """Returns a list of all python files in the tree starting from the provided directory_path

    Args:
        directory_path: the root directory of a python project.

    Returns:
        A list of python file paths.
    """
    return [Path(_) for _ in glob.glob(f"{directory_path}/**/*.py", recursive=True)]


def _filter_source_files(file_paths: List[Path]) -> List[Path]:
    """Filters out paths to files starting with an underscore from a list of paths.

    Args:
        file_paths: A list of file paths.

    Returns:
        A filtered list of file paths.

    """
    return [_ for _ in file_paths if not _.name.startswith("_")]


def get_relevant_source_files(directory_path: str) -> List[Path]:
    """Retrieves filepaths to all public python files in a directory tree.

    Args:
        directory_path: root folder of the directory tree to gather paths from.

    Returns:
        A list of paths to public python files.

    """
    return _filter_source_files(_gather_source_files(directory_path))


def check_file_for_whitelisted_elements(file_path: Path) -> bool:
    """Indicates if a file contains whitelisted classes or methods.

    Args:
        file_path: path to the file that should be checked.

    Returns:
        True if the file contains the tag "--Public API Whitelisted--" OR False if the file does not.

    """
    with open(file_path, 'r') as open_file:
        if PUBLIC_API_WHITELISTED in open_file.read():
            return True
        else:
            return False


def convert_to_import_path(file_path: Path) -> str:
    import_path = str(file_path).replace("../", "")
    import_path = import_path.replace("/", ".")
    import_path = import_path.replace(".py", "")
    return import_path


def gather_classes_to_document(import_path):
    module = importlib.import_module(import_path)
    imported_classes = dict(inspect.getmembers(module, inspect.isclass))
    for class_name, imported_class in imported_classes.items():
        synop, desc = pydoc.splitdoc(pydoc.getdoc(imported_class))
        if PUBLIC_API_WHITELISTED in desc:
            yield class_name, imported_class


def gather_whitelisted_methods(imported_class):
    methods = dict(inspect.getmembers(imported_class, inspect.isroutine))
    for method_name, class_method in methods.items():
        synop, desc = pydoc.splitdoc(pydoc.getdoc(class_method))
        if PUBLIC_API_WHITELISTED in desc:
            print(method_name, class_method)
            yield method_name, class_method


def parse_signature(signature, param_dict):
    sig_type = ""
    sig_default = ""
    sig_results = []
    for key, value in signature.parameters.items():
        value = str(value)
        if ":" in value:
            sig_parameter, remainder = value.split(":", 1)
            if "=" in remainder:
                sig_type, sig_default = remainder.rsplit("=", 1)
            else:
                sig_type = remainder
        elif "=" in value:
            sig_parameter, sig_default = value.split("=", 1)
        else:
            sig_parameter = value
        sig_results.append([sig_parameter, sig_type, sig_default, param_dict.get(sig_parameter, '')])
    return sig_results


def parse_args(docstring):
    arg_dict = {}
    for line in docstring.split("\n"):
        if ":" in line:
            key, value = line.strip().split(":", 1)
            key = key.strip("- *")
            value = value.strip("- *")
            arg_dict[key] = value
    return arg_dict


def build_method_document(method_name, whitelisted_method, class_path, github_path):
    description = pydoc.describe(whitelisted_method)
    title = f"{class_path.rsplit('.', 1)[-1]}.{method_name}"
    method_docstring = inspect.getdoc(whitelisted_method)
    if method_docstring:
        synopsis, docstring = pydoc.splitdoc(method_docstring)
    else:
        synopsis = docstring = ""


    description = description.capitalize()
    inprogress_output = ["---",
                         f"title: {title}",
                         "---",
                         f"[Back to class documentation](/docs/api_docs/classes/{class_path.replace('.', '-')})",
                         "",
                         f"## {description}",
                         "",
                         "### Fully qualified path",
                         "",
                         f"`{class_path}.{method_name}`",
                         "",
                         f"[See it on GitHub]({github_path})"]

    if synopsis:
        inprogress_output.extend(["",
                                  "### Synopsis",
                                  "",
                                  synopsis])

    signature = inspect.signature(whitelisted_method)
    pretty_docstring = prettify_docstring(docstring)
    param_dict = parse_args(pretty_docstring)
    arg_table = parse_signature(signature, param_dict)
    if arg_table:
        inprogress_output.extend(['### Parameters',
                                  "",
                                  'Parameter|Typing|Default|Description',
                                  '---------|------|-------|-----------'])
        described_lines = []
        for line in arg_table:
            described_lines.append([str(_) for _ in line])
            described_lines[-1].append(param_dict.get(described_lines[-1][0], ""))
        for line in described_lines:
            inprogress_output.append("|".join([str(_) for _ in line]))

    if method_docstring:
        inprogress_output.extend(["",
                                  "### Docstring",
                                  "",
                                  pretty_docstring])
    output_file = f"{class_path}.{method_name}".replace(".", "-")
    file_path = f'../docs/api_docs/methods/{output_file}'

    with open(f"{file_path}.md", 'w') as f:
        output = "\n".join(inprogress_output)
        while "\n\n\n" in output:
            output = output.replace("\n\n\n", "\n\n")
        f.write(output)

    abbreviated_output = ["",
                          f"**[.{method_name}(...):](/docs/api_docs/methods/{output_file})** {synopsis}",
                          ]
    return abbreviated_output


def build_class_document(class_name, imported_class, import_path, github_path):
    qualified_class_path = f'{import_path}.{class_name}'
    description = pydoc.describe(imported_class)
    class_docstring = inspect.getdoc(imported_class)
    synopsis, docstring = pydoc.splitdoc(class_docstring)

    inprogress_output = ["---",
                         f"title: {description}",
                         "---",
                         "### Import statement",
                         "",
                         "```python",
                         f"from {import_path} import {class_name}",
                         "```",
                         "",
                         f"[See it on GitHub]({github_path})"
                         ]

    if synopsis:
        inprogress_output.extend(["",
                                  "### Synopsis",
                                  "",
                                  synopsis])

    if class_docstring:
        inprogress_output.extend(["",
                                  "### Docstring",
                                  "",
                                  prettify_docstring(docstring)])

    whitelisted_methods = gather_whitelisted_methods(imported_class)
    inprogress_methods = []
    for method_name, whitelisted_method in whitelisted_methods:
        inprogress_methods.append("")
        inprogress_methods.extend(build_method_document(method_name, whitelisted_method, qualified_class_path,
                                                        github_path))

    if inprogress_methods:
        inprogress_output.extend(["",
                                  "### Public Methods (API documentation links)",
                                  ""])
        inprogress_output.extend(inprogress_methods)

    output = "\n".join(inprogress_output)
    while "\n\n\n" in output:
        output = output.replace("\n\n\n", "\n\n")

    output_file = f"{qualified_class_path}".replace(".", "-")
    file_path = f'../docs/api_docs/classes/{output_file}.md'

    with open(file_path, 'w') as f:
        f.write("\n".join(inprogress_output))


def prettify_args(docstring):
    in_args = False
    last_param = last_desc = ""
    new_string = []

    for line in docstring.split("\n"):
        if line.strip() in ("Args:", "Returns:", "Raises:"):
            if last_param and last_desc:
                new_string.append(f"- **{last_param}:** {last_desc}")
                last_param = last_desc = ""
            in_args = True
            new_string.append(line)
            continue
        if in_args:
            if line.strip():
                if ":" in line:
                    if last_param and last_desc:
                        new_string.append(f"- **{last_param}:** {last_desc}")
                    elif last_desc:
                        new_string.append(f"- {last_desc}")
                    last_param, last_desc = line.strip().split(":")
                else:
                    last_desc = f"{last_desc} {line.strip()}"
            else:
                in_args = False
                new_string.append(line)
        else:
            if last_param and last_desc:
                new_string.append(f"- **{last_param}:** {last_desc}")
                last_param = last_desc = ""
            elif last_desc:
                new_string.append(f"- {last_desc}")
                last_desc = ""
            else:
                new_string.append(line)
    if last_param and last_desc:
        new_string.append(f"- **{last_param}:** {last_desc}")
    elif last_desc:
        new_string.append(f"- {last_desc}")
    return "\n".join(new_string)


def prettify_relevant_documentation_paths(docstring):
    if '--Relevant Documentation--' in docstring:
        prefix, paths = docstring.split("--Relevant Documentation--", 1)
        paths, suffix = paths.split("--Relevant Documentation--", 1)
        if paths:
            docstring_constructor = [prefix,
                                     "",
                                     suffix,
                                     "",
                                     "### Related Documentation"]
            for line in paths.split("\n"):
                line = line.strip()
                if line:
                    docstring_constructor.append(f"- [{line.replace('@', '')}]({line})")
            docstring = "\n".join(docstring_constructor)
            print(docstring_constructor)
    return docstring


def prettify_docstring(docstring):
    docstring = prettify_args(docstring)
    for section in ("Args:", "Returns:", "Raises:"):
        docstring = docstring.replace(section, f"\n**{section}**\n")
    docstring = docstring.replace(PUBLIC_API_WHITELISTED, "")
    docstring = prettify_relevant_documentation_paths(docstring)
    return docstring



if __name__ == '__main__':
    def main_func():
        for source_file_path in (get_relevant_source_files("../great_expectations")):
            print("++")
            print(source_file_path)
            github_path = f"https://github.com/great-expectations/great_expectations/blob/develop{str(source_file_path).replace('..', '')}"
            if check_file_for_whitelisted_elements(source_file_path):
                import_path = convert_to_import_path(source_file_path)
                whitelisted_classes = gather_classes_to_document(import_path)
                for class_name, whitelisted_class in whitelisted_classes:
                    build_class_document(class_name, whitelisted_class, import_path, github_path)
    main_func()

    test = """
    Some Stuff Here
    Args:
        project_root_dir: path to the root directory in which to create a new great_expectations directory
        usage_statistics_enabled: boolean directive specifying whether or not to gather usage statistics
        runtime_environment: a dictionary of config variables that
        override both those set in config_variables.yml and the environment

    Returns:
        Nothing Much
    """
    print(prettify_docstring(test))