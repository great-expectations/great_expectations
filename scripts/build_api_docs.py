"""A script for generating API documents through introspection of class objects and docstring parsing.

    Usage:
    1. Run the script.
        1a. The files will be generated automatically.
    2. A series of print statements at the end of the script execution will alert you to documentation that needs to be
     updated with cross-linking (or have cross-linking removed).  Manually update these files as needed.
    3. Among the print statements will be a snippet for sidebar.js -- paste this snippet into the `items` list for the
     API documentation category under Reference, replacing the existing content of this list.
        3a. Correct the format of sidebars.js as needed (running pyCharm's 'format file' context will do).
"""
import glob
import importlib
import inspect
import pydoc
import re
import shutil
from pathlib import Path
from typing import Any, Dict
from typing import Generator as typeGenerator
from typing import List, Set, Tuple

WHITELISTED_TAG = "--Public API--"
DOCUMENTATION_TAG = "--Documentation--"
ARGS_TAG = "Args:"
RETURNS_TAG = "Returns:"
YIELDS_TAG = "Yields:"
RAISES_TAG = "Raises:"

API_DOCS_FOLDER = "/docs/api_docs/"
API_MODULES_FOLDER = f"{API_DOCS_FOLDER}modules/"
API_CLASSES_FOLDER = f"{API_DOCS_FOLDER}classes/"
API_METHODS_FOLDER = f"{API_DOCS_FOLDER}methods/"
API_CROSS_LINK_SUFFIX = "__api_links.mdx"


class BlockValueIndentationError(Exception):
    """An error that is raised when a multiline entry in an `Args:` block is not properly indented."""

    def __init__(self, block_header, block_contents, line):
        message = (
            f"The line `{line}` in the `{block_header}` block:\n"
            f"```\n"
            f"{block_contents}\n"
            f"```\n"
            f"is not indented correctly for a multiline value in an `{block_header}` block."
        )
        super().__init__(message)


class BlockFormatError(Exception):
    """An error that is raised when the first line of an `Args:` block does not start with a key: value pair."""

    def __init__(self, block_header, block_contents, line):
        message = (
            f"The line `{line}` in the `{block_header}` block:"
            f"```"
            f"{block_contents}"
            f"```"
            f"is not formatted properly for a key: value pair in an `{block_header}` block."
        )
        super().__init__(message)


def is_included_in_public_api(docstring: str) -> bool:
    """Determines if a docstring is part of the Public API

    Args:
        docstring: The docstring to check for inclusion in the Public API

    Returns:
        True if the docstring is marked as part of the Public API
        OR
        False if the docstring is not marked as being part of the Public API.

    """
    search_string = f"^ *{WHITELISTED_TAG}*$"
    return bool(re.search(search_string, docstring, re.MULTILINE))


def _escape_markdown_special_characters(string_to_escape: str) -> str:
    """Escapes the special characters used for Markdown formatting in a provided string.

    Args:
        string_to_escape: The string that needs to have Markdown special characters escaped.

    Returns:
        A string with the special characters used for Markdown formatting escaped.
    """
    for character in r"\`*_{}[]()#+-.!":
        string_to_escape = string_to_escape.replace(character, f"\\{character}")
    return string_to_escape


def _reformat_url_to_docusaurus_path(url: str) -> str:
    """Removes the site portion of a GE url.

    Args:
        url: the url including the GE site that should be converted to a Docusaurus absolute link.
    """
    return re.sub(r"^https://docs\.greatexpectations\.io", "", url).strip()


def get_document_paths(docstring: str) -> typeGenerator[str, None, None]:
    """Retrieves the Docusaurus paths of relevant docs according to the documentation urls found in a docstring.

    In a docstring, the tag:
    ```
    # Documentation  # Minus this comment which is present to prevent this from being mistaken as a documentation block.
    ```
    can be followed by urls to relevant documentation.  These are the urls that will be yielded by this generator.
    A blank line indicates the end of the `# Documentation` block.

    # Args:
        docstring: A docstring to extract docusaurus paths from.

    # Yields:
        Each url in the `# Documentation` block of the docstring, reformatted as a Docusaurus absolute path.
    """
    documentation_block = get_block_contents(DOCUMENTATION_TAG, docstring)
    if documentation_block:
        urls = [_.strip() for _ in documentation_block.split("\n")]
        for url in urls:
            yield _reformat_url_to_docusaurus_path(url)


def _get_indentation(value: str) -> int:
    """Calculates the number of leading spaces in a string.

    Args:
        value: the string to check for leading spaces.

    Returns:
        An integer indicating the number of leading spaces in the string.
    """
    return len(value) - len(value.lstrip())


def _get_dictionary_from_block_in_docstring(
    docstring: str, block_heading_text: str
) -> Dict[str, str]:
    """Builds a dictionary of key: description pairs from a block of text in a docstring.

    Note: The description portion of the key: description pair will have all non-alphanumerics that are used in Markdown
    formatting escaped in case there is content (such as a regex example) that could be mistaken as special formatting
    by Docusaurus' Markdown rendering.

    Args:
        docstring: the docstring to parse key: description pairs out of.
        block_heading_text: the heading line that indicates the start of the block that will be parsed.

    Returns:
        A dictionary of key: description pairs according to the provided docstring.

    Raises:
        ArgValueIndentationError: An error occurred due to improper indentation of a multiline description.
        ArgBlockFormatError: An error occurred due to an improperly formatted key: value in the first line of the
            specified block in the docstring.
    """
    block_contents = get_block_contents(block_heading_text, docstring)
    block_dict = {}
    if block_contents:
        arg_lines = block_contents.split("\n")
        base_indent = _get_indentation(arg_lines[0])
        key = None
        for line in arg_lines:
            if _get_indentation(line) == base_indent and re.search(
                r" *[a-z|A-Z|_|0-9]+:", line
            ):
                key, value = line.strip().split(":", 1)
                block_dict[key] = _escape_markdown_special_characters(value.strip())
            elif _get_indentation(line) > base_indent:
                block_dict[
                    key
                ] = f"{block_dict[key]} {_escape_markdown_special_characters(line.strip())}"
            elif key is None:
                raise BlockFormatError(block_heading_text, block_contents, line)
            else:
                raise BlockValueIndentationError(
                    block_heading_text, block_contents, line
                )
    return block_dict


def get_args_dictionary(docstring: str) -> dict:
    """Builds a dictionary of parameter: description pairs from a docstring's `Args:` block.

    Note: The description portion of the key: description pair will have all non-alphanumerics that are used in Markdown
    formatting escaped in case there is content (such as a regex example) that could be mistaken as special formatting
    by Docusaurus' Markdown rendering.

    Args:
        docstring: the docstring to parse parameters out of.

    Returns:
        A dictionary of parameter: description pairs according to the provided docstring.
    """
    return _get_dictionary_from_block_in_docstring(docstring, ARGS_TAG)


def get_raises_dictionary(docstring: str) -> dict:
    """Builds a dictionary of error: description pairs from a docstring's `Raises:` block.

    Note: The description portion of the key: description pair will have all non-alphanumerics that are used in Markdown
    formatting escaped in case there is content (such as a regex example) that could be mistaken as special formatting
    by Docusaurus' Markdown rendering.

    Args:
        docstring: the docstring to parse possible raised errors out of.

    Returns:
        A dictionary of error: description pairs according to the provided docstring.
    """
    return _get_dictionary_from_block_in_docstring(docstring, RAISES_TAG)


def get_block_contents(block_heading_text: str, docstring: str) -> str:
    """Gets the content of a block of information in a docstring.

    The block is considered to be everything after the `block_heading_text` up to the first blank line or the
        end of the docstring.

    Args:
        block_heading_text: The text that indicates the start of a block. ('Args:', 'Returns:', '# Documentation', etc.)
        docstring: The docstring that should be parsed for the block contents.

    Returns:
        A string containing the contents of a block if the block is found
        OR
        A blank string.
    """
    pattern = f" *{block_heading_text} *\n(.*?)(?:(?=\n *\n)|$)"
    block_contents = re.findall(pattern, docstring, re.DOTALL)
    if block_contents:
        assert len(block_contents) == 1
        block_contents = block_contents[0]
    return block_contents


def get_paragraph_block_contents(block_heading_text: str, docstring: str) -> str:
    """Retrieves the content of the specified block in a docstring.

    This retrieves the contents of the block starting with block_header in a docstring and formats it as a string
     without line breaks or extra whitespace.

    Args:
        block_heading_text: the text indicating the start of the block to return the contents of.
        docstring: the docstring to parse the block out of.


    Returns:
        Everything between the block_header line in a docstring and either a blank line or the end of the docstring.

    """
    block_contents = get_block_contents(block_heading_text, docstring)
    if block_contents:
        block_contents = re.sub("\n", " ", block_contents)
        block_contents = re.sub("  +", " ", block_contents)
    else:
        block_contents = ""
    return block_contents.strip()


def get_yield_or_return_block(docstring: str) -> Tuple[str, str]:
    """Retrieves the content of the `Yields:` or the `Returns:` block in a docstring, if one of those blocks exists.

    The retrieved block's contents will be reformatted as a string without line breaks or extra whitespace.

    Args:
        docstring: the docstring to parse the `Yields` or `Returns` block out of.

    Returns:
        A tuple consisting of two elements: the heading for the found block and the contents of the found block
        OR
        None if neither a `Yields:` block nor a `Returns:` block is found.
    """

    for block_header in (RETURNS_TAG, YIELDS_TAG):
        block_content = get_paragraph_block_contents(block_header, docstring)
        if block_content:
            return block_header, block_content


def remove_block(docstring: str, block_heading: str) -> str:
    """Removes the heading and contents of a block (`Args:`, `Returns:`, etc.) in a docstring.

    This method evaluates a "block" as everything between the `block_heading` and either the first blank line
    or the end of the docstring.

    Args:
        docstring: The docstring that the block should be removed from.
        block_heading: The text that indicates the start of a block.

    Returns:
        The original docstring, minus the removed block and it's heading.
    """
    heading_pattern = f"( *{block_heading} *\n)"
    heading_content = re.findall(heading_pattern, docstring, re.DOTALL)
    block_content = get_block_contents(block_heading, docstring)
    if heading_content and block_content:
        assert len(heading_content) == 1
        heading_content = heading_content[0]
        docstring = docstring.replace(heading_content, "")
        docstring = docstring.replace(block_content, "")
    return docstring


def condense_whitespace(docstring: str) -> str:
    """Collapses excess whitespace in a string.

    This will reduce any spaces in excess of 1 down to a single space and will strip excess leading spaces.  It will
    also collapse any newlines in excess of 2 down to 2 newlines (so single returns and blank lines are preserved,
    but multiple blank lines will be collapsed into a single blank line).

    Args:
        docstring: the docstring that needs to have excess whitespace condensed.

    Returns:
        A copy of the provided docstring with the excess whitespace condensed.
    """
    docstring = re.sub("  *", " ", docstring, flags=re.M)
    docstring = re.sub("^ ", "", docstring, flags=re.M)
    docstring = re.sub("\n\n\n+", "\n\n", docstring, flags=re.M)
    return docstring


def build_relevant_api_reference_files(
    docstring: str, api_doc_id: str, api_doc_path: str
) -> Set[str]:
    """Builds importable link snippets according to the contents of a docstring's `# Documentation` block.

    This method will create files if they do not exist, and will append links to the files that already do exist.

    Args:
        docstring: the docstring that contains the `# Documentation` block listing urls to be cross-linked.
        api_doc_id: A string representation of the API doc that will have the link applied to it.
        api_doc_path: a Docusaurus compliant path to the API document.

    Returns:
        A set containing the file paths that were created or appended to.
    """
    output_paths = set()
    document_paths = get_document_paths(docstring)
    for relevant_path in document_paths:
        links_path = Path(f"..{relevant_path}__api_links.mdx")
        output_paths.add(links_path)
        if links_path.exists():
            with open(links_path, "a") as f:
                f.write(f"- [{api_doc_id}]({api_doc_path})\n")
        else:
            with open(links_path, "w") as f:
                f.write(f"- [{api_doc_id}]({api_doc_path})\n")
    return output_paths


def get_whitelisted_methods(
    imported_class: object,
) -> typeGenerator[Tuple[str, object], None, None]:
    """Provides the method_name and a reference to the method itself for every Public API method in a class.

    Args:
        imported_class: the class object that needs to be checked for Public API methods.

    Yields:
        A tuple consisting of the name of a Public API method and a reference to the method itself.
    """
    methods = dict(inspect.getmembers(imported_class, inspect.isroutine))
    for method_name, class_method in methods.items():
        synopsis, desc = pydoc.splitdoc(pydoc.getdoc(class_method))
        if is_included_in_public_api(desc):
            yield method_name, class_method


def parse_method_signature(
    signature: inspect.Signature, param_dict: Dict[str, str]
) -> List[List[str]]:
    """Combines Signature with the contents of a param_dict to create rows for a parameter table.

    Args:
        signature: the output from running `inspect.signature` on a method
        param_dict: dictionary of parameter: parameter description key: values for the inspected method.

    Returns:
        A list containing lists of strings, each of which represents one row in a parameter's table.  The rows are in
        the format [parameter name, parameter typing, parameter default value, parameter description].

    """
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
        sig_results.append(
            [sig_parameter, sig_type, sig_default, param_dict.get(sig_parameter, "")]
        )
    return sig_results


def get_title(file_path: Path) -> str:
    """Gets the title value in a Docusaurus Head Metadata block.

    Args:
        file_path: The path to the file to parse.

    Returns:
        The title value that is found in the file's contents
        OR
        None if no title value is found or no Head Metadata is found.

    """
    with open(file_path) as f:
        data = f.read()
    if "---" in data:
        prefix, title_block = data.split("---", 1)
        title_block, suffix = title_block.split("---", 1)
        for line in title_block.split("\n"):
            if line.strip().startswith("title:"):
                title = line.split(":", 1)[1]
                title = title.strip().strip("'").strip('"')
                return title


def build_relevant_documentation_block(docstring: str) -> List[str]:
    """Builds a list of links to documentation listed in the '--Documentation--' block of a docstring.

    Args:
        docstring: The docstring to parse documentation links out of.

    Returns:
        A list of strings, each of which is formatted as a Markdown unordered list entry consisting of a link to
        the corresponding document in the '--Documentation--' block.
    """
    relevant_documentation_block = []
    for path in get_document_paths(docstring):
        relative_path = Path(f"..{path}.md")
        title = None
        if relative_path.exists():
            title = get_title(relative_path)
        relevant_documentation_block.append(f"- [{title or path}]({path})")
    return relevant_documentation_block


def build_method_document(
    method_name: str, method: Any, qualified_path: str, github_path: str
) -> Tuple[str, Set[str], str]:
    """Create API documentation for a given method.

    This method both creates the content for the API documentation and also writes it to the corresponding file.  It
    also creates importable snippet files for cross-linking the API document with the regular documentation according
    to the paths specified in the docstrings `# Documentation` block.

    NOTE: Due to the possibility of methods being inherited from super classes in different files than the
     `imported_class`, the `github_path` for methods is not yet included in the content generated for a method API doc.
     the arg `github_path` is a placeholder for when that functionality is included.

    Args:
        method_name: The name of the method as it would appear when being called.
        method: The method object to be documented.
        qualified_path: The fully qualified path to the class or module that the method resides in.
        github_path: The url to the develop branch GitHub page that corresponds to the import_path.

    Returns:
        A tuple consisting of a one line string that describes the method and links its name to the created API doc
        AND
        a set consisting of all the cross-linking snippets that were created or edited by this method.
        AND
        a string representing the path to use for the file in the generated Sidebar update.
    """
    title = f"{qualified_path.rsplit('.', 1)[-1]}.{method_name}"
    method_docstring = inspect.getdoc(method)
    if method_docstring:
        synopsis, docstring = pydoc.splitdoc(method_docstring)
    else:
        synopsis = docstring = ""

    # Add Title, Link back to Class, and Fully qualified path to document content.
    in_progress_output = [
        "---",
        f"title: {title}",
        "---",
        f"[Back to class documentation](/docs/api_docs/classes/{qualified_path.replace('.', '-')})",
        "",
        "### Fully qualified path",
        "",
        f"`{qualified_path}.{method_name}`",
    ]

    # Add synopsis to document content.
    if synopsis:
        in_progress_output.extend(["", "### Synopsis", "", synopsis])

    # Add docstring body (docstring contents minus blocks) to document content, if populated in docstring.
    if method_docstring:
        pretty_docstring = prettify_docstring(docstring)
        if pretty_docstring.strip():
            in_progress_output.extend(["", pretty_docstring])

    # Add parameter table to document content.
    signature = inspect.signature(method)
    param_dict = get_args_dictionary(docstring)
    arg_table = parse_method_signature(signature, param_dict)
    if arg_table:
        in_progress_output.extend(
            [
                "### Parameters",
                "",
                "Parameter|Typing|Default|Description",
                "---------|------|-------|-----------",
            ]
        )
        described_lines = []
        for line in arg_table:
            described_lines.append([str(_) for _ in line])
            described_lines[-1].append(param_dict.get(described_lines[-1][0], ""))
        for line in described_lines:
            in_progress_output.append("|".join([str(_) for _ in line]))

    # Add return/yield block to document content, if found in docstring.
    return_or_yield_content = get_yield_or_return_block(docstring)
    if return_or_yield_content:
        return_or_yield, content = return_or_yield_content
        in_progress_output.extend(
            ["", f"### {return_or_yield.replace(':', '')}", "", content]
        )

    # Add raises block to document content, if found in docstring.
    raises_dict = get_raises_dictionary(docstring)
    if raises_dict:
        in_progress_output.extend(["", "### Raises:", ""])
        for key in sorted(raises_dict.keys()):
            in_progress_output.append(f"- *{key}:* {raises_dict[key]}")

    # Add links to relevant documentation.
    relevant_documentation = build_relevant_documentation_block(docstring)
    if relevant_documentation:
        in_progress_output.extend(["", "## Relevant documentation (links)", ""])
        in_progress_output.extend(relevant_documentation)

    # Create importable cross-linking snippets for documents in `# Document` block.
    output_file = f"{qualified_path}.{method_name}".replace(".", "-")
    crosslink_snippets = build_relevant_api_reference_files(
        docstring, title, f"{API_METHODS_FOLDER}{output_file}"
    )

    # Write document contents to file.
    file_path = f"../docs/api_docs/methods/{output_file}"
    with open(f"{file_path}.md", "w") as f:
        output = condense_whitespace("\n".join(in_progress_output))
        f.write(output)

    # Generate abbreviated line describing the method and linking to its API documentation.
    abbreviated_output = (
        f"*[.{method_name}(...):]({API_METHODS_FOLDER}{output_file})* {synopsis}"
    )
    sidebar_id = file_path.replace("../docs/", "")

    return abbreviated_output, crosslink_snippets, sidebar_id


def build_class_document(
    class_name: str, imported_class: object, import_path: str, github_path: str
) -> Tuple[Set[str], str]:
    """Create API documentation for a given class and its methods that are part of the Public API.

    This method both creates the content for the API documentation and also writes it to the corresponding file.

    Args:
        class_name: The name of the class as it would appear in an import statement.
        imported_class: The class object to be documented.
        import_path: The fully qualified path to the module that the class resides in.
        github_path: The url to the develop branch GitHub page that corresponds to the import_path.

    Returns:
        A tuple consisting of a set containing all the paths of the cross-linking snippets that were created for this
        class and its Public API methods
        AND
        a string representing the sidebar entry for the created documentation. (Category containing a class overview
        page and public method pages.)
    """

    qualified_class_path = f"{import_path}.{class_name}"
    description = pydoc.describe(imported_class)
    class_docstring = inspect.getdoc(imported_class)
    synopsis, docstring = pydoc.splitdoc(class_docstring)
    output_file = f"{qualified_class_path}".replace(".", "-")
    build_relevant_api_reference_files(
        docstring, f"class {class_name}", f"{API_CLASSES_FOLDER}{output_file}"
    )

    # Add title block and GitHub link to document content.
    in_progress_output = [
        "---",
        f"title: {description}",
        "---",
        f"# {class_name}" "",
        f"[See it on GitHub]({github_path})",
    ]

    # Add synopsis line and docstring body (docstring minus `Args:`, `Returns:`, etc blocks) to document content.
    if synopsis or class_docstring:
        in_progress_output.extend(["", "## Synopsis"])
    if synopsis:
        in_progress_output.extend(["", synopsis])
    if class_docstring:
        in_progress_output.extend(["", prettify_docstring(docstring)])

    # Add import statement to document content.
    in_progress_output.extend(
        [
            "## Import statement",
            "",
            "```python",
            f"from {import_path} import {class_name}",
            "```",
            "",
        ]
    )

    # Build API documentation for the class's Public API methods and populate the sidebar update.
    whitelisted_methods = get_whitelisted_methods(imported_class)
    in_progress_methods = []
    all_edited_cross_link_files: Set[str] = set()
    sidebar_method_items = []
    for method_name, whitelisted_method in whitelisted_methods:
        abbreviated_method, cross_links, sidebar_id = build_method_document(
            method_name, whitelisted_method, qualified_class_path, github_path
        )
        in_progress_methods.append(f"- {abbreviated_method}")
        all_edited_cross_link_files.update(cross_links)
        sidebar_method_items.append(
            f"{{ label: '  .{method_name}(...)', type: 'doc', id: '{sidebar_id}' }}"
        )

    # Add class methods in the Public API to the document.
    if in_progress_methods:
        in_progress_output.extend(
            ["", "## Public Methods (API documentation links)", ""]
        )
        in_progress_output.extend(in_progress_methods)

    # Add links to relevant documentation to the document.
    relevant_documentation = build_relevant_documentation_block(docstring)
    if relevant_documentation:
        in_progress_output.extend(["", "## Relevant documentation (links)", ""])
        in_progress_output.extend(relevant_documentation)

    # Write document content to file.
    file_path = f"..{API_CLASSES_FOLDER}{output_file}.md"
    with open(file_path, "w") as f:
        f.write("\n".join(in_progress_output))

    # Build the sidebar category entry for this class.
    sidebar_class_id = f"{API_CLASSES_FOLDER}{output_file}".replace("/docs/", "")
    sidebar_entry = [
        f"{{ type: 'category',\n label: 'Class {class_name}',\n link: {{type: 'doc', id: '{sidebar_class_id}'}},\n items: ["
    ]
    sidebar_items = [
        # f"{{ type: 'doc', label: '{class_name} (Overview)', id: '{sidebar_class_id}' }}"
    ]
    sidebar_items.extend(sorted(sidebar_method_items))
    sidebar_entry.append(",\n".join(sidebar_items))
    sidebar_entry.append("]}")
    sidebar_entry = "\n".join(sidebar_entry)

    return all_edited_cross_link_files, sidebar_entry


def prettify_docstring(docstring: str) -> str:
    """Clean up the content of a docstring.

    This removes any information blocks ('Returns:', 'Args', '# Documentation', etc.) that will be displayed through
    custom rendering, and collapses any excess whitespace in the provided docstring.

    Args:
        docstring: the docstring to clean up.

    Returns:
        A cleaned up version of the provided docstring.
    """
    new_content = docstring.replace(WHITELISTED_TAG, "")
    for tag in (ARGS_TAG, RETURNS_TAG, YIELDS_TAG, RAISES_TAG, DOCUMENTATION_TAG):
        new_content = remove_block(new_content, tag)
    new_content = condense_whitespace(new_content)
    return new_content


def remove_existing_api_reference_files(directory_path: Path) -> List[Path]:
    """Remove any files in the file tree for `directory_path` that end in the value of API_CROSS_LINK_SUFFIX.

    Args:
        directory_path: the root directory of the file tree to traverse and remove existing api cross-link files from.

    Returns:
        A list of paths to files that have been removed because they end in the API_CROSS_LINK_SUFFIX.
    """
    paths = Path(directory_path).rglob(f"*{API_CROSS_LINK_SUFFIX}")
    output_paths = []
    for api_links_file in paths:
        api_links_file.unlink()
        output_paths.append(api_links_file)
    return output_paths


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
    with open(file_path) as open_file:
        if WHITELISTED_TAG in open_file.read():
            return True
        else:
            return False


def convert_to_import_path(file_path: Path) -> str:
    """Convert a file path into an import statement.

    Args:
        file_path: the path to the module that you want the import path for.

    Returns:
        An import statement that corresponds to the module at `file_path`

    """
    import_path = str(file_path).replace("../", "")
    import_path = import_path.replace("/", ".")
    import_path = import_path.replace(".py", "")
    return import_path


def gather_classes_to_document(
    import_path: str,
) -> typeGenerator[Tuple[str, object], None, None]:
    """Get all the class names and object references for Public API classes in a given module.

    Args:
        import_path: the import statement for the module to parse.

    Yields:
        A tuple consisting of the class name and a reference for the class itself for all Public API classes in the
        provided module.
    """
    module = importlib.import_module(import_path)
    imported_classes = dict(inspect.getmembers(module, inspect.isclass))
    for class_name, imported_class in imported_classes.items():
        synopsis, desc = pydoc.splitdoc(pydoc.getdoc(imported_class))
        if is_included_in_public_api(desc):
            yield class_name, imported_class


if __name__ == "__main__":

    def main_func() -> None:
        """Generate all the API docs according to the contents of class and method docstrings.

        This exists to keep the main process of this script from cluttering the global name space.

        Returns:
            None
        """
        sidebar_entry = []
        all_snippet_paths = set()
        # Remove the existing documentation.
        shutil.rmtree(f"..{API_DOCS_FOLDER}")
        Path(f"..{API_MODULES_FOLDER}").mkdir(parents=True, exist_ok=True)
        Path(f"..{API_CLASSES_FOLDER}").mkdir(parents=True, exist_ok=True)
        Path(f"..{API_METHODS_FOLDER}").mkdir(parents=True, exist_ok=True)
        removed_cross_link_files = remove_existing_api_reference_files(Path("../docs"))

        # Generate the new documentation.
        for source_file_path in get_relevant_source_files("../great_expectations"):
            github_path = (
                f"https://github.com/great-expectations/great_expectations/blob/develop"
                f"{str(source_file_path).replace('..', '')}"
            )
            if check_file_for_whitelisted_elements(source_file_path):
                print(source_file_path)
                import_path = convert_to_import_path(source_file_path)
                whitelisted_classes = gather_classes_to_document(import_path)
                print(whitelisted_classes)
                for class_name, whitelisted_class in whitelisted_classes:
                    print(class_name)
                    snippet_path_update, sidebar_update = build_class_document(
                        class_name, whitelisted_class, import_path, github_path
                    )
                    all_snippet_paths.update(snippet_path_update)
                    sidebar_entry.append(sidebar_update)

        # Print the report for documentation to update with cross-links, and the content to paste into the sidebar.
        print(
            "\n\nThe following is the sidebar content for the generated API docs.  Simply paste this section into"
            "\n`sidebars.js` and run the auto-format fix indentations context on the file.\n"
        )
        print(",\n".join(sidebar_entry))
        print(
            "\n\nThe following are the importable snippet files that were generated for cross-linking.  Please verify"
            "\nthat the corresponding documents are importing these files. (This check will be automated at some"
            "\npoint and then this list will only include those that have not been imported into the standard"
            "\ndocumentation.)\n"
        )
        for _ in all_snippet_paths:
            print(_)

        cross_links_to_remove_from_docs = set(removed_cross_link_files).difference(
            all_snippet_paths
        )
        if cross_links_to_remove_from_docs:
            print(
                "\n\nThe following are the importable snippet files that were removed at the start of the script and"
                "\nwere not replaced by a new importable snippet.  Please ensure that import statements for these"
                "\nfiles are removed from the corresponding documentation."
            )
            for _ in cross_links_to_remove_from_docs:
                print(_)
        else:
            print(
                "\n\nAll of the importable snippet files that were removed at the start of the script were replaced"
                "\nby new snippets; they should already be imported in the relevant documentation."
            )

    main_func()
