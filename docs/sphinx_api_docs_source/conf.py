# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
from __future__ import annotations

# -- Update syspath
import os
import sys

WHITELISTED_TAG = "--Public API--"


def _prepend_base_repository_dir_to_sys_path():
    """Add great_expectations base repo dir to the front of sys path. Used for docs processing."""
    sys.path.insert(0, os.path.abspath("../../"))


_prepend_base_repository_dir_to_sys_path()


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "great_expectations"
copyright = "2023, The Great Expectations Team"
author = "The Great Expectations Team"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "README.md"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = []


# Skip autodoc unless part of the Public API
DOCUMENTATION_TAGS = ["---Documentation---", "--Documentation--"]


def skip_if_not_whitelisted(app, what, name, obj, would_skip, options):
    """Skip rendering documentation for docstrings that are empty or not whitelisted.

    Whitelisted docstrings contain the WHITELISTED_TAG.
    """
    if obj.__doc__ is not None and WHITELISTED_TAG in obj.__doc__:
        return False
    return True


def custom_process_docstring(app, what, name, obj, options, lines):
    """Custom processing for use during docstring processing."""
    _remove_whitelist_tag(
        app=app, what=what, name=name, obj=obj, options=options, lines=lines
    )
    _process_relevant_documentation_tag(
        app=app, what=what, name=name, obj=obj, options=options, lines=lines
    )
    _remove_feature_maturity_info(
        app=app, what=what, name=name, obj=obj, options=options, lines=lines
    )
    _convert_code_snippets_to_docusaurus(
        app=app, what=what, name=name, obj=obj, options=options, lines=lines
    )


def _remove_whitelist_tag(app, what, name, obj, options, lines):
    """Remove the whitelisted tag from documentation before rendering.

    Note: This method modifies lines in place per sphinx documentation.
    """
    for idx, line in enumerate(lines):
        if WHITELISTED_TAG in line:
            trimmed_line = line.replace(WHITELISTED_TAG, "")
            lines[idx] = trimmed_line


def _process_relevant_documentation_tag(app, what, name, obj, options, lines):
    """Remove and replace documentation tag from documentation before rendering.

    Note: This method modifies lines in place per sphinx documentation.
    """
    for idx, line in enumerate(lines):
        for DOCUMENTATION_TAG in DOCUMENTATION_TAGS:
            if DOCUMENTATION_TAG in line:
                trimmed_line = line.replace(
                    DOCUMENTATION_TAG, "Relevant Documentation Links\n"
                )
                lines[idx] = trimmed_line


FEATURE_MATURITY_INFO_TAG = "--ge-feature-maturity-info--"


def _remove_feature_maturity_info(app, what, name, obj, options, lines):
    """Remove feature maturity info if there are starting and ending tags.

    Note: This method modifies lines in place per sphinx documentation.
    """
    feature_maturity_info_start = None
    feature_maturity_info_end = None
    for idx, line in enumerate(lines):
        if FEATURE_MATURITY_INFO_TAG in line:
            if feature_maturity_info_start is None:
                feature_maturity_info_start = idx
            else:
                feature_maturity_info_end = idx

    if feature_maturity_info_start and feature_maturity_info_end:
        del lines[feature_maturity_info_start : feature_maturity_info_end + 1]


def _convert_code_snippets_to_docusaurus(app, what, name, obj, options, lines):
    """Convert code snippets to docusaurus style using CodeBlock component.

    Code snippets
    ```yaml
    my_yaml:
      - code_snippet
    ```

    Note:
    1. There must be an opening and closing set of triple backticks.
    2. The opening backticks can have an optional language (see docusaurus for supported languages).
    """
    convert_code_blocks(lines=lines, name=name)


def setup(app):
    app.connect("autodoc-skip-member", skip_if_not_whitelisted)
    app.connect("autodoc-process-docstring", custom_process_docstring)


def convert_code_blocks(lines: list[str], name: str) -> None:
    """Convert code blocks to CodeBlock components.

    Modify lines in place to match Sphinx functionality.

    Args:
        lines: Lines in the docstring.
        name: Name of the entity whose docstring we are processing.
    """
    code_snippet_start: None | int = None
    code_snippet_end: None | int = None
    num_triple_quotes: int = 0

    # Find number of code snippets
    code_snippet_indices: list[tuple[int, int]] = []
    for idx, line in enumerate(lines):
        if "```" in line:
            num_triple_quotes += 1
            if not code_snippet_start:
                code_snippet_start = idx
            elif not code_snippet_end:
                code_snippet_end = idx
                code_snippet_indices.append((code_snippet_start, code_snippet_end))

            else:
                code_snippet_start = idx
                code_snippet_end = None

    if not num_triple_quotes % 2 == 0:
        raise ValueError(f"Triple quotes for code blocks in {name} must be matched.")

    # Replace code snippets with CodeBlock components
    for _ in range(len(code_snippet_indices)):
        code_snippet_start = None
        code_snippet_end = None

        for idx, line in enumerate(lines):
            if line.strip().startswith("```"):
                if not code_snippet_start:
                    code_snippet_start = idx
                elif not code_snippet_end:
                    code_snippet_end = idx

                    # Create and replace snippet with CodeBlock
                    language = lines[code_snippet_start].replace("```", "").strip()
                    content = lines[code_snippet_start + 1 : code_snippet_end]
                    stub = '<CodeBlock language="{language}">{{`{content}`}}</CodeBlock>'.format(
                        language=language, content="{" + "\n".join(content) + "}"
                    )
                    lines[code_snippet_start] = stub
                    del lines[code_snippet_start + 1 : code_snippet_end + 1]
                    break
