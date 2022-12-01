# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


# -- Update syspath
import os
import sys

from great_expectations.core._docs_decorators import WHITELISTED_TAG


def _prepend_base_repository_dir_to_sys_path():
    """Add great_expectations base repo dir to the front of sys path. Used for docs processing."""
    sys.path.insert(0, os.path.abspath("../../"))


_prepend_base_repository_dir_to_sys_path()

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "great_expectations"
copyright = "2022, The Great Expectations Team"
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
html_static_path = ["_static"]


# Skip autodoc unless part of the Public API
DOCUMENTATION_TAG = "--Documentation--"


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


def setup(app):
    app.connect("autodoc-skip-member", skip_if_not_whitelisted)
    app.connect("autodoc-process-docstring", custom_process_docstring)
