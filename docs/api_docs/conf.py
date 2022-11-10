# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


# -- Update syspath
import os
import sys


def _prepend_base_repository_dir_to_sys_path():
    """Add great_expectations base repo dir to the front of sys path. Used for docs processing."""
    sys.path.insert(0, os.path.abspath("../../"))


_prepend_base_repository_dir_to_sys_path()

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'great_expectations'
copyright = '2022, The Great Expectations Team'
author = 'The Great Expectations Team'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']


# Skip autodoc unless part of the Public API
WHITELISTED_TAG = "--Public API--"
def skip(app, what, name, obj, would_skip, options):
    if WHITELISTED_TAG in obj.__doc__:
        # breakpoint()
        return False
    return True

def setup(app):
    app.connect("autodoc-skip-member", skip)
