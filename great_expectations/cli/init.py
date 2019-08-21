# -*- coding: utf-8 -*-

import os
import glob
import shutil

from great_expectations.data_context.util import safe_mmkdir
from great_expectations import __version__ as __version__


def script_relative_path(file_path):
    '''
    Useful for testing with local files. Use a path relative to where the
    test resides and this function will return the absolute path
    of that file. Otherwise it will be relative to script that
    ran the test

    Note this is expensive performance wise so if you are calling this many
    times you may want to call it once and cache the base dir.
    '''
    # from http://bit.ly/2snyC6s

    import inspect
    scriptdir = inspect.stack()[1][1]
    return os.path.join(os.path.dirname(os.path.abspath(scriptdir)), file_path)


def scaffold_directories_and_notebooks(base_dir):
    """Add basic directories for an initial, opinionated GE project."""

    safe_mmkdir(base_dir, exist_ok=True)
    notebook_dir_name = "notebooks"

    open(os.path.join(base_dir, ".gitignore"), 'w').write("uncommitted/")

    for directory in [notebook_dir_name, "expectations", "datasources", "uncommitted", "plugins", "fixtures"]:
        safe_mmkdir(os.path.join(base_dir, directory), exist_ok=True)

    for uncommitted_directory in ["validations", "credentials", "documentation", "samples"]:
        safe_mmkdir(os.path.join(base_dir, "uncommitted",
                                 uncommitted_directory), exist_ok=True)

    for notebook in glob.glob(script_relative_path("../init_notebooks/*.ipynb")):
        notebook_name = os.path.basename(notebook)
        shutil.copyfile(notebook, os.path.join(
            base_dir, notebook_dir_name, notebook_name))


# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
greeting_1 = """
Always know what to expect from your data.

If you're new to Great Expectations, this tutorial is a good place to start:

    <blue>https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
""".format(__version__.replace(".", "_"))

msg_prompt_lets_begin = """
Let's add Great Expectations to your project, by scaffolding a new great_expectations directory:

    great_expectations
        ├── great_expectations.yml
        ├── datasources
        ├── expectations
        ├── fixtures
        ├── notebooks
        ├── plugins
        ├── uncommitted
        │   ├── validations
        │   ├── credentials
        │   ├── documentation
        │   └── samples
        └── .gitignore

OK to proceed?
"""
