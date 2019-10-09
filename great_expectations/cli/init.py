# -*- coding: utf-8 -*-

import os
import glob
import shutil

from great_expectations.data_context.util import safe_mmkdir
from great_expectations import __version__ as ge_version


def file_relative_path(dunderfile, relative_path):
    """
    This function is useful when one needs to load a file that is
    relative to the position of the current file. (Such as when
    you encode a configuration file path in source file and want
    in runnable in any current working directory)

    It is meant to be used like the following:
    file_relative_path(__file__, 'path/relative/to/file')

    H/T https://github.com/dagster-io/dagster/blob/8a250e9619a49e8bff8e9aa7435df89c2d2ea039/python_modules/dagster/dagster/utils/__init__.py#L34
    """
    return os.path.join(os.path.dirname(dunderfile), relative_path)


def script_relative_path(file_path):
    """
    Useful for testing with local files. Use a path relative to where the
    test resides and this function will return the absolute path
    of that file. Otherwise it will be relative to script that
    ran the test

    Note: this is function is very, very expensive (on the order of 1
    millisecond per invocation) so this should only be used in performance
    insensitive contexts. Prefer file_relative_path for anything with
    performance constraints.
    """
    # from http://bit.ly/2snyC6s

    import inspect
    scriptdir = inspect.stack()[1][1]
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(scriptdir)), file_path))


def scaffold_directories_and_notebooks(base_dir):
    """Add project directories for a new GE project."""

    safe_mmkdir(base_dir, exist_ok=True)
    notebook_dir_name = "notebooks"

    open(os.path.join(base_dir, ".gitignore"), 'w').write("uncommitted/\n.ipynb_checkpoints/")

    for directory in [notebook_dir_name, "expectations", "datasources", "uncommitted", "plugins"]:
        safe_mmkdir(os.path.join(base_dir, directory), exist_ok=True)

    for uncommitted_directory in ["validations", "data_docs", "samples"]:
        safe_mmkdir(os.path.join(base_dir, "uncommitted",
                                 uncommitted_directory), exist_ok=True)

    for notebook in glob.glob(file_relative_path(__file__, "../init_notebooks/*.ipynb")):
        notebook_name = os.path.basename(notebook)
        shutil.copyfile(notebook, os.path.join(
            base_dir, notebook_dir_name, notebook_name))


# !!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
greeting_1 = """     -- Always know what to expect from your data. --

If you're new to Great Expectations, this tutorial is a good place to start:
- <blue>https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
""".format(ge_version.replace(".", "_"))

msg_prompt_lets_begin = """Let's add Great Expectations to your project, by scaffolding a new great_expectations directory
that will look like this:

    great_expectations
    ├── .gitignore
    ├── datasources
    ├── expectations
    ├── great_expectations.yml
    ├── notebooks
    │   ├── create_expectations.ipynb
    │   └── integrate_validation_into_pipeline.ipynb
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── data_docs
        │   └── local_site
        ├── samples
        └── validations
    
OK to proceed?
"""
