import os
import glob
import shutil

from great_expectations.data_context.util import safe_mmkdir
from great_expectations import __version__


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
    #!!! FIXME: Check to see if the directory already exists. If it does, refuse with:
    # `great_expectations/` already exists.
    # If you're certain you want to re-initialize Great Expectations within this project,
    # please delete the whole `great_expectations/` directory and run `great_expectations init` again.

    safe_mmkdir(base_dir, exist_ok=True)
    notebook_dir_name = "notebooks"

    open(os.path.join(base_dir, ".gitignore"), 'w').write("""uncommitted/""")

    for directory in [notebook_dir_name, "expectations", "datasources", "uncommitted", "plugins", "fixtures"]:
        safe_mmkdir(os.path.join(base_dir, directory), exist_ok=True)

    for uncommitted_directory in ["validations", "credentials", "samples"]:
        safe_mmkdir(os.path.join(base_dir, "uncommitted",
                                 uncommitted_directory), exist_ok=True)

    for notebook in glob.glob(script_relative_path("../init_notebooks/*.ipynb")):
        notebook_name = os.path.basename(notebook)
        shutil.copyfile(notebook, os.path.join(
            base_dir, notebook_dir_name, notebook_name))


#!!! This injects a version tag into the docs. We should test that those versioned docs exist in RTD.
greeting_1 = """
Always know what to expect from your data.

If you're new to Great Expectations, this tutorial is a good place to start:

    https://great-expectations.readthedocs.io/en/v%s/intro.html#how-do-i-get-started
""" % __version__

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
        │   └── samples
        └── .gitignore

OK to proceed?
"""

msg_prompt_choose_data_source = """
Configure a data source
    1. Pandas data frames from local filesystem (CSV files)
    2. Relational database (SQL)
    3. Spark DataFrames from local filesystem (CSV files)
    4. None of the above
"""

#     msg_prompt_choose_data_source = """
# Time to create expectations for your data. This is done in Jupyter Notebook/Jupyter Lab.
#
# Before we point you to the right notebook, what data does your project work with?
#     1. Directory on local filesystem
#     2. Relational database (SQL)
#     3. DBT (data build tool) models
#     4. None of the above
#     """


#     msg_prompt_dbt_choose_profile = """
# Please specify the name of the dbt profile (from your ~/.dbt/profiles.yml file Great Expectations \
# should use to connect to the database
#     """

#     msg_dbt_go_to_notebook = """
# To create expectations for your dbt models start Jupyter and open notebook
# great_expectations/notebooks/using_great_expectations_with_dbt.ipynb -
# it will walk you through next steps.
#     """

msg_prompt_filesys_enter_base_path = """
Enter the path of the root directory where the data files are stored
(the path may be either absolute or relative to current directory)
"""

msg_filesys_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps.

To launch with jupyter notebooks:
    jupyter notebook great_expectations/notebooks/create_expectations_for_csv_files.ipynb

To launch with jupyter lab:
    jupyter lab great_expectations/notebooks/create_expectations_for_csv_files.ipynb
"""

msg_prompt_datasource_name = """
Give your new data source a short name
"""

msg_sqlalchemy_config_connection = """
Great Expectations relies on sqlalchemy to connect to relational databases.
Please make sure that you have it installed.

Next, we will configure database credentials and store them in the "{0:s}" section
of this config file: great_expectations/uncommitted/credentials/profiles.yml:
"""

msg_sqlalchemy_go_to_notebook = """
To create expectations for your SQL queries start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_sql.ipynb -
it will walk you through configuring the database connection and next steps.
"""

msg_unknown_data_source = """
We are looking for more types of data types to support.
Please create a GitHub issue here:
https://github.com/great-expectations/great_expectations/issues/new
In the meantime you can see what Great Expectations can do on CSV files.
To create expectations for your CSV files start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb -
it will walk you through configuring the database connection and next steps.
"""
msg_spark_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps.

To launch with jupyter notebooks:
    jupyter notebook great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb

To launch with jupyter lab:
    jupyter lab great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb
"""
