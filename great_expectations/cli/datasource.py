import os
import click

from great_expectations.datasource import PandasDatasource, SparkDFDatasource, SqlAlchemyDatasource
from .util import cli_message
from great_expectations.exceptions import DatasourceInitializationError
from great_expectations.data_context import DataContext

from great_expectations import rtd_url_ge_version

import logging
logger = logging.getLogger(__name__)


def add_datasource(context):
    cli_message(
        """
========== Datasources ==========

See <blue>https://docs.greatexpectations.io/en/latest/features/datasource.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue> for more information about datasources.
""".format(rtd_url_ge_version)
    )
    data_source_selection = click.prompt(
        msg_prompt_choose_datasource,
        type=click.Choice(["1", "2", "3", "4"]),
        show_choices=False
    )

    cli_message(data_source_selection)
    data_source_name = None

    if data_source_selection == "1":  # pandas
        data_source_name = _add_pandas_datasource(context)
    elif data_source_selection == "2":  # sqlalchemy
        data_source_name = _add_sqlalchemy_datasource(context)
    elif data_source_selection == "3":  # Spark
        data_source_name = _add_spark_datasource(context)
    # if data_source_selection == "5": # dbt
    #     dbt_profile = click.prompt(msg_prompt_dbt_choose_profile)
    #     log_message(msg_dbt_go_to_notebook, color="blue")
    #     context.add_datasource("dbt", "dbt", profile=dbt_profile)
    if data_source_selection == "4":  # None of the above
        cli_message(msg_unknown_data_source)
        cli_message(
            """
Skipping datasource configuration.
    - Add one by running `<green>great_expectations add-datasource</green>` or
    - ... by editing the `{}` file
""".format(DataContext.GE_YML)
        )

    return data_source_name


def _add_pandas_datasource(context):
    path = click.prompt(
        msg_prompt_filesys_enter_base_path,
        # default='/data/',
        type=click.Path(
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True
        ),
        show_default=True
    )
    if path.startswith("./"):
        path = path[2:]

    if path.endswith("/"):
        basenamepath = path[:-1]
    else:
        basenamepath = path

    default_data_source_name = os.path.basename(basenamepath) + "__dir"
    data_source_name = click.prompt(
        msg_prompt_datasource_name,
        default=default_data_source_name,
        show_default=True
    )

    configuration = PandasDatasource.build_configuration(base_directory=os.path.join("..", path))
    context.add_datasource(name=data_source_name, class_name='PandasDatasource', **configuration)
    return data_source_name


def _add_sqlalchemy_datasource(context):
    try:
        import sqlalchemy
        from sqlalchemy import create_engine, MetaData
    except ImportError:
        cli_message("""<red>ERROR: Unable find `sqlalchemy`</red>.
  - Please `pip install sqlalchemy` before trying again.""")
        return None

    data_source_name = click.prompt(
        msg_prompt_datasource_name, default="mydb", show_default=True)

    while True:
        cli_message(msg_sqlalchemy_config_connection.format(data_source_name))

        drivername = click.prompt("What is the driver for the sqlalchemy connection?", default="postgres",
                                  show_default=True)
        if drivername == "postgres":
            host = click.prompt("What is the host for the sqlalchemy connection?", default="localhost",
                                show_default=True)
            port = click.prompt("What is the port for the sqlalchemy connection?", default="5432",
                                show_default=True)
            username = click.prompt("What is the username for the sqlalchemy connection?", default="postgres",
                                show_default=True)
            password = click.prompt("What is the password for the sqlalchemy connection?", default="",
                                show_default=False, hide_input=True)
            database = click.prompt("What is the database name for the sqlalchemy connection?", default="postgres",
                                show_default=True)

            # Since we don't want to save the database credentials in the config file that will be
            # committed in the repo, we will use our Variable Substitution feature to store the credentials
            # in the credentials file (that will not be committed, since it is in the uncommitted directory)
            # with the datasource's name as the variable name.
            # The value of the datasource's "credentials" key in the config file (great_expectations.yml) will
            # be ${datasource name}.
            # GE will replace the ${datasource name} with the value from the credentials file in runtime.
            credentials = {
                "drivername": drivername,
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "database": database
            }
        else:
            sqlalchemy_url = click.prompt(
"""What is the url/connection string for the sqlalchemy connection?
(reference: https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
""",
                show_default=False)
            credentials = {
                "url": sqlalchemy_url
            }

        context.save_config_variable(data_source_name, credentials)

        message = """
<red>Cannot connect to the database.</red>
  - Please check your environment and the configuration you provided.
  - Database Error: {0:s}"""
        try:
            configuration = SqlAlchemyDatasource.build_configuration(credentials="${" + data_source_name + "}")
            context.add_datasource(name=data_source_name, class_name='SqlAlchemyDatasource', **configuration)
            break
        except ModuleNotFoundError as de:
            message = message + "\n  - Please `pip install psycopg2` and try again"
            cli_message(message.format(str(de)))
            return None

        except DatasourceInitializationError as de:
            cli_message(message.format(str(de)))
            if not click.confirm(
                    "Enter the credentials again?".format(str(de)),
                    default=True
            ):
                cli_message(
                    """You can add a datasource later by:
  - Running <green>great_expectations add-datasource</green>
  - Editing the {} file.
""".format(DataContext.GE_YML))
                return None

    return data_source_name


def _add_spark_datasource(context):
    path = click.prompt(
        msg_prompt_filesys_enter_base_path,
        # default='/data/',
        type=click.Path(
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True
        ),
        show_default=True
    )
    if path.startswith("./"):
        path = path[2:]

    if path.endswith("/"):
        path = path[:-1]
    default_data_source_name = os.path.basename(path) + "__dir"
    data_source_name = click.prompt(
        msg_prompt_datasource_name, default=default_data_source_name, show_default=True)

    configuration = SparkDFDatasource.build_configuration(base_directory=os.path.join("..", path))
    context.add_datasource(name=data_source_name, class_name='SparkDFDatasource', **configuration)
    return data_source_name


def profile_datasource(context, data_source_name, data_assets=None, profile_all_data_assets=False, max_data_assets=20,additional_batch_kwargs=None):
    """"Profile a named datasource using the specified context"""
    msg_intro = """
========== Profiling ==========

Profiling '{0:s}' will create expectations and documentation.

Please note: Profiling is still a beta feature in Great Expectations.  The current profiler will evaluate the entire 
data source (without sampling), which may be very time consuming. 
As a rule of thumb, we recommend starting with data smaller than 100MB.

To learn more about profiling, visit <blue>https://docs.greatexpectations.io/en/latest/features/profiling.html\
?utm_source=cli&utm_medium=init&utm_campaign={1:s}</blue>.
"""

    msg_confirm_ok_to_proceed = """
Would you like to profile '{0:s}'?
"""

    msg_skipping = "Skipping profiling for now. You can always do this later " \
                   "by running `<green>great_expectations profile</green>`."

    msg_some_data_assets_not_found = """Some of the data assets you specified were not found: {0:s}    
"""

    msg_too_many_data_assets = """There are {0:d} data assets in {1:s}. Profiling all of them might take too long.    
"""

    msg_prompt_enter_data_asset_list = """Enter comma-separated list of data asset names (e.g., {0:s})   
"""

    msg_options = """Choose how to proceed:
  1. Specify a list of the data assets to profile
  2. Exit and profile later
  3. Profile ALL data assets (this might take a while)
"""

    msg_data_doc_intro = """
========== Data Docs ==========

Great Expectations can create data documentation from the data you just profiled.

To learn more: <blue>https://docs.greatexpectations.io/en/latest/features/data_docs.html?utm_source=cli&utm_medium=init&utm_campaign={0:s}</blue>
"""

    cli_message(msg_intro.format(data_source_name, rtd_url_ge_version))

    if data_assets:
        data_assets = [item.strip() for item in data_assets.split(",")]

    # Call the data context's profiling method to check if the arguments are valid
    profiling_results = context.profile_datasource(
        data_source_name,
        data_assets=data_assets,
        profile_all_data_assets=profile_all_data_assets,
        max_data_assets=max_data_assets,
        dry_run=True,
        additional_batch_kwargs=additional_batch_kwargs
    )

    if profiling_results['success']: # data context is ready to profile - run profiling
        if data_assets or profile_all_data_assets or click.confirm(msg_confirm_ok_to_proceed.format(data_source_name), default=True):
            profiling_results = context.profile_datasource(
            data_source_name,
            data_assets=data_assets,
            profile_all_data_assets=profile_all_data_assets,
            max_data_assets=max_data_assets,
            dry_run=False,
            additional_batch_kwargs=additional_batch_kwargs
        )
        else:
            cli_message(msg_skipping)
            return
    else: # we need to get arguments from user interactively
        do_exit = False
        while not do_exit:
            if profiling_results['error']['code'] == DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND:
                cli_message(msg_some_data_assets_not_found.format("," .join(profiling_results['error']['not_found_data_assets'])))
            elif profiling_results['error']['code'] == DataContext.PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS:
                cli_message(msg_too_many_data_assets.format(profiling_results['error']['num_data_assets'], data_source_name))
            else: # unknown error
                raise ValueError("Unknown profiling error code: " + profiling_results['error']['code'])

            option_selection = click.prompt(
                msg_options,
                type=click.Choice(["1", "2", "3"]),
                show_choices=False
            )

            if option_selection == "1":
                data_assets = click.prompt(
                    msg_prompt_enter_data_asset_list.format(", ".join(profiling_results['error']['data_assets'][:3])),
                    default=None,
                    show_default=False
                )
                if data_assets:
                    data_assets = [item.strip() for item in data_assets.split(",")]
            elif option_selection == "3":
                profile_all_data_assets = True
            elif option_selection == "2": # skip
                cli_message(msg_skipping)
                return
            else:
                raise ValueError("Unrecognized option: " + option_selection)

            # after getting the arguments from the user, let's try to run profiling again
            # (no dry run this time)
            profiling_results = context.profile_datasource(
                data_source_name,
                data_assets=data_assets,
                profile_all_data_assets=profile_all_data_assets,
                max_data_assets=max_data_assets,
                dry_run=False
            )

            if profiling_results['success']: # data context is ready to profile
                break


    cli_message(msg_data_doc_intro.format(rtd_url_ge_version))

    if click.confirm("Build HTML Data Docs?", default=True):
        build_docs(context)
    else:
        cli_message("Okay, skipping HTML documentation for now.")


def build_docs(context, site_name=None):
    """Build documentation in a context"""
    logger.debug("Starting cli.datasource.build_docs")

    cli_message("Building <green>Data Docs</green>...")

    if site_name is not None:
        site_names = [site_name]
    else:
        site_names=None

    index_page_locator_infos = context.build_data_docs(site_names=site_names)

    msg = """
The following data documentation HTML sites were generated:

"""
    for site_name, index_page_locator_info in index_page_locator_infos.items():
        if os.path.isfile(index_page_locator_info):
            msg += site_name + ":\n"
            msg += "   <green>file://" + index_page_locator_info + "</green>\n\n"
        else:
            msg += site_name + "\n"

    cli_message(msg)


msg_prompt_choose_datasource = """
Configure a datasource:
    1. Pandas DataFrame
    2. Relational database (SQL)
    3. Spark DataFrame
    4. Skip datasource configuration
"""

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
Enter the path of the root directory where the data files are stored.
(The path may be either absolute or relative to current directory.)
"""

msg_prompt_datasource_name = """
Give your new data source a short name.
"""

msg_sqlalchemy_config_connection = """
Great Expectations relies on sqlalchemy to connect to relational databases.
Please make sure that you have it installed.

Next, we will configure database credentials and store them in the "{0:s}" section
of this config file: great_expectations/uncommitted/credentials/profiles.yml:
"""

msg_unknown_data_source = """
Do we not have the type of data source you want?
    - Please create a GitHub issue here: <blue>https://github.com/great-expectations/great_expectations/issues/new</blue>

In the meantime, consider reviewing the following notebook for an example of 
creating and saving an expectation suite for validation:
    - `<green>jupyter notebook great_expectations/notebooks/create_expectations.ipynb</green>`
"""

MSG_GO_TO_NOTEBOOK = """    - To create expectations for your data, start Jupyter and open a tutorial notebook:
        - To launch with jupyter notebooks:
            <green>jupyter notebook great_expectations/notebooks/create_expectations.ipynb</green>
"""