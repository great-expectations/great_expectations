import os
import click

from .util import cli_message
from great_expectations.render import DescriptivePageView


def add_datasource(context):
    data_source_selection = click.prompt(
        msg_prompt_choose_data_source,
        type=click.Choice(["1", "2", "3", "4"]),
        show_choices=False
    )

    cli_message(data_source_selection)

    if data_source_selection == "1":  # pandas
        print("This init script will configure a local ")
        path = click.prompt(
            msg_prompt_filesys_enter_base_path,
            # default='/data/',
            type=click.Path(
                exists=False,
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

        context.add_datasource(data_source_name, "pandas", base_directory=path)

    elif data_source_selection == "2":  # sqlalchemy
        data_source_name = click.prompt(
            msg_prompt_datasource_name, default="mydb", show_default=True)

        cli_message(msg_sqlalchemy_config_connection.format(
            data_source_name))

        drivername = click.prompt("What is the driver for the sqlalchemy connection?", default="postgres",
                                  show_default=True)
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

        credentials = {
            "drivername": drivername,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database
        }
        context.add_profile_credentials(data_source_name, **credentials)

        context.add_datasource(
            data_source_name, "sqlalchemy", profile=data_source_name)

    elif data_source_selection == "3":  # Spark
        path = click.prompt(
            msg_prompt_filesys_enter_base_path,
            default='/data/',
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
        default_data_source_name = os.path.basename(basenamepath)
        data_source_name = click.prompt(
            msg_prompt_datasource_name, default=default_data_source_name, show_default=True)

        context.add_datasource(data_source_name, "spark", base_directory=path)

    # if data_source_selection == "5": # dbt
    #     dbt_profile = click.prompt(msg_prompt_dbt_choose_profile)
    #     log_message(msg_dbt_go_to_notebook, color="blue")
    #     context.add_datasource("dbt", "dbt", profile=dbt_profile)
    if data_source_selection == "4":  # None of the above
        cli_message(msg_unknown_data_source)
        print("Skipping datasource configuration. You can add a datasource later by editing the great_expectations.yml file.")
        return None

    if data_source_name != None:

        if click.confirm(
            "\nWould you like to profile '%s' to create candidate expectations and documentation?\n" % (
                data_source_name),
            default=True
        ):
            data_asset_names = context.profile_datasource(
                data_source_name,
                max_data_assets=20
            )
            if click.confirm(
                "\nWould you like to view render html documentation for the profiled datasource?\n",
                default = True
            ):
                for data_asset in data_asset_names:
                    validation_result = context.get_validation_result(data_asset)
                    cli_message("Rendering validation result: %s" % data_asset)
                    DescriptivePageView.render(validation_result)
    
        else:
            cli_message(
                "Okay, skipping profiling for now. You can always do this later by running `great_expectations profile`."
            )

    if data_source_selection == "1":  # Pandas
        cli_message(msg_filesys_go_to_notebook)

    elif data_source_selection == "2":  # SQL
        cli_message(msg_sqlalchemy_go_to_notebook)

    elif data_source_selection == "3":  # Spark
        cli_message(msg_spark_go_to_notebook)


msg_prompt_choose_data_source = """
Configure a data source:
    1. Pandas data frames (including local filesystem)
    2. Relational database (SQL)
    3. Spark DataFrames
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
Enter the path of the root directory where the data files are stored
(the path may be either absolute or relative to current directory)
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

msg_unknown_data_source = """
We are looking for more types of data types to support.
Please create a GitHub issue here:
https://github.com/great-expectations/great_expectations/issues/new
In the meantime you can see what Great Expectations can do on CSV files.
To create expectations for your CSV files start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb -
it will walk you through configuring the database connection and next steps.
"""

msg_filesys_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps.

To launch with jupyter notebooks:
    <blue>jupyter notebook great_expectations/notebooks/create_expectations_for_csv_files.ipynb</blue>

To launch with jupyter lab:
    <blue>jupyter lab great_expectations/notebooks/create_expectations_for_csv_files.ipynb</blue>
"""

msg_sqlalchemy_go_to_notebook = """
To create expectations for your SQL queries start Jupyter and open notebook
great_expectations/notebooks/using_great_expectations_with_sql.ipynb -
it will walk you through configuring the database connection and next steps.
"""

msg_spark_go_to_notebook = """
To create expectations for your CSV files start Jupyter and open the notebook
great_expectations/notebooks/using_great_expectations_with_pandas.ipynb.
it will walk you through configuring the database connection and next steps.

To launch with jupyter notebooks:
    <blue>jupyter notebook great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb</blue>

To launch with jupyter lab:
    <blue>jupyter lab great_expectations/notebooks/create_expectations_for_spark_dataframes.ipynb</blue>
"""
