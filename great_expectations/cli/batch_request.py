import logging
import os
import sys
import uuid

import click

from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.core import ExpectationSuite
from great_expectations.datasource import (
    PandasDatasource,
    SparkDFDatasource,
    SqlAlchemyDatasource,
)
from great_expectations.datasource.batch_kwargs_generator import (
    TableBatchKwargsGenerator,
)
from great_expectations.exceptions import BatchKwargsError
from great_expectations.validator.validator import BridgeValidator

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sqlalchemy = None


# TODO consolidate all the myriad CLI tests into this
def select_batch_kwargs_generator(
    context, datasource_name, available_data_assets_dict=None
):
    msg_prompt_select_generator = "Select batch kwarg generator"

    if available_data_assets_dict is None:
        available_data_assets_dict = context.get_available_data_asset_names(
            datasource_names=datasource_name
        )

    available_data_asset_names_by_generator = {}
    for key, value in available_data_assets_dict[datasource_name].items():
        if len(value["names"]) > 0:
            available_data_asset_names_by_generator[key] = value["names"]

    if len(available_data_asset_names_by_generator.keys()) == 0:
        return None
    elif len(available_data_asset_names_by_generator.keys()) == 1:
        return list(available_data_asset_names_by_generator.keys())[0]
    else:  # multiple batch_kwargs_generators
        generator_names = list(available_data_asset_names_by_generator.keys())
        choices = "\n".join(
            [
                "    {}. {}".format(i, generator_name)
                for i, generator_name in enumerate(generator_names, 1)
            ]
        )
        option_selection = click.prompt(
            msg_prompt_select_generator + "\n" + choices,
            type=click.Choice(
                [str(i) for i, generator_name in enumerate(generator_names, 1)]
            ),
            show_choices=False,
        )
        batch_kwargs_generator_name = generator_names[int(option_selection) - 1]

        return batch_kwargs_generator_name


# TODO this method needs testing
# TODO this method has different numbers of returned objects
def get_batch_kwargs(
    context,
    datasource_name=None,
    batch_kwargs_generator_name=None,
    data_asset_name=None,
    additional_batch_kwargs=None,
):
    """
    This method manages the interaction with user necessary to obtain batch_kwargs for a batch of a data asset.

    In order to get batch_kwargs this method needs datasource_name, batch_kwargs_generator_name and data_asset_name
    to combine them into a fully qualified data asset identifier(datasource_name/batch_kwargs_generator_name/data_asset_name).
    All three arguments are optional. If they are present, the method uses their values. Otherwise, the method
    prompts user to enter them interactively. Since it is possible for any of these three components to be
    passed to this method as empty values and to get their values after interacting with user, this method
    returns these components' values in case they changed.

    If the datasource has batch_kwargs_generators that can list available data asset names, the method lets user choose a name
    from that list (note: if there are multiple batch_kwargs_generators, user has to choose one first). If a name known to
    the chosen batch_kwargs_generator is selected, the batch_kwargs_generators will be able to yield batch_kwargs. The method also gives user
    an alternative to selecting the data asset name from the batch_kwargs_generators's list - user can type in a name for their
    data asset. In this case a passthrough batch kwargs batch_kwargs_generators will be used to construct a fully qualified data asset
    identifier (note: if the datasource has no passthrough batch_kwargs_generators configured, the method will exist with a failure).
    Since no batch_kwargs_generators can yield batch_kwargs for this data asset name, the method prompts user to specify batch_kwargs
    by choosing a file (if the datasource is pandas or spark) or by writing a SQL query (if the datasource points
    to a database).

    :param context:
    :param datasource_name:
    :param batch_kwargs_generator_name:
    :param data_asset_name:
    :param additional_batch_kwargs:
    :return: a tuple: (datasource_name, batch_kwargs_generator_name, data_asset_name, batch_kwargs). The components
                of the tuple were passed into the methods as optional arguments, but their values might
                have changed after this method's execution. If the returned batch_kwargs is None, it means
                that the batch_kwargs_generator will know to yield batch_kwargs when called.
    """
    try:
        available_data_assets_dict = context.get_available_data_asset_names(
            datasource_names=datasource_name
        )
    except ValueError:
        # the datasource has no batch_kwargs_generators
        available_data_assets_dict = {datasource_name: {}}

    data_source = toolkit.select_datasource(context, datasource_name=datasource_name)
    datasource_name = data_source.name

    if batch_kwargs_generator_name is None:
        batch_kwargs_generator_name = select_batch_kwargs_generator(
            context,
            datasource_name,
            available_data_assets_dict=available_data_assets_dict,
        )

    # if the user provided us with the batch kwargs generator name and the data asset, we have everything we need -
    # let's ask the generator to build batch kwargs for this asset - we are done.
    if batch_kwargs_generator_name is not None and data_asset_name is not None:
        generator = data_source.get_batch_kwargs_generator(batch_kwargs_generator_name)
        batch_kwargs = generator.build_batch_kwargs(
            data_asset_name, **additional_batch_kwargs
        )
        return batch_kwargs

    if isinstance(
        context.get_datasource(datasource_name), (PandasDatasource, SparkDFDatasource)
    ):
        (
            data_asset_name,
            batch_kwargs,
        ) = _get_batch_kwargs_from_generator_or_from_file_path(
            context,
            datasource_name,
            batch_kwargs_generator_name=batch_kwargs_generator_name,
        )

    elif isinstance(context.get_datasource(datasource_name), SqlAlchemyDatasource):
        data_asset_name, batch_kwargs = _get_batch_kwargs_for_sqlalchemy_datasource(
            context, datasource_name, additional_batch_kwargs=additional_batch_kwargs
        )

    else:
        raise ge_exceptions.DataContextError(
            "Datasource {:s} is expected to be a PandasDatasource or SparkDFDatasource, but is {:s}".format(
                datasource_name, str(type(context.get_datasource(datasource_name)))
            )
        )

    return (datasource_name, batch_kwargs_generator_name, data_asset_name, batch_kwargs)


def _get_batch_kwargs_from_generator_or_from_file_path(
    context,
    datasource_name,
    batch_kwargs_generator_name=None,
    additional_batch_kwargs=None,
):
    if additional_batch_kwargs is None:
        additional_batch_kwargs = {}

    msg_prompt_generator_or_file_path = """
Would you like to:
    1. choose from a list of data assets in this datasource
    2. enter the path of a data file
"""
    msg_prompt_file_path = """
Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
"""

    msg_prompt_enter_data_asset_name = "\nWhich data would you like to use?\n"

    msg_prompt_enter_data_asset_name_suffix = (
        "    Don't see the name of the data asset in the list above? Just type it\n"
    )

    msg_prompt_file_type = """
We could not determine the format of the file. What is it?
    1. CSV
    2. Parquet
    3. Excel
    4. JSON
"""

    reader_method_file_extensions = {
        "1": "csv",
        "2": "parquet",
        "3": "xlsx",
        "4": "json",
    }

    data_asset_name = None

    datasource = context.get_datasource(datasource_name)
    if batch_kwargs_generator_name is not None:
        generator = datasource.get_batch_kwargs_generator(batch_kwargs_generator_name)

        option_selection = click.prompt(
            msg_prompt_generator_or_file_path,
            type=click.Choice(["1", "2"]),
            show_choices=False,
        )

        if option_selection == "1":

            available_data_asset_names = sorted(
                generator.get_available_data_asset_names()["names"], key=lambda x: x[0]
            )
            available_data_asset_names_str = [
                "{} ({})".format(name[0], name[1])
                for name in available_data_asset_names
            ]

            data_asset_names_to_display = available_data_asset_names_str[:50]
            choices = "\n".join(
                [
                    "    {}. {}".format(i, name)
                    for i, name in enumerate(data_asset_names_to_display, 1)
                ]
            )
            prompt = (
                msg_prompt_enter_data_asset_name
                + choices
                + "\n"
                + msg_prompt_enter_data_asset_name_suffix.format(
                    len(data_asset_names_to_display)
                )
            )

            data_asset_name_selection = click.prompt(prompt, show_default=False)

            data_asset_name_selection = data_asset_name_selection.strip()
            try:
                data_asset_index = int(data_asset_name_selection) - 1
                try:
                    data_asset_name = [name[0] for name in available_data_asset_names][
                        data_asset_index
                    ]
                except IndexError:
                    pass
            except ValueError:
                data_asset_name = data_asset_name_selection

            batch_kwargs = generator.build_batch_kwargs(
                data_asset_name, **additional_batch_kwargs
            )
            return (data_asset_name, batch_kwargs)

    # No generator name was passed or the user chose to enter a file path

    # We should allow a directory for Spark, but not for Pandas
    dir_okay = isinstance(datasource, SparkDFDatasource)

    path = None
    while True:
        # do not use Click to check if the file exists - the get_batch
        # logic will check this
        path = click.prompt(
            msg_prompt_file_path,
            type=click.Path(dir_okay=dir_okay),
            default=path,
        )

        if not path.startswith("gs:") and not path.startswith("s3"):
            path = os.path.abspath(path)

        batch_kwargs = {"path": path, "datasource": datasource_name}

        reader_method = None
        try:
            reader_method = datasource.guess_reader_method_from_path(path)[
                "reader_method"
            ]
        except BatchKwargsError:
            pass

        if reader_method is None:

            while True:

                option_selection = click.prompt(
                    msg_prompt_file_type,
                    type=click.Choice(["1", "2", "3", "4"]),
                    show_choices=False,
                )

                try:
                    reader_method = datasource.guess_reader_method_from_path(
                        path + "." + reader_method_file_extensions[option_selection]
                    )["reader_method"]
                except BatchKwargsError:
                    pass

                if reader_method is not None:
                    batch_kwargs["reader_method"] = reader_method
                    if (
                        isinstance(datasource, SparkDFDatasource)
                        and reader_method == "csv"
                    ):
                        header_row = click.confirm(
                            "\nDoes this file contain a header row?", default=True
                        )
                        batch_kwargs["reader_options"] = {"header": header_row}
                    batch = datasource.get_batch(batch_kwargs=batch_kwargs)
                    break
        else:
            try:
                batch_kwargs["reader_method"] = reader_method
                if isinstance(datasource, SparkDFDatasource) and reader_method == "csv":
                    header_row = click.confirm(
                        "\nDoes this file contain a header row?", default=True
                    )
                    batch_kwargs["reader_options"] = {"header": header_row}
                batch = datasource.get_batch(batch_kwargs=batch_kwargs)
                break
            except Exception as e:
                file_load_error_message = """
<red>Cannot load file.</red>
  - Please check the file and try again or select a different data file.
  - Error: {0:s}"""
                cli_message(file_load_error_message.format(str(e)))
                if not click.confirm("\nTry again?", default=True):
                    cli_message(
                        """
We have saved your setup progress. When you are ready, run great_expectations init to continue.
"""
                    )
                    sys.exit(1)

    if data_asset_name is None and batch_kwargs.get("path"):
        try:
            # Try guessing a filename
            filename = os.path.split(batch_kwargs.get("path"))[1]
            # Take all but the last part after the period
            filename = ".".join(filename.split(".")[:-1])
            data_asset_name = filename
        except (OSError, IndexError):
            pass

    batch_kwargs["data_asset_name"] = data_asset_name

    return (data_asset_name, batch_kwargs)


def _get_default_schema(datasource):
    inspector = sqlalchemy.inspect(datasource.engine)
    return inspector.default_schema_name


def _get_batch_kwargs_for_sqlalchemy_datasource(
    context, datasource_name, additional_batch_kwargs=None
):
    data_asset_name = None
    sql_query = None
    datasource = context.get_datasource(datasource_name)
    msg_prompt_how_to_connect_to_data = """
You have selected a datasource that is a SQL database. How would you like to specify the data?
1. Enter a table name and schema
2. Enter a custom SQL query
3. List all tables in the database (this may take a very long time)
"""
    default_schema = _get_default_schema(datasource)
    temp_generator = TableBatchKwargsGenerator(name="temp", datasource=datasource)

    while data_asset_name is None:
        single_or_multiple_data_asset_selection = click.prompt(
            msg_prompt_how_to_connect_to_data,
            type=click.Choice(["1", "2", "3"]),
            show_choices=False,
        )
        if single_or_multiple_data_asset_selection == "1":  # name the table and schema
            schema_name = click.prompt(
                "Please provide the schema name of the table (this is optional)",
                default=default_schema,
            )
            table_name = click.prompt(
                "Please provide the table name (this is required)"
            )
            data_asset_name = f"{schema_name}.{table_name}"

        elif single_or_multiple_data_asset_selection == "2":  # SQL query
            sql_query = click.prompt("Please provide the SQL query")
            data_asset_name = "custom_sql_query"

        elif single_or_multiple_data_asset_selection == "3":  # list it all
            msg_prompt_warning = fr"""Warning: If you have a large number of tables in your datasource, this may take a very long time. \m
                    Would you like to continue?"""
            confirmation = click.prompt(
                msg_prompt_warning, type=click.Choice(["y", "n"]), show_choices=True
            )
            if confirmation == "y":
                # avoid this call until necessary
                available_data_asset_names = (
                    temp_generator.get_available_data_asset_names()["names"]
                )
                available_data_asset_names_str = [
                    "{} ({})".format(name[0], name[1])
                    for name in available_data_asset_names
                ]

                data_asset_names_to_display = available_data_asset_names_str
                choices = "\n".join(
                    [
                        "    {}. {}".format(i, name)
                        for i, name in enumerate(data_asset_names_to_display, 1)
                    ]
                )
                msg_prompt_enter_data_asset_name = (
                    "\nWhich table would you like to use? (Choose one)\n"
                )
                prompt = msg_prompt_enter_data_asset_name + choices + os.linesep
                selection = click.prompt(prompt, show_default=False)
                selection = selection.strip()
                try:
                    data_asset_index = int(selection) - 1
                    try:
                        data_asset_name = [
                            name[0] for name in available_data_asset_names
                        ][data_asset_index]

                    except IndexError:
                        print(
                            f"You have specified {selection}, which is an incorrect index"
                        )
                        pass
                except ValueError:
                    print(
                        f"You have specified {selection}, which is an incorrect value"
                    )
                    pass

    if additional_batch_kwargs is None:
        additional_batch_kwargs = {}

    # Some backends require named temporary table parameters. We specifically elicit those and add them
    # where appropriate.
    temp_table_kwargs = dict()
    datasource = context.get_datasource(datasource_name)

    if datasource.engine.dialect.name.lower() == "bigquery":
        # bigquery also requires special handling
        bigquery_temp_table = click.prompt(
            "Great Expectations will create a table to use for "
            "validation." + os.linesep + "Please enter a name for this table: ",
            default="SOME_PROJECT.SOME_DATASET.ge_tmp_" + str(uuid.uuid4())[:8],
        )
        temp_table_kwargs = {
            "bigquery_temp_table": bigquery_temp_table,
        }

    # now building the actual batch_kwargs
    if sql_query is None:
        batch_kwargs = temp_generator.build_batch_kwargs(
            data_asset_name, **additional_batch_kwargs
        )
        batch_kwargs.update(temp_table_kwargs)
    else:
        batch_kwargs = {"query": sql_query, "datasource": datasource_name}
        batch_kwargs.update(temp_table_kwargs)
        BridgeValidator(
            batch=datasource.get_batch(batch_kwargs),
            expectation_suite=ExpectationSuite("throwaway"),
        ).get_dataset()

    batch_kwargs["data_asset_name"] = data_asset_name
    return data_asset_name, batch_kwargs
