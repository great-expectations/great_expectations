
import click

from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import (
    cli_message,
    cli_message_list,
    load_data_context_with_error_handling,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    send_usage_message,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    safe_mmkdir,
    safe_rrmdir,
    substitute_all_config_variables,
)


@click.group()
def docs():
    """Data Docs operations"""
    pass


@docs.command(name="build")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--site-name",
    "-s",
    help="The site for which to generate documentation. See data_docs section in great_expectations.yml",
)
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True,
)
def docs_build(directory, site_name, view=True):
    """ Build Data Docs for a project."""
    context = load_data_context_with_error_handling(directory)
    build_docs(context, site_name=site_name, view=view)
    send_usage_message(data_context=context, event="cli.docs.build", success=True)


@docs.command(name="list")
@click.option(
    '--directory',
    '-d',
    default=None,
    help="The project's great_expectations directory."
)
def docs_list(directory):
    """List known Data Docs Sites."""
    context = load_data_context_with_error_handling(directory)

    docs_sites_url_dicts = context.get_docs_sites_urls()
    docs_sites_strings = [
        " - <cyan>{}</cyan>: {}".format(docs_site_dict["site_name"], docs_site_dict["site_url"])\
        for docs_site_dict in docs_sites_url_dicts
    ]

    if len(docs_sites_strings) == 0:
        cli_message("No Data Docs sites found")
    else:
        list_intro_string = _build_intro_string(docs_sites_strings)
        cli_message_list(docs_sites_strings, list_intro_string)
        failed = False
        send_usage_message(
            data_context=context,
            event="cli.docs.list",
            success=True
        )
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
    finally:
        if failed and context is not None:
            send_usage_message(
                data_context=context,
                event="cli.docs.list",
                success=False
            )


@docs.command(name="clean")
@click.option(
    '--directory',
    '-d',
    default=None,
    help="Clean data docs"
)
@click.option(
    "--site-name",
    "-s",
    help="The site for which to generate documentation. See data_docs section in great_expectations.yml",
)
@click.option(
    "--all",
    "-a",
    help="The site for which to generate documentation. See data_docs section in great_expectations.yml",
)
def clean_data_docs(directory, site_name=None, all=None):
    """Delete data docs"""
    try:
        context = DataContext(directory)
        failed = True
        if (site_name is None and all is None):
            cli_message("<red>{}</red>".format("Please specify --all y to remove all sites or specify specific site using site_name"))
            sys.exit(1)
        context.clean_data_docs(site_name=site_name)
        failed = False
        send_usage_message(
            data_context=context,
            event="cli.docs.clean",
            success=True
        )
        cli_message("<green>{}</green>".format("Cleaned data docs"))
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(1)
    except ge_exceptions.PluginModuleNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.PluginClassNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    finally:
        if failed and context is not None:
            send_usage_message(
                data_context=context,
                event="cli.docs.clean",
                success=False
            )

    send_usage_message(data_context=context, event="cli.docs.list", success=True)


def _build_intro_string(docs_sites_strings):
    doc_string_count = len(docs_sites_strings)
    if doc_string_count == 1:
        list_intro_string = "1 Data Docs site found:"
    elif doc_string_count > 1:
        list_intro_string = f"{doc_string_count} Data Docs sites found:"
    return list_intro_string


def build_docs(context, site_name=None, view=True):
    """Build documentation in a context"""
    logger.debug("Starting cli.datasource.build_docs")

    cli_message("Building Data Docs...")

    if site_name is not None:
        site_names = [site_name]
    else:
        site_names = None

    index_page_locator_infos = context.build_data_docs(site_names=site_names)

    msg = "The following Data Docs sites were built:\n"
    for site_name, index_page_locator_info in index_page_locator_infos.items():
        if os.path.isfile(index_page_locator_info):
            msg += " - <cyan>{}:</cyan> ".format(site_name)
            msg += "file://{}\n".format(index_page_locator_info)
        else:
            msg += " - <cyan>{}:</cyan> ".format(site_name)
            msg += "{}\n".format(index_page_locator_info)

    msg = msg.rstrip("\n")
    cli_message(msg)

    if view:
        context.open_data_docs()
