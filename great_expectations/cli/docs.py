import sys

import click

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.build_docs import build_docs
from great_expectations.cli.pretty_printing import (
    cli_message,
    cli_message_list,
    display_not_implemented_message_and_exit,
)


@click.group()
@click.pass_context
def docs(ctx):
    """Data Docs operations"""
    directory: str = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    context: DataContext = toolkit.load_data_context_with_error_handling(
        directory=directory,
        from_cli_upgrade_command=False,
    )
    # TODO consider moving this all the way up in to the CLIState constructor
    ctx.obj.data_context = context


@docs.command(name="build")
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
@click.option(
    "--assume-yes",
    "--yes",
    "-y",
    is_flag=True,
    help="By default request confirmation to build docs unless you specify -y/--yes/--assume-yes flag to skip dialog",
    default=False,
)
@click.pass_context
def docs_build(ctx, site_name, view=True, assume_yes=False):
    """ Build Data Docs for a project."""
    display_not_implemented_message_and_exit()
    context = ctx.obj.data_context
    build_docs(context, site_name=site_name, view=view, assume_yes=assume_yes)
    toolkit.send_usage_message(
        data_context=context, event="cli.docs.build", success=True
    )


@docs.command(name="list")
@click.pass_context
def docs_list(ctx):
    """List known Data Docs Sites."""
    display_not_implemented_message_and_exit()
    context = ctx.obj.data_context

    docs_sites_url_dicts = context.get_docs_sites_urls()
    docs_sites_strings = [
        " - <cyan>{}</cyan>: {}".format(
            docs_site_dict["site_name"],
            docs_site_dict.get("site_url")
            or f"site configured but does not exist. Run the following command to build site: great_expectations "
            f'docs build --site-name {docs_site_dict["site_name"]}',
        )
        for docs_site_dict in docs_sites_url_dicts
    ]

    if len(docs_sites_strings) == 0:
        cli_message("No Data Docs sites found")
    else:
        list_intro_string = _build_intro_string(docs_sites_strings)
        cli_message_list(docs_sites_strings, list_intro_string)

    toolkit.send_usage_message(
        data_context=context, event="cli.docs.list", success=True
    )


@docs.command(name="clean")
@click.option(
    "--site-name",
    "-s",
    help="The site that you want documentation cleaned for. See data_docs section in great_expectations.yml",
)
@click.option(
    "--all",
    "-a",
    is_flag=True,
    help="With this, all sites will get their data docs cleaned out. See data_docs section in great_expectations.yml",
)
@click.pass_context
def clean_data_docs(ctx, site_name=None, all=None):
    """Delete data docs"""
    display_not_implemented_message_and_exit()
    context = ctx.obj.data_context
    failed = True
    if site_name is None and all is None:
        cli_message(
            "<red>{}</red>".format(
                "Please specify --all to remove all sites or specify a specific site using "
                "--site_name"
            )
        )
        sys.exit(1)
    context.clean_data_docs(site_name=site_name)
    failed = False
    if not failed and context is not None:
        toolkit.send_usage_message(
            data_context=context, event="cli.docs.clean", success=True
        )
        cli_message("<green>{}</green>".format("Cleaned data docs"))

    if failed and context is not None:
        toolkit.send_usage_message(
            data_context=context, event="cli.docs.clean", success=False
        )


def _build_intro_string(docs_sites_strings):
    doc_string_count = len(docs_sites_strings)
    if doc_string_count == 1:
        list_intro_string = "1 Data Docs site configured:"
    elif doc_string_count > 1:
        list_intro_string = f"{doc_string_count} Data Docs sites configured:"
    return list_intro_string
