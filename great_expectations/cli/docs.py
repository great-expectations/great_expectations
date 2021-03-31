import click

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.build_docs import build_docs
from great_expectations.cli.pretty_printing import cli_message, cli_message_list
from great_expectations.exceptions import DataContextError


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
@click.pass_context
def docs_build(ctx, site_name, view=True):
    """Build Data Docs for a project."""
    context: DataContext = ctx.obj.data_context
    build_docs(context, site_name=site_name, view=view, assume_yes=ctx.obj.assume_yes)
    toolkit.send_usage_message(
        data_context=context, event="cli.docs.build", success=True
    )


@docs.command(name="list")
@click.pass_context
def docs_list(ctx):
    """List known Data Docs sites."""
    context = ctx.obj.data_context
    docs_sites_url_dicts = context.get_docs_sites_urls()

    if len(docs_sites_url_dicts) == 0:
        cli_message("No Data Docs sites found")
    else:
        docs_sites_strings = [
            " - <cyan>{}</cyan>: {}".format(
                docs_site_dict["site_name"],
                docs_site_dict.get("site_url")
                or f"site configured but does not exist. Run the following command to build site: great_expectations "
                f'docs build --site-name {docs_site_dict["site_name"]}',
            )
            for docs_site_dict in docs_sites_url_dicts
        ]
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
    "all_sites",
    is_flag=True,
    help="With this, all sites will get their data docs cleaned out. See data_docs section in great_expectations.yml",
)
@click.pass_context
def clean_data_docs(ctx, site_name=None, all_sites=False):
    """Delete data docs"""
    context = ctx.obj.data_context

    if (site_name is None and all_sites is False) or (site_name and all_sites):
        toolkit.exit_with_failure_message_and_stats(
            context,
            usage_event="cli.docs.clean",
            message="<red>Please specify either --all to remove all sites or a specific site using --site_name</red>",
        )
    try:
        # if site_name is None, context.clean_data_docs(site_name=site_name)
        # will clean all sites.
        context.clean_data_docs(site_name=site_name)
        toolkit.send_usage_message(
            data_context=context, event="cli.docs.clean", success=True
        )
        cli_message("<green>{}</green>".format("Cleaned data docs"))
    except DataContextError as de:
        toolkit.exit_with_failure_message_and_stats(
            context,
            usage_event="cli.docs.clean",
            message=f"<red>{de}</red>",
        )


def _build_intro_string(docs_sites_strings):
    doc_string_count = len(docs_sites_strings)
    if doc_string_count == 1:
        list_intro_string = "1 Data Docs site configured:"
    elif doc_string_count > 1:
        list_intro_string = f"{doc_string_count} Data Docs sites configured:"
    return list_intro_string
