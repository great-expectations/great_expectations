from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

import click

from great_expectations.cli import toolkit
from great_expectations.cli.build_docs import build_docs
from great_expectations.cli.pretty_printing import cli_message, cli_message_list
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.exceptions import DataContextError

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.file_data_context import (
        FileDataContext,
    )


@click.group()
@click.pass_context
def docs(ctx: click.Context) -> None:
    """Data Docs operations"""
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()

    invoked_subcommand = ctx.invoked_subcommand
    assert (
        invoked_subcommand
    ), "Proper registration of subcommand has not occurred; please review parent Click context"

    cli_event_noun: str = "docs"
    (
        begin_event_name,
        end_event_name,
    ) = UsageStatsEvents.get_cli_begin_and_end_event_names(
        noun=cli_event_noun,
        verb=invoked_subcommand,
    )
    send_usage_message(
        data_context=ctx.obj.data_context,
        event=begin_event_name,
        success=True,
    )
    ctx.obj.usage_event_end = end_event_name


@docs.command(name="build")
@click.option(
    "--site-name",
    "-sn",
    help="The site for which to generate documentation. If not present all sites will be built. See data_docs section in great_expectations.yml",
)
@click.option(
    "--no-view",
    "-nv",
    "no_view",
    is_flag=True,
    help="By default open in browser unless you specify the --no-view flag",
    default=False,
)
@click.pass_context
def docs_build(
    ctx: click.Context, site_name: Optional[str] = None, no_view: bool = False
) -> None:
    """Build Data Docs for a project."""
    context: FileDataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end

    if site_name is not None and site_name not in context.get_site_names():
        toolkit.exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event_end,
            message=f"<red>The specified site name `{site_name}` does not exist in this project.</red>",
        )
    if site_name is None:
        sites_to_build = context.get_site_names()
    else:
        sites_to_build = [site_name]

    build_docs(
        context,
        usage_stats_event=usage_event_end,
        site_names=sites_to_build,
        view=not no_view,
        assume_yes=ctx.obj.assume_yes,
    )
    send_usage_message(
        data_context=context,
        event=usage_event_end,
        success=True,
    )


@docs.command(name="list")
@click.pass_context
def docs_list(ctx: click.Context):
    """List known Data Docs sites."""
    context = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    docs_sites_url_dicts = context.get_docs_sites_urls()

    try:
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

        send_usage_message(
            data_context=context,
            event=usage_event_end,
            success=True,
        )
    except Exception as e:
        toolkit.exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event_end,
            message=f"<red>{e}</red>",
        )
        return


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
def docs_clean(
    ctx: click.Context, site_name: Optional[str] = None, all_sites: bool = False
) -> None:
    """
    Remove all files from a Data Docs site.

    This is a useful first step if you wish to completely re-build a site from scratch.
    """
    context = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end

    if (site_name is None and all_sites is False) or (site_name and all_sites):
        toolkit.exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event_end,
            message="<red>Please specify either --all to clean all sites or a specific site using --site-name</red>",
        )
    try:
        # if site_name is None, context.clean_data_docs(site_name=site_name)
        # will clean all sites.
        context.clean_data_docs(site_name=site_name)
        send_usage_message(
            data_context=context,
            event=usage_event_end,
            success=True,
        )
        cli_message("<green>Cleaned data docs</green>")
    except DataContextError as de:
        toolkit.exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event_end,
            message=f"<red>{de}</red>",
        )


def _build_intro_string(docs_sites_strings: List[str]) -> str:
    doc_string_count = len(docs_sites_strings)
    if doc_string_count == 1:
        list_intro_string = "1 Data Docs site configured:"
    elif doc_string_count > 1:
        list_intro_string = f"{doc_string_count} Data Docs sites configured:"
    return list_intro_string
