import click

from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message, cli_message_dict
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message


@click.group()
@click.pass_context
def store(ctx: click.Context) -> None:
    """Store operations"""
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()

    cli_event_noun: str = "store"
    (
        begin_event_name,
        end_event_name,
    ) = UsageStatsEvents.get_cli_begin_and_end_event_names(
        noun=cli_event_noun,
        verb=ctx.invoked_subcommand,  # type: ignore[arg-type]
    )
    send_usage_message(
        data_context=ctx.obj.data_context,
        event=begin_event_name,
        success=True,
    )
    ctx.obj.usage_event_end = end_event_name


@store.command(name="list")
@click.pass_context
def store_list(ctx: click.Context):
    """List active Stores."""
    context = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    try:
        stores = context.list_active_stores()
        cli_message(f"{len(stores)} active Stores found:")
        for store in stores:
            cli_message("")
            cli_message_dict(store)

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
