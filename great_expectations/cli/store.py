import click

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message, cli_message_dict
from great_expectations.core.usage_statistics.util import send_usage_message


@click.group()
@click.pass_context
def store(ctx):
    """Store operations"""
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()

    usage_stats_prefix = f"cli.store.{ctx.invoked_subcommand}"
    send_usage_message(
        data_context=ctx.obj.data_context,
        event=f"{usage_stats_prefix}.begin",
        success=True,
    )
    ctx.obj.usage_event_end = f"{usage_stats_prefix}.end"


@store.command(name="list")
@click.pass_context
def store_list(ctx):
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
            context=context,
            usage_event=usage_event_end,
            message=f"<red>{e}</red>",
        )
        return
