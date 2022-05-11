import click

from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message, cli_message_dict
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message


@click.group()
@click.pass_context
def store(ctx: click.Context) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Store operations"
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()
    cli_event_noun: str = "store"
    (
        begin_event_name,
        end_event_name,
    ) = UsageStatsEvents.get_cli_begin_and_end_event_names(
        noun=cli_event_noun, verb=ctx.invoked_subcommand
    )
    send_usage_message(
        data_context=ctx.obj.data_context, event=begin_event_name, success=True
    )
    ctx.obj.usage_event_end = end_event_name


@store.command(name="list")
@click.pass_context
def store_list(ctx: click.Context):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "List active Stores."
    context = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    try:
        stores = context.list_active_stores()
        cli_message(f"{len(stores)} active Stores found:")
        for store in stores:
            cli_message("")
            cli_message_dict(store)
        send_usage_message(data_context=context, event=usage_event_end, success=True)
    except Exception as e:
        toolkit.exit_with_failure_message_and_stats(
            context=context, usage_event=usage_event_end, message=f"<red>{e}</red>"
        )
        return
