import click

from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import (
    cli_message,
    cli_message_dict,
    display_not_implemented_message_and_exit,
)


@click.group()
def store():
    """Store operations"""
    pass


@store.command(name="list")
@click.pass_context
def store_list(ctx):
    """List known Stores."""
    display_not_implemented_message_and_exit()
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    context = toolkit.load_data_context_with_error_handling(directory)

    try:
        stores = context.list_stores()

        if len(stores) == 0:
            cli_message("No Stores found")
            toolkit.send_usage_message(
                data_context=context, event="cli.store.list", success=True
            )
            return
        elif len(stores) == 1:
            list_intro_string = "1 Store found:"
        else:
            list_intro_string = "{} Stores found:".format(len(stores))

        cli_message(list_intro_string)

        for store in stores:
            cli_message("")
            cli_message_dict(store)

        toolkit.send_usage_message(
            data_context=context, event="cli.store.list", success=True
        )
    except Exception as e:
        toolkit.send_usage_message(
            data_context=context, event="cli.store.list", success=False
        )
        raise e
