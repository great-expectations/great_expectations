import click

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.pretty_printing import cli_message, cli_message_dict


@click.group()
@click.pass_context
def store(ctx):
    """Store operations"""
    directory: str = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    context: DataContext = toolkit.load_data_context_with_error_handling(
        directory=directory,
        from_cli_upgrade_command=False,
    )
    # TODO consider moving this all the way up in to the CLIState constructor
    ctx.obj.data_context = context


@store.command(name="list")
@click.pass_context
def store_list(ctx):
    """List known Stores."""
    context = ctx.obj.data_context
    stores = context.list_stores()
    cli_message(f"{len(stores)} Stores found:")
    for store in stores:
        cli_message("")
        cli_message_dict(store)

    toolkit.send_usage_message(
        data_context=context, event="cli.store.list", success=True
    )
