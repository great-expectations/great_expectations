import importlib
import logging
from typing import Optional

import click

from great_expectations import DataContext
from great_expectations import __version__ as ge_version
from great_expectations.cli.cli_logging import _set_up_logger
from great_expectations.cli.pretty_printing import cli_message

try:
    from colorama import init as init_colorama

    init_colorama()
except ImportError:
    pass


class CLIState:
    def __init__(
        self,
        legacy_api: bool = False,
        config_file_location: Optional[str] = None,
        data_context: DataContext = None,
    ):
        self.legacy_api = legacy_api
        self.config_file_location = config_file_location
        self._data_context = data_context

    @property
    def data_context(self):
        return self._data_context

    @data_context.setter
    def data_context(self, data_context):
        assert isinstance(data_context, DataContext)
        self._data_context = data_context

    def __repr__(self):
        return f"CLIState(legacy_api={self.legacy_api}, config_file_location={self.config_file_location})"


class CLI(click.MultiCommand):
    def list_commands(self, ctx):
        # note that if --help is called this method is invoked before any flags
        # are parsed.or context set.

        return [
            "checkpoint",
            "datasource",
            "docs",
            "init",
            "project",
            "store",
            "suite",
            "validation_operator",
        ]

    def get_command(self, ctx, name):
        module_name = name.replace("-", "_")
        legacy_module = ""

        if self.is_legacy(ctx):
            legacy_module += ".v012"
        try:
            requested_module = f"great_expectations.cli{legacy_module}.{module_name}"
            module = importlib.import_module(requested_module)
            return getattr(module, module_name)

        except ModuleNotFoundError:
            cli_message(
                f"<red>The command `{name}` does not exist.\nPlease use one of: {self.list_commands(None)}</red>"
            )
            return None

    @staticmethod
    def print_ctx_debugging(ctx):
        print(f"ctx.args: {ctx.args}")
        print(f"ctx.params: {ctx.params}")
        print(f"ctx.obj: {ctx.obj}")
        print(f"ctx.protected_args: {ctx.protected_args}")
        print(f"ctx.find_root().args: {ctx.find_root().args}")
        print(f"ctx.find_root().params: {ctx.find_root().params}")
        print(f"ctx.find_root().obj: {ctx.find_root().obj}")
        print(f"ctx.find_root().protected_args: {ctx.find_root().protected_args}")

    @staticmethod
    def is_legacy(ctx):
        """Determine if legacy api is requested by searching context params."""
        if ctx.params:
            return (
                ctx.params
                and "legacy_api" in ctx.params.keys()
                and ctx.params["legacy_api"]
            )

        root_ctx_params = ctx.find_root().params
        return (
            root_ctx_params
            and "legacy_api" in root_ctx_params.keys()
            and root_ctx_params["legacy_api"]
        )


@click.group(cls=CLI, name="great_expectations")
@click.version_option(version=ge_version)
@click.option(
    "--legacy-api/--new-api",
    "legacy_api",
    is_flag=True,
    default=True,
    help="Default to legacy APIs (before 0.13). Use --new-api for new APIs (after version 0.13)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set great_expectations to use verbose output.",
)
# TODO: <Alex>ALEX -- Update this --help page to fit with the new design.</Alex>
@click.option(
    "--config",
    "-c",
    "config_file_location",
    default=None,
    help="Path to great_expectations configuration file location (great_expectations.yml). Inferred if not provided.",
)
@click.pass_context
def cli(ctx, legacy_api, verbose, config_file_location):
    """
    Welcome to the great_expectations CLI!

    Most commands follow this format: great_expectations <NOUN> <VERB>

    The nouns are: checkpoint, datasource, docs, init, project, store, suite, validation-operator.
    Most nouns accept the following verbs: new, list, edit
    """
    logger = _set_up_logger()
    if verbose:
        # Note we are explicitly not using a logger in all CLI output to have
        # more control over console UI.
        logger.setLevel(logging.DEBUG)
    ctx.obj = CLIState(legacy_api=legacy_api, config_file_location=config_file_location)

    if legacy_api:
        cli_message("<yellow>Using legacy APIs (pre-0.13)</yellow>")
    else:
        cli_message("<green>Using new APIs (0.13.x)</green>")


def main():
    cli()


if __name__ == "__main__":
    main()
