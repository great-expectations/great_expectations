import importlib
import logging
from typing import Optional

import click

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations import __version__ as ge_version
from great_expectations.cli import toolkit
from great_expectations.cli.cli_logging import _set_up_logger
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.data_context.types.base import (
    FIRST_GE_CONFIG_VERSION_WITH_CHECKPOINT_STORE,
)

try:
    from colorama import init as init_colorama

    init_colorama()
except ImportError:
    pass


class CLIState:
    def __init__(
        self,
        v3_api: bool = True,
        config_file_location: Optional[str] = None,
        data_context: Optional[DataContext] = None,
        assume_yes: bool = False,
    ):
        self.v3_api = v3_api
        self.config_file_location = config_file_location
        self._data_context = data_context
        self.assume_yes = assume_yes

    def get_data_context_from_config_file(self) -> DataContext:
        directory: str = toolkit.parse_cli_config_file_location(
            config_file_location=self.config_file_location
        ).get("directory")
        context: DataContext = toolkit.load_data_context_with_error_handling(
            directory=directory,
            from_cli_upgrade_command=False,
        )
        return context

    @property
    def data_context(self):
        return self._data_context

    @data_context.setter
    def data_context(self, data_context):
        assert isinstance(data_context, DataContext)
        self._data_context = data_context

    def __repr__(self):
        return f"CLIState(v3_api={self.v3_api}, config_file_location={self.config_file_location})"


class CLI(click.MultiCommand):
    def list_commands(self, ctx):
        # note that if --help is called this method is invoked before any flags
        # are parsed or context set.
        commands = [
            "checkpoint",
            "datasource",
            "docs",
            "init",
            "project",
            "store",
            "suite",
            "docker"
        ]

        return commands

    def get_command(self, ctx, name):
        module_name = name.replace("-", "_")
        legacy_module = ""
        if not self.is_v3_api(ctx):
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
    def is_v3_api(ctx):
        """Determine if v3 api is requested by searching context params."""
        if ctx.params:
            return ctx.params and "v3_api" in ctx.params.keys() and ctx.params["v3_api"]

        root_ctx_params = ctx.find_root().params
        return (
            root_ctx_params
            and "v3_api" in root_ctx_params.keys()
            and root_ctx_params["v3_api"]
        )


@click.group(cls=CLI, name="great_expectations")
@click.version_option(version=ge_version)
@click.option(
    "--v3-api/--v2-api",
    "v3_api",
    is_flag=True,
    default=True,
    help="Default to v3 (Batch Request) API. Use --v2-api for v2 (Batch Kwargs) API",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set great_expectations to use verbose output.",
)
@click.option(
    "--config",
    "-c",
    "config_file_location",
    default=None,
    help="Path to great_expectations configuration file location (great_expectations.yml). Inferred if not provided.",
)
@click.option(
    "--assume-yes",
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help='Assume "yes" for all prompts.',
)
@click.pass_context
def cli(ctx, v3_api, verbose, config_file_location, assume_yes):
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
    ctx.obj = CLIState(
        v3_api=v3_api, config_file_location=config_file_location, assume_yes=assume_yes
    )

    if v3_api:
        cli_message("Using v3 (Batch Request) API")
    else:
        cli_message("Using v2 (Batch Kwargs) API")

        ge_config_version: float = (
            ctx.obj.get_data_context_from_config_file().get_config().config_version
        )
        if ge_config_version >= FIRST_GE_CONFIG_VERSION_WITH_CHECKPOINT_STORE:
            raise ge_exceptions.InvalidDataContextConfigError(
                f"Using the legacy v2 (Batch Kwargs) API with a recent config version ({ge_config_version}) is illegal."
            )


def main():
    cli()


if __name__ == "__main__":
    main()
