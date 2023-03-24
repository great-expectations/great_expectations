from __future__ import annotations

import importlib
import logging
from typing import List, Optional

import click

from great_expectations import __version__ as ge_version
from great_expectations.cli import toolkit
from great_expectations.cli.cli_logging import _set_up_logger
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)

try:
    from colorama import init as init_colorama

    init_colorama()
except ImportError:
    pass


class CLIState:
    def __init__(
        self,
        config_file_location: Optional[str] = None,
        data_context: Optional[FileDataContext] = None,
        assume_yes: bool = False,
    ) -> None:
        self.config_file_location = config_file_location
        self._data_context = data_context
        self.assume_yes = assume_yes

    def get_data_context_from_config_file(self) -> FileDataContext:
        directory: str = toolkit.parse_cli_config_file_location(  # type: ignore[assignment] # could be None
            config_file_location=self.config_file_location  # type: ignore[arg-type] # could be None
        ).get(
            "directory"
        )
        context: FileDataContext = toolkit.load_data_context_with_error_handling(  # type: ignore[assignment] # will exit if error
            directory=directory,
            from_cli_upgrade_command=False,
        )
        return context

    @property
    def data_context(self) -> Optional[FileDataContext]:
        return self._data_context

    @data_context.setter
    def data_context(self, data_context: FileDataContext) -> None:
        assert isinstance(
            data_context, FileDataContext
        ), "GX CLI interaction requires a FileDataContext"
        self._data_context = data_context

    def __repr__(self) -> str:
        return f"CLIState(config_file_location={self.config_file_location})"


class CLI(click.MultiCommand):
    def list_commands(self, ctx: click.Context) -> List[str]:
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
        ]

        return commands

    def get_command(self, ctx: click.Context, name: str) -> Optional[str]:  # type: ignore[override] # MultiCommand returns `Optional[Command]`
        module_name = name.replace("-", "_")
        legacy_module = ""
        try:
            requested_module = f"great_expectations.cli{legacy_module}.{module_name}"
            module = importlib.import_module(requested_module)
            return getattr(module, module_name)

        except ModuleNotFoundError:
            cli_message(
                f"<red>The command `{name}` does not exist.\nPlease use one of: {self.list_commands(None)}</red>"  # type: ignore[arg-type] # expects click Context
            )
            return None


@click.group(cls=CLI, name="great_expectations")
@click.version_option(version=ge_version)
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
def cli(
    ctx: click.Context,
    verbose: bool,
    config_file_location: Optional[str],
    assume_yes: bool,
) -> None:
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
    ctx.obj = CLIState(config_file_location=config_file_location, assume_yes=assume_yes)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
