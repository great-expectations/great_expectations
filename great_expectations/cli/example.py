import pathlib
import subprocess

import click

from great_expectations.cli.pretty_printing import cli_message


@click.group(short_help="Examples")
def example() -> None:
    """
    Examples
    Examples of Great Expectations usage in various environments.
    Also known as "Reference environments".
    Helpful to get started, demo or reproduce issues.
    NOTE: Reference environments are experimental, the api is likely to change.
    """
    pass


@example.command(name="postgres")
@click.option(
    "--shutdown",
    is_flag=True,
    help="Stop example and clean up. Default false.",
    default=False,
)
def example_postgres(shutdown: bool) -> None:
    """Start a postgres database example."""
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = (
        repo_root / "examples" / "reference_environments" / "postgres_preloaded_data"
    )
    assert example_directory.is_dir(), "Example directory not found"
    if shutdown:
        cli_message("<green>Shutting down...</green>")
        shutdown_commands = ["docker", "compose", "down"]
        subprocess.run(shutdown_commands, cwd=example_directory)
        cli_message("<green>Done shutting down...</green>")
    else:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message("<green>Setting up postgres database example...</green>")
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        outside = "postgresql://example_user@localhost/gx_example_db"
        inside = "postgresql://example_user@db/gx_example_db"
        cli_message(
            f"<green>Postgres db will be available outside the container at:\n{outside}\nand inside the container at:\n{inside}\n</green>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)
