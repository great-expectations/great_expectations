import pathlib
import subprocess

import click

from great_expectations.cli.pretty_printing import cli_message


@click.group(short_help="Examples")
def example() -> None:
    """
    Examples

    Examples of Great Expectations usage in various environments.

    Helpful to get started, demo or reproduce issues.
    """
    pass


@example.command(name="airflow")
def example_airflow() -> None:
    """Start an airflow example."""
    cli_message("<green>Setting up airflow example...</green>")
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "airflow"
    example_setup_file = example_directory / "setup_airflow_2_6_0.sh"
    subprocess.run(example_setup_file, cwd=example_directory)


@example.command(name="pandas")
def example_pandas() -> None:
    """Start a pandas filesystem example."""
    cli_message("<green>Setting up pandas filesystem example...</green>")
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "pandas_filesystem"
    setup_commands = ["docker", "compose", "up"]
    subprocess.run(setup_commands, cwd=example_directory)

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
    example_directory = repo_root / "examples" / "postgres"
    if shutdown:
        cli_message("<green>Shutting down...</green>")
        shutdown_commands = ["docker", "compose", "down"]
        subprocess.run(shutdown_commands, cwd=example_directory)
        cli_message("<green>Done shutting down...</green>")
    else:
        cli_message("<green>Setting up postgres database example...</green>")
        outside = "postgresql://example_user@localhost/gx_example_db"
        inside = "postgresql://example_user@db/gx_example_db"
        cli_message(f"<green>Postgres db will be available outside the container at {outside} and inside the container at {inside}</green>")
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)
