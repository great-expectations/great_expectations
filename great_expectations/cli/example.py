import pathlib
import subprocess

import click

from great_expectations.cli.pretty_printing import cli_message


@click.group(short_help="Examples")
@click.pass_context
def example(ctx: click.Context) -> None:
    """
    Examples

    Examples of Great Expectations usage in various environments.

    Helpful to get started, demo or reproduce issues.
    """
    pass


@example.command(name="airflow")
@click.pass_context
def example_airflow(ctx: click.Context) -> None:
    """Start an airflow example."""
    cli_message("<green>Setting up airflow example...</green>")
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "airflow"
    example_setup_file = example_directory / "setup_airflow_2_6_0.sh"
    subprocess.run(example_setup_file, cwd=example_directory)


@example.command(name="pandas")
@click.pass_context
def example_pandas(ctx: click.Context) -> None:
    """Start a pandas filesystem example."""
    cli_message("<green>Setting up pandas filesystem example...</green>")
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "pandas_filesystem"
    example_setup_commands = ["docker", "compose", "up"]
    subprocess.run(example_setup_commands, cwd=example_directory)
