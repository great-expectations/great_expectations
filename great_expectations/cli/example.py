import json
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
    "--stop",
    is_flag=True,
    help="Stop example and clean up. Default false.",
    default=False,
)
@click.option(
    "--url",
    is_flag=True,
    help="Print url for jupyter notebook.",
    default=False,
)
@click.option(
    "--bash",
    is_flag=True,
    help="Open a bash terminal in the container (container should already be running).",
    default=False,
)
def example_postgres(stop: bool, url: bool, bash: bool) -> None:
    """Start a postgres database example."""
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "postgres"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_postgres_example_jupyter"
    if stop:
        cli_message("<green>Stopping example containers...</green>")
        stop_commands = ["docker", "compose", "down"]
        subprocess.run(stop_commands, cwd=example_directory)
        cli_message("<green>Done stopping containers.</green>")
    elif url:
        url_commands = [
            "docker",
            "exec",
            container_name,
            "jupyter",
            "server",
            "list",
            "--json",
        ]
        url_json = subprocess.run(
            url_commands,
            cwd=example_directory,
            capture_output=True,
        ).stdout
        raw_json = json.loads(url_json)
        notebook_url = (
            f"http://127.0.0.1:{raw_json['port']}/lab?token={raw_json['token']}"
        )
        cli_message(f"<green>Url for jupyter notebook:</green> {notebook_url}")
    elif bash:
        bash_commands = ["docker", "exec", "-it", container_name, "bash"]
        subprocess.run(bash_commands, cwd=example_directory)
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
