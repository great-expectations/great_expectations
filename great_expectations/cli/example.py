from __future__ import annotations

import json
import os
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


@example.command(name="snowflake")
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
def example_snowflake(stop: bool, url: bool) -> None:
    """Start a snowflake database example."""
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "snowflake"
    assert example_directory.is_dir(), "Example directory not found"
    if stop:
        cli_message("<green>Stopping example containers...</green>")
        stop_commands = ["docker", "compose", "down"]
        subprocess.run(stop_commands, cwd=example_directory)
        cli_message("<green>Done stopping containers.</green>")
    elif url:
        container_name = "gx_snowflake_example_jupyter"
        notebook_url = _get_jupyter_url(container_name, example_directory)
        cli_message(f"<green>Url for jupyter notebook:</green> {notebook_url}")
    else:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message("<green>Setting up snowflake database example...</green>")
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


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
        notebook_url = _get_jupyter_url(container_name, example_directory)
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


@example.command(name="s3")
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
@click.option(
    "--rebuild",
    is_flag=True,
    help="Rebuild the containers.",
    default=False,
)
@click.option(
    "--metadata-store-bucket-name",
    help="Bucket name to use for metadata stores",
    default="",
)
def example_s3(
    stop: bool, url: bool, bash: bool, rebuild: bool, metadata_store_bucket_name: str
) -> None:
    """Start an s3 example, using s3 as a datasource and optionally for metadata stores."""
    unset_env_vars = _check_aws_env_vars()
    if unset_env_vars:
        cli_message(
            f"<red>Please check your config, currently we only support connecting via env vars. You are missing the following vars: {', '.join(unset_env_vars)}</red>"
        )
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "s3"
    container_name = "gx_s3_example_jupyter"
    if stop:
        cli_message("<green>Shutting down...</green>")
        stop_commands = ["docker", "compose", "down"]
        subprocess.run(stop_commands, cwd=example_directory)
        cli_message("<green>Done shutting down...</green>")
    elif url:
        notebook_url = _get_jupyter_url(container_name, example_directory)
        cli_message(f"<green>Url for jupyter notebook:</green> {notebook_url}")
    elif bash:
        bash_commands = ["docker", "exec", "-it", container_name, "bash"]
        subprocess.run(bash_commands, cwd=example_directory)
    elif rebuild:
        cli_message("<green>Rebuilding containers...</green>")
        rebuild_commands = ["docker", "compose", "build"]
        subprocess.run(rebuild_commands, cwd=example_directory)
        cli_message("<green>Done rebuilding containers.</green>")
    else:
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        cli_message("<green>Setting up s3 example...</green>")
        if metadata_store_bucket_name:
            cli_message(
                f"<green>Using s3 bucket {metadata_store_bucket_name} for metadata stores.</green>"
            )
        else:
            cli_message("<green>Using local metadata stores.</green>")

        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        cli_message(
            "<green>------------------------------------------------------------------------------------------</green>"
        )
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


def _get_jupyter_url(container_name: str, example_directory: pathlib.Path) -> str:
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
    notebook_url = f"http://127.0.0.1:{raw_json['port']}/lab?token={raw_json['token']}"
    return notebook_url


def _check_aws_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    )
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result
