from __future__ import annotations

import dataclasses
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


@dataclasses.dataclass
class CommandOptions:
    stop: bool
    url: bool
    bash: bool
    rebuild: bool


@example.command(name="snowflake")
@click.option(
    "--stop",
    "--down",
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
def example_snowflake(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
) -> None:
    """Start a snowflake database example."""
    unset_env_vars = _check_snowflake_env_vars()
    if unset_env_vars:
        cli_message(
            f"<red>Please check your config, currently we only support connecting via env vars. You are missing the following vars: {', '.join(unset_env_vars)}</red>"
        )
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "snowflake"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_snowflake_example_jupyter"
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message("<green>Setting up snowflake database example...</green>")
        print_green_line()
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


@example.command(name="postgres")
@click.option(
    "--stop",
    "--down",
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
def example_postgres(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
) -> None:
    """Start a postgres database example."""
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "postgres"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_postgres_example_jupyter"
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message("<green>Setting up postgres database example...</green>")
        print_green_line()
        outside = "postgresql://example_user@localhost/gx_example_db"
        inside = "postgresql://example_user@db/gx_example_db"
        cli_message(
            f"<green>Postgres db will be available outside the container at:\n{outside}\nand inside the container at:\n{inside}\n</green>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


@example.command(name="s3")
@click.option(
    "--stop",
    "--down",
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
def example_s3(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
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
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        print_green_line()
        cli_message("<green>Setting up s3 example...</green>")

        cli_message(
            "<green>Using local filesystem metadata stores. To use s3 metadata stores, please edit the default configuration in the container.</green>"
        )

        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


@example.command(name="gcs")
@click.option(
    "--stop",
    "--down",
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
def example_gcs(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
) -> None:
    """Start a google cloud storage example."""
    unset_env_vars = _check_gcs_env_vars()
    if unset_env_vars:
        cli_message(
            f"<red>Please check your config, currently we only support connecting via env vars. You are missing the following vars: {', '.join(unset_env_vars)}</red>"
        )
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "gcs"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_gcs_example_jupyter"
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


@example.command(name="bigquery")
@click.option(
    "--stop",
    "--down",
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
def example_bigquery(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
) -> None:
    """Start a bigquery database example."""
    unset_env_vars = _check_bigquery_env_vars()
    if unset_env_vars:
        cli_message(
            f"<red>Please check your config, currently we only support connecting via env vars. You are missing the following vars: {', '.join(unset_env_vars)}</red>"
        )
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "bigquery"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_bigquery_example_jupyter"
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


@example.command(name="abs")
@click.option(
    "--stop",
    "--down",
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
def example_abs(
    stop: bool,
    url: bool,
    bash: bool,
    rebuild: bool,
) -> None:
    """Start an Azure Blob Storage example."""
    unset_env_vars = _check_abs_env_vars()
    if unset_env_vars:
        cli_message(
            f"<red>Please check your config, currently we only support connecting via env vars. You are missing the following vars: {', '.join(unset_env_vars)}</red>"
        )
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "reference_environments" / "abs"
    assert example_directory.is_dir(), "Example directory not found"
    container_name = "gx_abs_example_jupyter"
    command_options = CommandOptions(stop, url, bash, rebuild)
    executed_standard_function = _execute_standard_functions(
        command_options, example_directory, container_name
    )
    if not executed_standard_function:
        cli_message(
            "<yellow>Reference environments are experimental, the api is likely to change.</yellow>"
        )
        cli_message(
            "<green>To connect to the jupyter server, please use the links at the end of the log messages.</green>"
        )
        print_green_line()
        setup_commands = ["docker", "compose", "up"]
        subprocess.run(setup_commands, cwd=example_directory)


def _execute_standard_functions(
    command_options: CommandOptions,
    example_directory: pathlib.Path,
    container_name: str,
) -> bool:
    """Execute standard functions for all examples based on flags.

    Returns:
        bool: True if a function was executed, False otherwise.
    """

    if (
        sum(
            [
                int(option)
                for option in (
                    command_options.stop,
                    command_options.url,
                    command_options.bash,
                    command_options.rebuild,
                )
            ]
        )
        > 1
    ):
        raise click.UsageError(
            "Please only use one of --stop, --url, --bash, --rebuild"
        )

    executed = False
    if command_options.stop:
        cli_message("<green>Shutting down...</green>")
        stop_commands = ["docker", "compose", "down"]
        subprocess.run(stop_commands, cwd=example_directory)
        cli_message("<green>Done shutting down...</green>")
        executed = True
    elif command_options.url:
        notebook_url = _get_jupyter_url(container_name, example_directory)
        cli_message(f"<green>Url for jupyter notebook:</green> {notebook_url}")
        executed = True
    elif command_options.bash:
        bash_commands = ["docker", "exec", "-it", container_name, "bash"]
        subprocess.run(bash_commands, cwd=example_directory)
        executed = True
    elif command_options.rebuild:
        cli_message("<green>Rebuilding containers...</green>")
        rebuild_commands = ["docker", "compose", "build"]
        subprocess.run(rebuild_commands, cwd=example_directory)
        cli_message("<green>Done rebuilding containers.</green>")
        executed = True

    return executed


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


def print_green_line() -> None:
    """Print a green line."""
    cli_message(
        "<green>------------------------------------------------------------------------------------------</green>"
    )


def _check_aws_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    )
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result


def _check_bigquery_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = ("GOOGLE_APPLICATION_CREDENTIALS", "BIGQUERY_CONNECTION_STRING")
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result


def _check_gcs_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = ("GOOGLE_APPLICATION_CREDENTIALS", "GCP_PROJECT_NAME")
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result


def _check_snowflake_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = ("SNOWFLAKE_CONNECTION_STRING",)
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result


def _check_abs_env_vars() -> set[str]:
    """Return list of env var names that are not set."""
    env_vars_to_check = ("AZURE_STORAGE_ACCOUNT_URL", "AZURE_CREDENTIAL")
    result = {ev for ev in env_vars_to_check if not os.getenv(ev)}

    return result
