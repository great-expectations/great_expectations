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


@example.command(name="s3")
@click.option(
    "--shutdown",
    is_flag=True,
    help="Stop example and clean up. Default false.",
    default=False,
)
@click.option(
    "--metadata-store-bucket-name",
    help="Bucket name to use for metadata stores",
    default="",
)
def example_s3(shutdown: bool, metadata_store_bucket_name: str) -> None:
    """Start an s3 example, using s3 as a datasource and optionally for metadata stores."""
    repo_root = pathlib.Path(__file__).parents[2]
    example_directory = repo_root / "examples" / "s3"
    if shutdown:
        cli_message("<green>Shutting down...</green>")
        shutdown_commands = ["docker", "compose", "down"]
        subprocess.run(shutdown_commands, cwd=example_directory)
        cli_message("<green>Done shutting down...</green>")
    else:
        cli_message("<green>Setting up s3 example...</green>")
        if metadata_store_bucket_name:
            cli_message(
                f"<green>Using s3 bucket {metadata_store_bucket_name} for metadata stores.</green>"
            )
        else:
            cli_message("<green>Using local metadata stores.</green>")
        example_setup_file = example_directory / "setup_s3.sh"
        subprocess.run(example_setup_file, cwd=example_directory)
