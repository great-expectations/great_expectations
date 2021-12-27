import os
import subprocess
import sys
from typing import Tuple

import click
from cookiecutter.main import cookiecutter


def init_cmd(url: str) -> None:
    click.echo(click.style("Configure your template:\n", fg="blue", bold=True))
    cookiecutter(url, overwrite_if_exists=False)
    click.echo(
        click.style("\nSuccessfully set up contrib package!", fg="green", bold=True)
    )


def publish_cmd() -> None:
    success = _perform_check(suppress_output=True)
    if not success:
        click.echo(
            click.style(
                "Please run the `check` command to diagnose before publishing",
                fg="red",
                bold=True,
            )
        )
        return

    click.echo(
        click.style(
            "All checks have succeeded; you are ready to publish!",
            fg="green",
            bold=True,
        )
    )

    _publish_to_pypi()


def check_cmd() -> None:
    _perform_check(suppress_output=False)


def _perform_check(suppress_output: bool) -> bool:
    commands = [
        (
            "black --check .",
            "Please ensure that your files are linted properly with `black .`",
        ),
        (
            "isort --profile black --check .",
            "Please ensure that your imports are sorted properly with `isort --profile black .`",
        ),
        ("pytest .", "Please ensure that you've written tests and that they all pass"),
        (
            "mypy --ignore-missing-imports --disallow-untyped-defs --show-error-codes --exclude venv .",
            "Please ensure that all functions are type hinted",
        ),
    ]

    count = 0
    for i, command in enumerate(commands):
        success = _run_check_command(i + 1, command, suppress_output=suppress_output)
        if success:
            count += 1

    is_successful = count == len(commands)
    click.echo(
        click.style(
            f"Summary: [{count}/{len(commands)}] checks have passed!",
            fg="green" if is_successful else "red",
            bold=True,
        )
    )

    return is_successful


def _run_check_command(
    idx: int, command: Tuple[str, str], suppress_output: bool
) -> bool:
    # If applicable, set STDOUT to dev/null
    stdout = sys.stdout
    if suppress_output:
        sys.stdout = open(os.devnull, "w")

    cmd, err = command
    args = cmd.split(" ")
    name = args[0]

    click.echo(click.style(f"{idx}) [{name}]", fg="blue", bold=True))
    result = subprocess.run(args, shell=False, stdout=sys.stdout, stderr=sys.stdout)

    success = result.returncode == 0
    if success:
        click.echo(click.style("Check succeeded\n", fg="green"))
    else:
        click.echo(click.style(f"Check failed - {err}\n", fg="red"))

    # If reassigned before, set STDOUT back
    sys.stdout = stdout
    return success


def _publish_to_pypi() -> None:
    create_wheel = subprocess.run(
        ["python", "setup.py", "sdist", "bdist_wheel"], shell=False
    )

    if create_wheel.returncode != 0:
        click.echo(
            click.style(
                "Something went wrong when creating a wheel", fg="red", bold=True
            )
        )
        return

    upload_twine = subprocess.run(
        ["twine", "upload", "--repository", "testpypi", "dist/*"], shell=False
    )

    if upload_twine.returncode != 0:
        click.echo(
            click.style(
                "Something went wrong when uploading with twine", fg="red", bold=True
            )
        )
        return

    click.echo(
        click.style(
            "Succesfully uploaded package to PyPi! Congratulations :)",
            fg="green",
            bold=True,
        )
    )
