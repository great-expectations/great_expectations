import os
import subprocess
import sys
from typing import Tuple

import click
from cookiecutter.main import cookiecutter


def init_cmd(url: str) -> None:
    """
    Initializes a contributor package by pulling down the Cookiecutter template
    and hydrating it.
    """
    echo("Configure your template:\n", "blue", bold=True)
    cookiecutter(url, overwrite_if_exists=False)
    echo("\nSuccessfully set up contrib package!", "green", bold=True)


def check_cmd() -> None:
    """
    Performs a series of checks on a contributor package.
    These include code style, testing, docstrings, and more.
    """
    perform_check(suppress_output=False)


def publish_cmd() -> None:
    """
    Performs same checks as `check_cmd`; if they pass, the user is prompted to
    supply PyPi credentials. Valid inputs will result in an uploaded package.
    """
    success = perform_check(suppress_output=True)
    if not success:
        echo(
            "Please run the `check` command to diagnose before publishing",
            "red",
            bold=True,
        )
        return

    echo("All checks have succeeded; you are ready to publish!", "green", bold=True)
    publish_to_pypi()


def perform_check(suppress_output: bool) -> bool:
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

    successes = 0
    for i, command in enumerate(commands):
        if run_command(i + 1, command, suppress_output=suppress_output):
            successes += 1

    is_successful = successes == len(commands)
    color = "green" if is_successful else "red"
    echo(
        f"Summary: [{successes}/{len(commands)}] checks have passed!", color, bold=True
    )

    return is_successful


def publish_to_pypi() -> None:
    commands = [
        (
            "python setup.py sdist bdist_wheel",
            "Something went wrong when creating a wheel",
        ),
        (
            "twine upload --repository testpypi dist/*",
            "Something went wrong when uploading with twine",
        ),
    ]

    for i, command in enumerate(commands):
        if not run_command(i + 1, command):
            return

    echo(
        "Successfully uploaded package to PyPi! Congratulations on a job well done :)",
        "green",
        bold=True,
    )


def run_command(
    idx: int, command: Tuple[str, str], suppress_output: bool = False
) -> bool:
    # If suppressed, set STDOUT to dev/null
    stdout = sys.stdout
    if suppress_output:
        sys.stdout = open(os.devnull, "w")

    cmd, err = command
    args = cmd.split(" ")
    name = args[0]

    echo(f"{idx}) [{name}]", "blue", bold=True)
    result = subprocess.run(args, shell=False, stdout=sys.stdout, stderr=sys.stdout)

    success = result.returncode == 0
    if success:
        echo("[SUCCEEDED]\n", "green")
    else:
        echo(f"[FAILED] {err}\n", "red")

    # If reassigned before, set STDOUT back to its default value
    sys.stdout = stdout
    return success


def echo(msg: str, color: str, bold: bool = False) -> None:
    click.echo(click.style(msg, fg=color, bold=bold))
