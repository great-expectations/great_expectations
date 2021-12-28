import os
import subprocess
import sys
from collections import namedtuple

import click
from cookiecutter.main import cookiecutter

Command = namedtuple("Command", ["name", "full_command", "error_message"])


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
        Command(
            "black",
            "black --check .",
            "Please ensure that your files are linted properly with `black .`",
        ),
        Command(
            "isort",
            "isort --profile black --check .",
            "Please ensure that your imports are sorted properly with `isort --profile black .`",
        ),
        Command(
            "pytest",
            "pytest .",
            "Please ensure that you've written tests and that they all pass",
        ),
        Command(
            "mypy",
            "mypy --ignore-missing-imports --disallow-untyped-defs --show-error-codes --exclude venv .",
            "Please ensure that all functions are type hinted",
        ),
    ]

    successes = 0
    for command in commands:
        if run_command(command, suppress_output=suppress_output):
            successes += 1

    is_successful = successes == len(commands)
    color = "green" if is_successful else "red"
    echo(
        f"Summary: [{successes}/{len(commands)}] checks have passed!", color, bold=True
    )

    return is_successful


def publish_to_pypi() -> None:
    commands = [
        Command(
            "wheel",
            "python setup.py sdist bdist_wheel",
            "Something went wrong when creating a wheel",
        ),
        Command(
            "twine",
            "twine upload --repository testpypi dist/*",
            "Something went wrong when uploading with twine",
        ),
    ]

    for command in commands:
        if not run_command(command):
            return

    echo(
        "Successfully uploaded package to PyPi! Congratulations on a job well done :)",
        "green",
        bold=True,
    )


def run_command(command: Command, suppress_output: bool = False) -> bool:
    # If suppressed, set STDOUT to dev/null
    stdout = sys.stdout
    if suppress_output:
        sys.stdout = open(os.devnull, "w")

    name, full_command, err = command

    echo(f"{name}:", "blue", bold=True)
    result = subprocess.run(
        full_command.split(" "), shell=False, stdout=sys.stdout, stderr=sys.stdout
    )

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
