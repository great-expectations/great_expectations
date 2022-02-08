import click
from great_expectations_contrib.commands import check_cmd, init_cmd, publish_cmd

# The following link points to the repo where the Cookiecutter template is hosted
URL = "https://github.com/great-expectations/great-expectations-contrib-cookiecutter"


@click.group()
def cli() -> None:
    """
    Welcome to the great_expectations_contrib CLI!
    This tool is meant to make contributing new packages to Great Expectations as smooth as possible.

    Usage: `great_expectations_contrib <VERB>`

    Create a package using `init`, check your code with `check`, and upload your work to PyPi with `publish`.
    """
    pass


@cli.command(help="Initialize a contributor package")
def init() -> None:
    init_cmd(URL)


@cli.command(help="Publish your package to PyPi")
def publish() -> None:
    publish_cmd()


@cli.command(help="Check your package to make sure it's met all the requirements")
def check() -> None:
    check_cmd()


if __name__ == "__main__":
    cli()
