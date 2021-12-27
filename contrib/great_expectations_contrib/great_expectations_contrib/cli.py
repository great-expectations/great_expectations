import click
from great_expectations_contrib.commands import check_cmd, init_cmd, publish_cmd

URL = "https://github.com/superconductive/ge-contrib-cookiecutter"


@click.group()
def cli() -> None:
    """
    Welcome to the ge_contrib CLI!
    This tool is meant to make contributing new packages to Great Expectations as smooth as possible.

    Most commands follow this format: `ge_contrib <VERB>`

    Use `init` to set up a package, `check` to verify that you've met the requirements, and
    `publish` to upload your work to PyPi. Happy coding!
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
