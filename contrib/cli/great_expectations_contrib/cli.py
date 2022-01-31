import click
from great_expectations_contrib.commands import check_cmd, init_cmd, publish_cmd
from great_expectations_contrib.package import GreatExpectationsContribPackage

# The following link points to the repo where the Cookiecutter template is hosted
URL = "https://github.com/great-expectations/great-expectations-contrib-cookiecutter"


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """
    Welcome to the great_expectations_contrib CLI!
    This tool is meant to make contributing new packages to Great Expectations as smooth as possible.

    Usage: `great_expectations_contrib <VERB>`

    Create a package using `init`, check your code with `check`, and upload your work to PyPi with `publish`.
    """
    pkg = GreatExpectationsContribPackage.from_json_file()
    ctx.obj = pkg


@cli.command(help="Initialize a contributor package")
def init() -> None:
    init_cmd(URL)


@cli.command(help="Publish your package to PyPi")
@click.pass_obj
def publish(pkg: GreatExpectationsContribPackage) -> None:
    publish_cmd()
    pkg.update_package_state()


@cli.command(help="Check your package to make sure it's met all the requirements")
@click.pass_obj
def check(pkg: GreatExpectationsContribPackage) -> None:
    check_cmd()
    pkg.update_package_state()


if __name__ == "__main__":
    cli()
