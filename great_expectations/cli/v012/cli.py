import logging

import click

from great_expectations import __version__ as ge_version
from great_expectations.cli.v012.checkpoint import checkpoint
from great_expectations.cli.v012.cli_logging import _set_up_logger
from great_expectations.cli.v012.datasource import datasource
from great_expectations.cli.v012.docs import docs
from great_expectations.cli.v012.init import init
from great_expectations.cli.v012.project import project
from great_expectations.cli.v012.store import store
from great_expectations.cli.v012.suite import suite
from great_expectations.cli.v012.validation_operator import validation_operator

try:
    from colorama import init as init_colorama

    init_colorama()
except ImportError:
    pass


@click.group()
@click.version_option(version=ge_version)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set great_expectations to use verbose output.",
)
def cli(verbose) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    Welcome to the great_expectations CLI!\n\n    Most commands follow this format: great_expectations <NOUN> <VERB>\n\n    The nouns are: datasource, docs, project, suite, validation-operator\n\n    Most nouns accept the following verbs: new, list, edit\n\n    In particular, the CLI supports the following special commands:\n\n    - great_expectations init : create a new great_expectations project\n\n    - great_expectations datasource profile : profile a datasource\n\n    - great_expectations docs build : compile documentation from expectations"
    logger = _set_up_logger()
    if verbose:
        logger.setLevel(logging.DEBUG)


cli.add_command(datasource)
cli.add_command(docs)
cli.add_command(init)
cli.add_command(project)
cli.add_command(suite)
cli.add_command(validation_operator)
cli.add_command(store)
cli.add_command(checkpoint)


def main() -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    cli()


if __name__ == "__main__":
    main()
