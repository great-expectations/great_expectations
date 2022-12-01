import importlib

import invoke


def _exit_with_error_if_docs_dependencies_are_not_installed():
    """Check and report which dependencies are not installed."""

    module_dependencies = ("sphinx", "myst_parser", "pydata_sphinx_theme")
    modules_not_installed = []

    for module_name in module_dependencies:
        try:
            importlib.import_module(module_name)
        except ImportError:
            modules_not_installed.append(module_name)

    if modules_not_installed:
        raise invoke.Exit(
            f"Please make sure to install missing docs dependencies: {', '.join(modules_not_installed)} by running pip install -r docs/sphinx_api_docs_source/requirements-dev-api-docs.txt",
            code=1,
        )


def _remove_existing_sphinx_api_docs(ctx: invoke.context.Context):
    """Remove existing sphinx api docs."""
    cmds = ["make clean"]
    ctx.run(" ".join(cmds), echo=True, pty=True)


def _build_html_api_docs_in_temp_folder(ctx: invoke.context.Context):
    """Build html api documentation in temporary folder."""

    cmds = ["sphinx-build -M html ./ ../../temp_docs_build_dir/sphinx_api_docs"]
    ctx.run(" ".join(cmds), echo=True, pty=True)