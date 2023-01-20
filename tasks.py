"""
PyInvoke developer task file
https://www.pyinvoke.org/

These tasks can be run using `invoke <NAME>` or `inv <NAME>` from the project root.

To show all available tasks `invoke --list`

To show task help page `invoke <NAME> --help`
"""
from __future__ import annotations

import io
import json
import os
import pathlib
import shutil
import sys
from typing import TYPE_CHECKING

import invoke

from scripts import check_public_api_docstrings, check_type_hint_coverage

try:
    from tests.integration.usage_statistics import usage_stats_utils

    is_ge_installed: bool = True
except ModuleNotFoundError:
    is_ge_installed = False

if TYPE_CHECKING:
    from invoke.context import Context

    from great_expectations.experimental.datasources.experimental_base_model import (
        ExperimentalBaseModel,
    )

_CHECK_HELP_DESC = "Only checks for needed changes without writing back. Exit with error code if changes needed."
_EXCLUDE_HELP_DESC = "Exclude files or directories"
_PATH_HELP_DESC = "Target path. (Default: .)"


@invoke.task(
    help={
        "check": _CHECK_HELP_DESC,
        "exclude": _EXCLUDE_HELP_DESC,
        "path": _PATH_HELP_DESC,
    }
)
def sort(
    ctx: Context, path: str = ".", check: bool = False, exclude: str | None = None
):
    """Sort module imports."""
    cmds = ["isort", path]
    if check:
        cmds.append("--check-only")
    if exclude:
        cmds.extend(["--skip", exclude])
    ctx.run(" ".join(cmds), echo=True)


@invoke.task(
    help={
        "check": _CHECK_HELP_DESC,
        "exclude": _EXCLUDE_HELP_DESC,
        "path": _PATH_HELP_DESC,
        "sort": "Disable import sorting. Runs by default.",
    }
)
def fmt(
    ctx: Context,
    path: str = ".",
    sort_: bool = True,
    check: bool = False,
    exclude: str | None = None,
):
    """
    Run code formatter.
    """
    if sort_:
        sort(ctx, path, check=check, exclude=exclude)

    cmds = ["black", path]
    if check:
        cmds.append("--check")
    if exclude:
        cmds.extend(["--exclude", exclude])
    ctx.run(" ".join(cmds), echo=True)


@invoke.task(help={"path": _PATH_HELP_DESC})
def lint(ctx: Context, path: str = "."):
    """Run code linter"""
    cmds = ["flake8", path, "--statistics"]
    ctx.run(" ".join(cmds), echo=True)


@invoke.task(help={"path": _PATH_HELP_DESC})
def upgrade(ctx: Context, path: str = "."):
    """Run code syntax upgrades."""
    cmds = ["pyupgrade", path, "--py3-plus"]
    ctx.run(" ".join(cmds))


@invoke.task(
    help={
        "all_files": "Run hooks against all files, not just the current changes.",
        "diff": "Show the diff of changes on hook failure.",
        "sync": "Re-install the latest git hooks.",
    }
)
def hooks(
    ctx: Context, all_files: bool = False, diff: bool = False, sync: bool = False
):
    """Run and manage pre-commit hooks."""
    cmds = ["pre-commit", "run"]
    if diff:
        cmds.append("--show-diff-on-failure")
    if all_files:
        cmds.extend(["--all-files"])
    else:
        # used in CI - runs faster and only checks files that have changed
        cmds.extend(["--from-ref", "origin/HEAD", "--to-ref", "HEAD"])

    ctx.run(" ".join(cmds))

    if sync:
        print("  Re-installing hooks ...")
        ctx.run(" ".join(["pre-commit", "uninstall"]), echo=True)
        ctx.run(" ".join(["pre-commit", "install"]), echo=True)


@invoke.task(aliases=["docstring"], iterable=("paths",))
def docstrings(ctx: Context, paths: list[str] | None = None):
    """
    Check public API docstrings.

    Optionally pass a directory or file.
    To pass multiple items:
        invoke docstrings -p=great_expectations/core -p=great_expectations/util.py
    """
    scripts_path = pathlib.Path.cwd().parent.parent / "scripts"
    sys.path.append(str(scripts_path))

    if paths:
        select_paths = [pathlib.Path(p) for p in paths]
    else:
        select_paths = None
    try:
        check_public_api_docstrings.main(select_paths=select_paths)
    except AssertionError as err:
        raise invoke.Exit(
            message=f"{err}\n\nGenerated with {check_public_api_docstrings.__file__}",
            code=1,
        )


@invoke.task(aliases=["type-cov"])  # type: ignore
def type_coverage(ctx: Context):
    """
    Check total type-hint coverage compared to `develop`.
    """
    try:
        check_type_hint_coverage.main()
    except AssertionError as err:
        raise invoke.Exit(
            message=f"{err}\n\n  See {check_type_hint_coverage.__file__}", code=1
        )


@invoke.task(
    aliases=["types"],
    iterable=["packages"],
    help={
        "packages": "One or more `great_expectatations` sub-packages to type-check with mypy.",
        "install-types": "Automatically install any needed types from `typeshed`.",
        "daemon": "Run mypy in daemon mode with faster analysis."
        " The daemon will be started and re-used for subsequent calls."
        " For detailed usage see `dmypy --help`.",
        "clear-cache": "Clear the local mypy cache directory.",
    },
)
def type_check(
    ctx: Context,
    packages: list[str],
    install_types: bool = False,
    pretty: bool = False,
    warn_unused_ignores: bool = False,
    daemon: bool = False,
    clear_cache: bool = False,
    report: bool = False,
    ci: bool = False,
):
    """Run mypy static type-checking on select packages."""
    mypy_cache = pathlib.Path(".mypy_cache")

    if ci:
        mypy_cache.mkdir(exist_ok=True)
        print(f"  mypy cache {mypy_cache.absolute()}")

        type_check(
            ctx,
            packages,
            install_types=True,
            pretty=pretty,
            warn_unused_ignores=True,
            daemon=daemon,
            clear_cache=clear_cache,
            report=report,
            ci=False,
        )
        return  # don't run twice

    if clear_cache:
        print(f"  Clearing {mypy_cache} ... ", end="")
        try:
            shutil.rmtree(mypy_cache)
            print("✅"),
        except FileNotFoundError as exc:
            print(f"❌\n  {exc}")

    if daemon:
        bin = "dmypy run --"
    else:
        bin = "mypy"

    ge_pkgs = [f"great_expectations.{p}" for p in packages]
    cmds = [
        bin,
        *ge_pkgs,
    ]
    if install_types:
        cmds.extend(["--install-types", "--non-interactive"])
    if daemon:
        # see related issue https://github.com/python/mypy/issues/9475
        cmds.extend(["--follow-imports=normal"])
    if report:
        cmds.extend(["--txt-report", "type_cov", "--html-report", "type_cov"])
    if pretty:
        cmds.extend(["--pretty"])
    if warn_unused_ignores:
        cmds.extend(["--warn-unused-ignores"])
    # use pseudo-terminal for colorized output
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(aliases=["get-stats"])
def get_usage_stats_json(ctx: Context):
    """
    Dump usage stats event examples to json file
    """
    if not is_ge_installed:
        raise invoke.Exit(
            message="This invoke task requires Great Expecations to be installed in the environment. Please try again.",
            code=1,
        )

    events = usage_stats_utils.get_usage_stats_example_events()
    version = usage_stats_utils.get_gx_version()

    outfile = f"v{version}_example_events.json"
    with open(outfile, "w") as f:
        json.dump(events, f)

    print(f"File written to '{outfile}'.")


@invoke.task(pre=[get_usage_stats_json], aliases=["move-stats"])
def mv_usage_stats_json(ctx: Context):
    """
    Use databricks-cli lib to move usage stats event examples to dbfs:/
    """
    version = usage_stats_utils.get_gx_version()
    outfile = f"v{version}_example_events.json"
    cmd = "databricks fs cp --overwrite {0} dbfs:/schemas/{0}"
    cmd = cmd.format(outfile)
    ctx.run(cmd)
    print(f"'{outfile}' copied to dbfs.")


UNIT_TEST_DEFAULT_TIMEOUT: float = 2.0


@invoke.task(
    aliases=["test"],
    help={
        "unit": "Runs tests marked with the 'unit' marker. Default behavior.",
        "integration": "Runs integration tests and exclude unit-tests. By default only unit tests are run.",
        "ignore-markers": "Don't exclude any test by not passing any markers to pytest.",
        "slowest": "Report on the slowest n number of tests",
        "ci": "execute tests assuming a CI environment. Publish XML reports for coverage reporting etc.",
        "timeout": f"Fails unit-tests if calls take longer than this value. Default {UNIT_TEST_DEFAULT_TIMEOUT} seconds",
        "html": "Create html coverage report",
        "package": "Run tests on a specific package. Assumes there is a `tests/<PACKAGE>` directory of the same name.",
        "full-cov": "Show coverage report on the entire `great_expectations` package regardless of `--package` param.",
    },
)
def tests(
    ctx: Context,
    unit: bool = True,
    integration: bool = False,
    ignore_markers: bool = False,
    ci: bool = False,
    html: bool = False,
    cloud: bool = True,
    slowest: int = 5,
    timeout: float = UNIT_TEST_DEFAULT_TIMEOUT,
    package: str | None = None,
    full_cov: bool = False,
):
    """
    Run tests. Runs unit tests by default.

    Use `invoke tests -p=<TARGET_PACKAGE>` to run tests on a particular package and measure coverage (or lack thereof).
    """
    markers = []
    if integration:
        markers += ["integration"]
        unit = False
    markers += ["unit" if unit else "not unit"]

    marker_text = " and ".join(markers)

    cov_param = "--cov=great_expectations"
    if package and not full_cov:
        cov_param += f"/{package.replace('.', '/')}"

    cmds = [
        "pytest",
        f"--durations={slowest}",
        cov_param,
        "--cov-report term",
        "-vv",
    ]
    if not ignore_markers:
        cmds += ["-m", f"'{marker_text}'"]
    if unit and not ignore_markers:
        try:
            import pytest_timeout  # noqa: F401

            cmds += [f"--timeout={timeout}"]
        except ImportError:
            print("`pytest-timeout` is not installed, cannot use --timeout")

    if cloud:
        cmds += ["--cloud"]
    if ci:
        cmds += ["--cov-report", "xml"]
    if html:
        cmds += ["--cov-report", "html"]
    if package:
        cmds += [f"tests/{package.replace('.', '/')}"]  # allow `foo.bar`` format
    ctx.run(" ".join(cmds), echo=True, pty=True)


PYTHON_VERSION_DEFAULT: float = 3.8


@invoke.task(
    help={
        "name": "Docker image name.",
        "tag": "Docker image tag.",
        "build": "If True build the image, otherwise run it. Defaults to False.",
        "detach": "Run container in background and print container ID. Defaults to False.",
        "py": f"version of python to use. Default is {PYTHON_VERSION_DEFAULT}",
        "cmd": "Command for docker image. Default is bash.",
    }
)
def docker(
    ctx: Context,
    name: str = "gx38local",
    tag: str = "latest",
    build: bool = False,
    detach: bool = False,
    cmd: str = "bash",
    py: float = PYTHON_VERSION_DEFAULT,
):
    """
    Build or run gx docker image.
    """

    _exit_with_error_if_not_in_repo_root(task_name="docker")

    filedir = os.path.realpath(os.path.dirname(os.path.realpath(__file__)))

    cmds = ["docker"]

    if build:
        cmds.extend(
            [
                "buildx",
                "build",
                "-f",
                "docker/Dockerfile.tests",
                f"--tag {name}:{tag}",
                *[
                    f"--build-arg {arg}"
                    for arg in ["SOURCE=local", f"PYTHON_VERSION={py}"]
                ],
                ".",
            ]
        )

    else:
        cmds.append("run")
        if detach:
            cmds.append("--detach")
        cmds.extend(
            [
                "-it",
                "--rm",
                "--mount",
                f"type=bind,source={filedir},target=/great_expectations",
                "-w",
                "/great_expectations",
                f"{name}:{tag}",
                f"{cmd}",
            ]
        )

    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    aliases=("schema",),
    help={
        "type": "Simple type name for a registered ZEP `DataAsset` or `Datasource` class. Or '--list'",
        "save_path": (
            "Filepath to write the schema to. Will overwrite or create the file if it does not exist."
            " If not provided the schema will be sent to the console."
        ),
        "indent": "Indent size for nested json objects. Default: 4",
    },
)
def type_schema(
    ctx: Context,
    type: str,
    save_path: str | pathlib.Path | None = None,
    indent: int = 4,
):
    """
    Show the jsonschema for a given ZEP `type`

    Example: invoke schema sqlite

    --list to show all available types
    """
    from great_expectations.experimental.datasources.sources import _SourceFactories

    buffer = io.StringIO()

    if type == "--list":
        buffer.write(
            "--------------------\nRegistered ZEP types\n--------------------\n"
        )
        buffer.write("\t" + "\n\t".join(_SourceFactories.type_lookup.type_names()))
    elif type == "--help" or type == "-h":
        ctx.run("invoke --help schema")
    else:
        try:
            model: ExperimentalBaseModel = _SourceFactories.type_lookup[type]
            buffer.write(model.schema_json(indent=indent))
        except KeyError:
            raise invoke.Exit(
                f"No '{type}' type found. Try 'invoke schema --list' to see available types",
                code=1,
            )

    text: str = buffer.getvalue()
    if save_path:
        save_path = pathlib.Path(save_path).resolve()
        save_path.write_text(text)
        print(f"'{type}' schema written to {save_path}")
    else:
        print(text)


def _exit_with_error_if_not_in_repo_root(task_name: str):
    """Exit if the command was not run from the repository root."""
    filedir = os.path.realpath(os.path.dirname(os.path.realpath(__file__)))
    curdir = os.path.realpath(os.getcwd())
    exit_message = f"The {task_name} task must be invoked from the same directory as the tasks.py file at the top of the repo."
    if filedir != curdir:
        raise invoke.Exit(
            exit_message,
            code=1,
        )
