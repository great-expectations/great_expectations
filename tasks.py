"""
PyInvoke developer task file
https://www.pyinvoke.org/

These tasks can be run using `invoke <NAME>` or `inv <NAME>` from the project root.

To show all available tasks `invoke --list`

To show task help page `invoke <NAME> --help`
"""

from __future__ import annotations

import importlib
import logging
import os
import pathlib
import pkgutil
import shutil
import sys
from collections.abc import Generator, Mapping, Sequence
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, NamedTuple, Union

import invoke

from docs.sphinx_api_docs_source import check_public_api_docstrings, public_api_report
from docs.sphinx_api_docs_source.build_sphinx_api_docs import SphinxInvokeDocsBuilder

if TYPE_CHECKING:
    from invoke.context import Context
    from typing_extensions import Literal


LOGGER = logging.getLogger(__name__)

GX_ROOT_DIR: Final = pathlib.Path(__file__).parent
GX_PACKAGE_DIR: Final = GX_ROOT_DIR / "great_expectations"
REQS_DIR: Final = GX_ROOT_DIR / "reqs"

_CHECK_HELP_DESC = (
    "Only checks for needed changes without writing back. Exit with error code if changes needed."
)
_EXCLUDE_HELP_DESC = "Exclude files or directories"
_PATH_HELP_DESC = "Target path. (Default: .)"
# https://www.pyinvoke.org/faq.html?highlight=pty#why-is-my-command-behaving-differently-under-invoke-versus-being-run-by-hand
_PTY_HELP_DESC = "Whether or not to use a pseudo terminal"


@invoke.task(
    help={
        "check": _CHECK_HELP_DESC,
        "exclude": _EXCLUDE_HELP_DESC,
        "path": _PATH_HELP_DESC,
        "isort": "Use `isort` to sort packages. Default behavior.",
        "ruff": (
            "Use `ruff` instead of `isort` to sort imports."
            " This will eventually become the default."
        ),
        "pty": _PTY_HELP_DESC,
    }
)
def sort(
    ctx: Context,
    path: str = ".",
    check: bool = False,
    exclude: str | None = None,
    ruff: bool = False,  # isort is the current default
    isort: bool = False,
    pty: bool = True,
):
    """Sort module imports."""
    if ruff and isort:
        raise invoke.Exit("cannot use both `--ruff` and `--isort`", code=1)  # noqa: TRY003
    if not isort:
        cmds = [
            "ruff",
            "check",
            path,
            "--select I",
            "--diff" if check else "--fix",
        ]
        if exclude:
            cmds.extend(["--extend-exclude", exclude])
    else:
        cmds = ["isort", path]
        if check:
            cmds.append("--check-only")
        if exclude:
            cmds.extend(["--skip", exclude])
    ctx.run(" ".join(cmds), echo=True, pty=pty)


@invoke.task(
    aliases=("fmt",),
    help={
        "check": _CHECK_HELP_DESC,
        "exclude": _EXCLUDE_HELP_DESC,
        "path": _PATH_HELP_DESC,
        "sort": "Disable import sorting. Runs by default.",
        "pty": _PTY_HELP_DESC,
    },
)
def format(
    ctx: Context,
    path: str = ".",
    sort_: bool = True,
    check: bool = False,
    exclude: str | None = None,
    pty: bool = True,
):
    """
    Run code formatter.
    """
    if sort_:
        sort(ctx, path, check=check, exclude=exclude, pty=pty)

    cmds = ["ruff", "format", path]
    if check:
        cmds.append("--check")
    if exclude:
        cmds.extend(["--exclude", exclude])
    ctx.run(" ".join(cmds), echo=True, pty=pty)


@invoke.task(
    help={
        "path": _PATH_HELP_DESC,
        "fmt": "Disable formatting. Runs by default.",
        "fix": "Attempt to automatically fix lint violations.",
        "unsafe-fixes": "Enable potentially unsafe fixes.",
        "watch": "Run in watch mode by re-running whenever files change.",
        "pty": _PTY_HELP_DESC,
    }
)
def lint(
    ctx: Context,
    path: str = ".",
    fmt_: bool = True,
    fix: bool = False,
    unsafe_fixes: bool = False,
    output_format: Literal["full", "concise", "github"] | None = None,
    watch: bool = False,
    pty: bool = True,
):
    """Run formatter (ruff format) and linter (ruff)"""
    if fmt_:
        format(ctx, path, check=not fix, pty=pty)

    # Run code linter (ruff)
    cmds = ["ruff", "check", path]
    if fix:
        cmds.append("--fix")
    if unsafe_fixes:
        cmds.append("--unsafe-fixes")
    if watch:
        cmds.append("--watch")
    if output_format:
        cmds.append(f"--output-format={output_format}")
    elif os.getenv("GITHUB_ACTIONS"):
        cmds.append("--output-format=github")
    ctx.run(" ".join(cmds), echo=True, pty=pty)


@invoke.task(help={"path": _PATH_HELP_DESC, "safe-only": "Only apply 'safe' fixes."})
def fix(ctx: Context, path: str = ".", safe_only: bool = False):
    """
    Automatically fix all possible code issues.
    Applies unsafe fixes by default.
    https://docs.astral.sh/ruff/linter/#fix-safety
    """
    unsafe_fixes = not safe_only
    lint(ctx, path=path, fmt_=False, fix=True, unsafe_fixes=unsafe_fixes)
    format(ctx, path=path, check=False, sort_=False)


@invoke.task(help={"path": _PATH_HELP_DESC})
def upgrade(ctx: Context, path: str = "."):
    """Run code syntax upgrades."""
    cmds = ["ruff", path, "--select", "UP", "--fix"]
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(
    help={
        "all_files": "Run hooks against all files, not just the current changes.",
        "diff": "Show the diff of changes on hook failure.",
        "sync": "Re-install the latest git hooks.",
    }
)
def hooks(ctx: Context, all_files: bool = False, diff: bool = False, sync: bool = False):
    """Run and manage pre-commit hooks."""
    cmds = ["pre-commit", "run"]
    if diff:
        cmds.append("--show-diff-on-failure")
    if all_files:
        cmds.extend(["--all-files"])
    else:
        # used in CI - runs faster and only checks files that have changed
        cmds.extend(["--from-ref", "origin/HEAD", "--to-ref", "HEAD"])

    ctx.run(" ".join(cmds), echo=True, pty=True)

    if sync:
        print("  Re-installing hooks ...")
        ctx.run(" ".join(["pre-commit", "uninstall"]), echo=True)
        ctx.run(" ".join(["pre-commit", "install"]), echo=True)


@invoke.task(aliases=("docstring",), iterable=("paths",))
def docstrings(ctx: Context, paths: list[str] | None = None):
    """
    Check public API docstrings.

    Optionally pass a directory or file.
    To pass multiple items:
        invoke docstrings -p=great_expectations/core -p=great_expectations/util.py
    """

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


@invoke.task()
def marker_coverage(
    ctx: Context,
):
    pytest_cmds = ["pytest", "--verify-marker-coverage-and-exit"]
    ctx.run(" ".join(pytest_cmds), echo=True, pty=True)


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
        "check-stub-sources": "Check the implementation `.py` files for any `.pyi`"
        " stub files in `great_expectations`."
        " By default `mypy` will not check implementation files if a `.pyi` stub file exists."
        " This should be run in CI in addition to the normal type-checking step.",
        "python-version": "Type check as if running a specific python version."
        " Default to version set in pyproject.toml",
    },
)
def type_check(  # noqa: C901, PLR0912
    ctx: Context,
    packages: list[str],
    install_types: bool = False,
    pretty: bool = False,
    warn_unused_ignores: bool = False,
    disallow_untyped_decorators: bool = True,
    daemon: bool = False,
    clear_cache: bool = False,
    report: bool = False,
    check_stub_sources: bool = False,
    ci: bool = False,
    python_version: str = "",
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
            disallow_untyped_decorators=True,
            daemon=daemon,
            clear_cache=clear_cache,
            report=report,
            check_stub_sources=check_stub_sources,
            ci=False,
            python_version=python_version,
        )
        return  # don't run twice

    if clear_cache:
        print(f"  Clearing {mypy_cache} ... ", end="")
        try:
            shutil.rmtree(mypy_cache)
            print("✅")
        except FileNotFoundError as exc:
            print(f"❌\n  {exc}")

    if daemon:
        bin = "dmypy run --"
    else:
        bin = "mypy"

    cmds = [bin]

    ge_pkgs = [f"great_expectations.{p}" for p in packages]

    if check_stub_sources:
        # see --help docs for explanation of this flag
        for stub_file in GX_PACKAGE_DIR.glob("**/*.pyi"):
            source_file = stub_file.with_name(  # TODO:py3.9 .with_stem()
                f"{stub_file.name[:-1]}"
            )
            relative_path = source_file.relative_to(GX_ROOT_DIR)
            ge_pkgs.append(str(relative_path))
        # following imports here can cause mutually exclusive import errors with normal type-checking  # noqa: E501
        cmds.append("--follow-imports=silent")

    cmds.extend(ge_pkgs)

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
    if disallow_untyped_decorators:
        cmds.extend(["--disallow-untyped-decorators"])
    if python_version:
        cmds.extend(["--python-version", python_version])
    # use pseudo-terminal for colorized output
    ctx.run(" ".join(cmds), echo=True, pty=True)


UNIT_TEST_DEFAULT_TIMEOUT: float = (
    2.0  # TODO: revert the timeout back to 1.5 or lower after resolving arc issues
)


@invoke.task(
    aliases=["test"],
    help={
        "unit": "Runs tests marked with the 'unit' marker. Default behavior.",
        "cloud": "Runs tests marked with the 'cloud' marker. Default behavior.",
        "ignore-markers": "Don't exclude any test by not passing any markers to pytest.",
        "slowest": "Report on the slowest n number of tests",
        "ci": "execute tests assuming a CI environment. Publish XML reports for coverage reporting etc.",  # noqa: E501
        "timeout": f"Fails unit-tests if calls take longer than this value. Default {UNIT_TEST_DEFAULT_TIMEOUT} seconds",  # noqa: E501
        "html": "Create html coverage report",
        "package": "Run tests on a specific package. Assumes there is a `tests/<PACKAGE>` directory of the same name.",  # noqa: E501
        "full-cov": "Show coverage report on the entire `great_expectations` package regardless of `--package` param.",  # noqa: E501
    },
)
def tests(  # noqa: C901
    ctx: Context,
    unit: bool = True,
    ignore_markers: bool = False,
    ci: bool = False,
    html: bool = False,
    cloud: bool = True,
    slowest: int = 5,
    timeout: float = UNIT_TEST_DEFAULT_TIMEOUT,
    package: str | None = None,
    full_cov: bool = False,
    verbose: bool = False,
):
    """
    Run tests. Runs unit tests by default.

    Use `invoke tests -p=<TARGET_PACKAGE>` to run tests on a particular package and measure coverage (or lack thereof).

    See also, the newer `invoke ci-tests --help`.
    """  # noqa: E501
    markers = []
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
        "-rEf",  # show extra test summary info for errors & failed tests
    ]
    if verbose:
        cmds.append("-vv")
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
        "target": "Set the target build stage to build.",
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
    target: str | None = None,
):
    """
    Build or run gx docker image.
    """

    _exit_with_error_if_not_in_repo_root(task_name="docker")

    filedir = os.path.realpath(
        os.path.dirname(os.path.realpath(__file__))  # noqa: PTH120
    )

    cmds = ["docker"]

    if build:
        cmds.extend(
            [
                "buildx",
                "build",
                "-f",
                "docker/Dockerfile.tests",
                f"--tag {name}:{tag}",
                *[f"--build-arg {arg}" for arg in ["SOURCE=local", f"PYTHON_VERSION={py}"]],
                ".",
            ]
        )
        if target:
            cmds.extend(["--target", target])

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
    aliases=("schema", "schemas"),
    help={
        "sync": "Update the json schemas",
        "indent": "Indent size for nested json objects. Default: 4",
        "clean": "Delete all schema files and sub directories."
        " Can be combined with `--sync` to reset the /schemas dir and remove stale schemas",
    },
)
def type_schema(  # noqa: C901 - too complex
    ctx: Context,
    sync: bool = False,
    clean: bool = False,
    indent: int = 4,
):
    """
    Show all the json schemas for Fluent Datasources & DataAssets

    Generate json schema for each Datasource & DataAsset with `--sync`.
    """
    import pandas

    from great_expectations.datasource.fluent import (
        _PANDAS_SCHEMA_VERSION,
        BatchRequest,
        DataAsset,
        Datasource,
    )
    from great_expectations.datasource.fluent.sources import (
        _iter_all_registered_types,
    )
    from great_expectations.expectations import core

    data_source_schema_dir_root: Final[pathlib.Path] = (
        GX_PACKAGE_DIR / "datasource" / "fluent" / "schemas"
    )
    expectation_schema_dir_root: Final[pathlib.Path] = (
        GX_PACKAGE_DIR / "expectations" / "core" / "schemas"
    )
    if clean:
        shutil.rmtree(data_source_schema_dir_root)
        shutil.rmtree(expectation_schema_dir_root)

    data_source_schema_dir_root.mkdir(exist_ok=True)
    expectation_schema_dir_root.mkdir(exist_ok=True)

    datasource_dir: pathlib.Path = data_source_schema_dir_root
    expectation_dir: pathlib.Path = expectation_schema_dir_root

    if not sync:
        print("--------------------\nRegistered Fluent types\n--------------------\n")

    name_model: list[tuple[str, type[Datasource | BatchRequest | DataAsset]]] = [
        ("BatchRequest", BatchRequest),
        (Datasource.__name__, Datasource),
        *_iter_all_registered_types(),
    ]

    # handle data sources
    for name, model in name_model:
        if issubclass(model, Datasource):
            datasource_dir = data_source_schema_dir_root.joinpath(model.__name__)
            datasource_dir.mkdir(exist_ok=True)
            schema_dir = data_source_schema_dir_root
            print("-" * shutil.get_terminal_size()[0])
        else:
            schema_dir = datasource_dir
            print("  ", end="")

        if not sync:
            print(f"{name} - {model.__name__}.json")
            continue

        if (
            datasource_dir.name.startswith("Pandas")
            and pandas.__version__ != _PANDAS_SCHEMA_VERSION
        ):
            print(
                f"🙈  {name} - was generated with pandas"
                f" {_PANDAS_SCHEMA_VERSION} but you have {pandas.__version__}; skipping"
            )
            continue

        try:
            schema_path = schema_dir.joinpath(f"{model.__name__}.json")
            json_str: str = model.schema_json(indent=indent) + "\n"

            if schema_path.exists():
                if json_str == schema_path.read_text():
                    print(f"✅  {name} - {schema_path.name} unchanged")
                    continue

            schema_path.write_text(json_str)
            print(f"🔃  {name} - {schema_path.name} schema updated")
        except TypeError as err:
            print(f"❌  {name} - Could not sync schema - {type(err).__name__}:{err}")

    # handle expectations
    supported_expectations = [
        core.ExpectColumnValuesToBeNull,
        core.ExpectColumnValuesToNotBeNull,
        core.ExpectColumnValuesToBeUnique,
        core.ExpectColumnValuesToBeInSet,
        core.ExpectColumnMaxToBeBetween,
        core.ExpectColumnMeanToBeBetween,
        core.ExpectColumnMedianToBeBetween,
        core.ExpectColumnMinToBeBetween,
        core.ExpectColumnValuesToBeInTypeList,
        core.ExpectColumnValuesToBeOfType,
        core.ExpectTableColumnsToMatchOrderedList,
        core.ExpectTableRowCountToBeBetween,
        core.ExpectTableRowCountToEqual,
        core.ExpectColumnPairValuesToBeEqual,
        core.ExpectMulticolumnSumToEqual,
        core.ExpectCompoundColumnsToBeUnique,
        core.ExpectSelectColumnValuesToBeUniqueWithinRecord,
        core.ExpectColumnPairValuesAToBeGreaterThanB,
        core.ExpectColumnToExist,
        core.ExpectTableColumnCountToEqual,
        core.ExpectTableColumnsToMatchSet,
        core.ExpectTableColumnCountToBeBetween,
        core.ExpectTableRowCountToEqualOtherTable,
        core.ExpectColumnPairValuesToBeInSet,
        core.ExpectColumnProportionOfUniqueValuesToBeBetween,
        core.ExpectColumnUniqueValueCountToBeBetween,
        core.ExpectColumnDistinctValuesToBeInSet,
        core.ExpectColumnDistinctValuesToContainSet,
        core.ExpectColumnDistinctValuesToEqualSet,
        core.ExpectColumnMostCommonValueToBeInSet,
        core.ExpectColumnStdevToBeBetween,
        core.ExpectColumnSumToBeBetween,
        core.ExpectColumnKLDivergenceToBeLessThan,
        core.ExpectColumnQuantileValuesToBeBetween,
        core.ExpectColumnValueLengthsToBeBetween,
        core.ExpectColumnValueLengthsToEqual,
        core.ExpectColumnValueZScoresToBeLessThan,
        core.ExpectColumnValuesToBeBetween,
        core.ExpectColumnValuesToMatchLikePattern,
        core.ExpectColumnValuesToMatchLikePatternList,
        core.ExpectColumnValuesToMatchRegex,
        core.ExpectColumnValuesToMatchRegexList,
        core.ExpectColumnValuesToNotBeInSet,
        core.ExpectColumnValuesToNotBeNull,
        core.ExpectColumnValuesToNotMatchLikePattern,
        core.ExpectColumnValuesToNotMatchLikePatternList,
        core.ExpectColumnValuesToNotMatchRegex,
        core.ExpectColumnValuesToNotMatchRegexList,
        core.UnexpectedRowsExpectation,
    ]
    for x in supported_expectations:
        schema_path = expectation_dir.joinpath(f"{x.__name__}.json")
        json_str = x.schema_json(indent=indent) + "\n"  # type: ignore[attr-defined] # FIXME low priority
        if sync:
            schema_path.write_text(json_str)
            print(f"🔃  {x.__name__}.json updated")

    raise invoke.Exit(code=0)


def _exit_with_error_if_not_in_repo_root(task_name: str):
    """Exit if the command was not run from the repository root."""
    filedir = os.path.realpath(
        os.path.dirname(os.path.realpath(__file__))  # noqa: PTH120
    )
    curdir = os.path.realpath(os.getcwd())  # noqa: PTH109
    exit_message = f"The {task_name} task must be invoked from the same directory as the tasks.py file at the top of the repo."  # noqa: E501
    if filedir != curdir:
        raise invoke.Exit(
            exit_message,
            code=1,
        )


@invoke.task
def api_docs(ctx: Context):
    """Build api documentation."""

    repo_root = pathlib.Path(__file__).parent

    _exit_with_error_if_not_run_from_correct_dir(task_name="docs", correct_dir=repo_root)
    sphinx_api_docs_source_dir = repo_root / "docs" / "sphinx_api_docs_source"

    doc_builder = SphinxInvokeDocsBuilder(
        ctx=ctx, api_docs_source_path=sphinx_api_docs_source_dir, repo_root=repo_root
    )

    doc_builder.build_docs()


@invoke.task(
    name="docs",
    help={
        "build": "Build docs via yarn build instead of serve via yarn start. Default False.",
        "start": "Only run yarn start, do not process versions. For example if you have already run invoke docs and just want to serve docs locally for editing.",  # noqa: E501
        "lint": "Run the linter",
        "clear": "Delete the docs' generated assets, caches, and build artifacts.",
    },
)
def docs(
    ctx: Context,
    build: bool = False,
    start: bool = False,
    lint: bool = False,
    version: str | None = None,
    clear: bool = False,
):
    """Build documentation site, including api documentation and earlier doc versions. Note: Internet access required to download earlier versions."""  # noqa: E501
    from packaging.version import parse as parse_version

    from docs.docs_build import DocsBuilder

    repo_root = pathlib.Path(__file__).parent

    _exit_with_error_if_not_run_from_correct_dir(task_name="docs", correct_dir=repo_root)

    print("Running invoke docs from:", repo_root)
    old_cwd = pathlib.Path.cwd()
    docusaurus_dir = repo_root / "docs/docusaurus"
    os.chdir(docusaurus_dir)

    if lint:
        ctx.run(" ".join(["yarn lint"]), echo=True)
    elif version:
        docs_builder = DocsBuilder(ctx, docusaurus_dir)
        docs_builder.create_version(version=parse_version(version))
    elif start:
        ctx.run(" ".join(["yarn start"]), echo=True)
    elif clear:
        ctx.run(" ".join(["yarn", "clear"]), echo=True)
    else:
        docs_builder = DocsBuilder(ctx, docusaurus_dir)
        print("Making sure docusaurus dependencies are installed.")
        ctx.run(" ".join(["yarn install"]), echo=True)

        if build:
            print("Running build_docs from:", docusaurus_dir)
            docs_builder.build_docs()
        else:
            print("Running build_docs_locally from:", docusaurus_dir)
            docs_builder.build_docs_locally()

    os.chdir(old_cwd)


@invoke.task(
    name="public-api",
    help={
        "write_to_file": "Write items to be addressed to public_api_report.txt, default False",
    },
)
def public_api_task(
    ctx: Context,
    write_to_file: bool = False,
):
    """Generate a report to determine the state of our Public API. Lists classes, methods and functions that are used in examples in our documentation, and any manual includes or excludes (see public_api_report.py). Items listed when generating this report need the @public_api decorator (and a good docstring) or to be excluded from consideration if they are not applicable to our Public API."""  # noqa: E501

    repo_root = pathlib.Path(__file__).parent

    _exit_with_error_if_not_run_from_correct_dir(task_name="public-api", correct_dir=repo_root)

    # Docs folder is not reachable from install of Great Expectations
    api_docs_dir = repo_root / "docs" / "sphinx_api_docs_source"
    sys.path.append(str(api_docs_dir.resolve()))

    public_api_report.generate_public_api_report(write_to_file=write_to_file)


def _exit_with_error_if_not_run_from_correct_dir(
    task_name: str, correct_dir: Union[pathlib.Path, None] = None
) -> None:
    """Exit if the command was not run from the correct directory."""
    if not correct_dir:
        correct_dir = pathlib.Path(__file__).parent
    curdir = pathlib.Path.cwd()
    exit_message = (
        f"The {task_name} task must be invoked from the same directory as the tasks.py file."
    )
    if correct_dir != curdir:
        raise invoke.Exit(
            exit_message,
            code=1,
        )


@invoke.task(
    aliases=("links",),
    help={"skip_external": "Skip external link checks (is slow), default is True"},
)
def link_checker(ctx: Context, skip_external: bool = True):
    """Checks the Docusaurus docs for broken links"""
    import docs.checks.docs_link_checker as checker

    path = pathlib.Path("docs/docusaurus/docs")
    docs_root = pathlib.Path("docs/docusaurus/docs")
    static_root = pathlib.Path("docs/docusaurus/static")
    site_prefix: str = "docs"
    static_prefix: str = "static"

    code, message = checker.scan_docs(
        path=path,
        docs_root=docs_root,
        static_root=static_root,
        site_prefix=site_prefix,
        static_prefix=static_prefix,
        skip_external=skip_external,
    )
    raise invoke.Exit(message, code)


@invoke.task(
    aliases=("automerge",),
)
def show_automerges(ctx: Context):
    """Show github pull requests currently in automerge state."""
    import requests

    url = "https://api.github.com/repos/great-expectations/great_expectations/pulls"
    response = requests.get(
        url,
        params={  # type: ignore[arg-type]
            "state": "open",
            "sort": "updated",
            "direction": "desc",
            "per_page": 50,
        },
    )
    LOGGER.debug(f"{response.request.method} {response.request.url} - {response}")

    if response.status_code != requests.codes.ok:
        print(f"Error: {response.reason}\n{pf(response.json(), depth=2)}")
        response.raise_for_status()

    pr_details = response.json()
    LOGGER.debug(pf(pr_details, depth=2))

    if automerge_prs := tuple(x for x in pr_details if x["auto_merge"]):
        print(f"\tAutomerge PRs: {len(automerge_prs)}")
        for i, pr in enumerate(automerge_prs, start=1):
            print(f"{i}. @{pr['user']['login']} {pr['title']} {pr['html_url']}")
    else:
        print("\tNo PRs set to automerge")


class TestDependencies(NamedTuple):
    requirement_files: tuple[str, ...]
    services: tuple[str, ...] = tuple()
    extra_pytest_args: tuple[  # TODO: remove this once remove the custom flagging system
        str, ...
    ] = tuple()


MARKER_DEPENDENCY_MAP: Final[Mapping[str, TestDependencies]] = {
    "athena": TestDependencies(("reqs/requirements-dev-athena.txt",)),
    "aws_deps": TestDependencies(("reqs/requirements-dev-lite.txt",)),
    "clickhouse": TestDependencies(("reqs/requirements-dev-clickhouse.txt",)),
    "cloud": TestDependencies(
        (
            "reqs/requirements-dev-cloud.txt",
            "reqs/requirements-dev-snowflake.txt",
            "reqs/requirements-dev-spark.txt",
        ),
        services=(
            "mercury",
            "spark",
        ),
        extra_pytest_args=("--cloud",),
    ),
    "databricks": TestDependencies(
        requirement_files=("reqs/requirements-dev-databricks.txt",),
    ),
    "docs-basic": TestDependencies(
        # these installs are handled by the CI
        requirement_files=(
            "reqs/requirements-dev-test.txt",
            "reqs/requirements-dev-mssql.txt",
            "reqs/requirements-dev-mysql.txt",
            "reqs/requirements-dev-postgresql.txt",
            # "Deprecated API features detected" warning/error for test_docs[split_data_on_whole_table_bigquery] when pandas>=2.0  # noqa: E501
            "reqs/requirements-dev-sqlalchemy2.txt",
            "reqs/requirements-dev-trino.txt",
        ),
        services=("postgresql", "mssql", "mysql", "trino"),
        extra_pytest_args=(
            "--mssql",
            "--mysql",
            "--postgresql",
            "--trino",
            "--docs-tests",
        ),
    ),
    "docs-creds-needed": TestDependencies(
        # these installs are handled by the CI
        requirement_files=(
            "reqs/requirements-dev-test.txt",
            "reqs/requirements-dev-azure.txt",
            "reqs/requirements-dev-bigquery.txt",
            "reqs/requirements-dev-redshift.txt",
            "reqs/requirements-dev-snowflake.txt",
            # "Deprecated API features detected" warning/error for test_docs[split_data_on_whole_table_bigquery] when pandas>=2.0  # noqa: E501
            "reqs/requirements-dev-sqlalchemy2.txt",
        ),
        extra_pytest_args=(
            "--aws",
            "--azure",
            "--bigquery",
            "--redshift",
            "--snowflake",
            "--docs-tests",
        ),
    ),
    "docs-spark": TestDependencies(
        requirement_files=(
            "reqs/requirements-dev-test.txt",
            "reqs/requirements-dev-spark.txt",
        ),
        services=("spark",),
        extra_pytest_args=("--spark", "--docs-tests"),
    ),
    "mssql": TestDependencies(
        ("reqs/requirements-dev-mssql.txt",),
        services=("mssql",),
        extra_pytest_args=("--mssql",),
    ),
    "mysql": TestDependencies(
        ("reqs/requirements-dev-mysql.txt",),
        services=("mysql",),
        extra_pytest_args=("--mysql",),
    ),
    "pyarrow": TestDependencies(("reqs/requirements-dev-arrow.txt",)),
    "postgresql": TestDependencies(
        ("reqs/requirements-dev-postgresql.txt",),
        services=("postgresql",),
        extra_pytest_args=("--postgresql",),
    ),
    "snowflake": TestDependencies(
        requirement_files=("reqs/requirements-dev-snowflake.txt",),
    ),
    "spark": TestDependencies(
        requirement_files=("reqs/requirements-dev-spark.txt",),
        services=("spark",),
        extra_pytest_args=("--spark",),
    ),
    "spark_connect": TestDependencies(
        requirement_files=(
            "reqs/requirements-dev-spark.txt",
            "reqs/requirements-dev-spark-connect.txt",
        ),
        services=("spark",),
        extra_pytest_args=("--spark_connect",),
    ),
    "trino": TestDependencies(
        ("reqs/requirements-dev-trino.txt",),
        services=("trino",),
        extra_pytest_args=("--trino",),
    ),
}


def _add_all_backends_marker(marker_string: str) -> bool:
    # We should generalize this, possibly leveraging MARKER_DEPENDENCY_MAP, but for now
    # right I've hardcoded all the containerized backend services we support in testing.
    return marker_string in [
        "postgresql",
        "mssql",
        "mysql",
        "spark",
        "trino",
    ]


def _tokenize_marker_string(marker_string: str) -> Generator[str, None, None]:
    """_summary_

    Args:
        marker_string (str): _description_

    Yields:
        Generator[str, None, None]: _description_
    """
    tokens = marker_string.split()
    if len(tokens) == 1:
        yield tokens[0]
    elif (
        marker_string
        == "athena or clickhouse or openpyxl or pyarrow or project or sqlite or aws_creds"
    ):
        yield "aws_creds"
        yield "athena"
        yield "clickhouse"
        yield "openpyxl"
        yield "pyarrow"
        yield "project"
        yield "sqlite"
    else:
        raise ValueError(f"Unable to tokenize marker string: {marker_string}")  # noqa: TRY003


def _get_marker_dependencies(markers: str | Sequence[str]) -> list[TestDependencies]:
    if isinstance(markers, str):
        markers = [markers]
    dependencies: list[TestDependencies] = []
    for marker_string in markers:
        for marker_token in _tokenize_marker_string(marker_string):
            if marker_depedencies := MARKER_DEPENDENCY_MAP.get(marker_token):
                LOGGER.debug(f"'{marker_token}' has dependencies")
                dependencies.append(marker_depedencies)
    return dependencies


@invoke.task(
    iterable=["markers", "requirements_dev"],
    help={
        "markers": "Optional marker to install dependencies for. Can be specified multiple times.",
        "requirements_dev": "Short name of `requirements-dev-*.txt` file to install, e.g. test, spark, cloud, etc. Can be specified multiple times.",  # noqa: E501
        "constraints": "Optional flag to install dependencies with constraints, default True",
    },
)
def deps(
    ctx: Context,
    markers: list[str],
    requirements_dev: list[str],
    constraints: bool = True,
    gx_install: bool = False,
    editable_install: bool = False,
):
    """
    Install dependencies for development and testing.

    Specific requirement files needed for a specific test marker can be registered in `MARKER_DEPENDENCY_MAP`,
    `invoke deps` will always check for and use these when installing dependencies.

    If no `markers` or `requirements-dev` are specified, the dev-contrib and
    core requirements are installed.

    Example usage:
    Installing the needed dependencies for running the `external_sqldialect` tests and
    the 'requirements-dev-cloud.txt' dependencies.

    $ invoke deps -m external_sqldialect -r cloud
    """  # noqa: E501
    cmds = ["pip", "install"]
    if editable_install:
        cmds.append("-e .")
    elif gx_install:
        cmds.append(".")

    req_files: list[str] = ["requirements.txt"]

    for test_deps in _get_marker_dependencies(markers):
        req_files.extend(test_deps.requirement_files)

    for name in requirements_dev:
        req_path: pathlib.Path = REQS_DIR / f"requirements-dev-{name}.txt"
        assert req_path.exists(), f"Requirement file {req_path} does not exist"
        req_files.append(str(req_path))

    if not markers and not requirements_dev:
        req_files.append("reqs/requirements-dev-contrib.txt")

    for req_file in req_files:
        cmds.append(f"-r {req_file}")

    if constraints:
        cmds.append("-c constraints-dev.txt")

    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(iterable=["service_names", "up_services", "verbose"])
def docs_snippet_tests(
    ctx: Context,
    marker: str,
    up_services: bool = False,
    verbose: bool = False,
    reports: bool = False,
):
    pytest_cmds = [
        "pytest",
        "-rEf",
    ]
    if reports:
        pytest_cmds.extend(["--cov=great_expectations", "--cov-report=xml"])

    if verbose:
        pytest_cmds.append("-vv")

    for test_deps in _get_marker_dependencies(marker):
        if up_services:
            service(ctx, names=test_deps.services, markers=test_deps.services)

        for extra_pytest_arg in test_deps.extra_pytest_args:
            pytest_cmds.append(extra_pytest_arg)

    pytest_cmds.append("tests/integration/test_script_runner.py")
    ctx.run(" ".join(pytest_cmds), echo=True, pty=True)


@invoke.task(
    help={
        "pty": _PTY_HELP_DESC,
        "reports": "Generate coverage & result reports to be uploaded to codecov",
        "W": "Warnings control",
    },
    iterable=["service_names", "up_services", "verbose"],
)
def ci_tests(  # noqa: C901 - too complex (9)
    ctx: Context,
    marker: str,
    up_services: bool = False,
    restart_services: bool = False,
    verbose: bool = False,
    reports: bool = False,
    slowest: int = 5,
    timeout: float = 0.0,  # 0 indicates no timeout
    xdist: bool = False,
    W: str | None = None,
    pty: bool = True,
):
    """
    Run tests in CI.

    This method looks up the pytest marker provided and runs the tests for that marker,
    as well as looking up any required services, testing dependencies and extra CLI flags
    that are need and starting them if `up_services` is True.

    `up_services` is False by default to avoid starting services which may already be up
    when running tests locally.

    `restart_services` is False by default to avoid always restarting the services.

    Defined this as a new invoke task to avoid some of the baggage of our old test setup.
    """
    pytest_options = [f"--durations={slowest}", "-rEf"]

    if xdist:
        pytest_options.append("-n 4")

    if timeout != 0:
        pytest_options.append(f"--timeout={timeout}")

    if reports:
        pytest_options.extend(
            ["--cov=great_expectations", "--cov-report=xml", "--junitxml=junit.xml"]
        )

    if verbose:
        pytest_options.append("-vv")

    if W:
        # https://docs.python.org/3/library/warnings.html#describing-warning-filters
        pytest_options.append(f"-W={W}")

    for test_deps in _get_marker_dependencies(marker):
        if restart_services or up_services:
            service(
                ctx,
                names=test_deps.services,
                markers=test_deps.services,
                restart_services=restart_services,
                pty=pty,
            )

        for extra_pytest_arg in test_deps.extra_pytest_args:
            pytest_options.append(extra_pytest_arg)

    marker_statement = (
        f"'all_backends or {marker}'" if _add_all_backends_marker(marker) else f"'{marker}'"
    )

    pytest_cmd = ["pytest", "-m", marker_statement] + pytest_options
    ctx.run(" ".join(pytest_cmd), echo=True, pty=pty)


@invoke.task(
    aliases=("services",),
    help={"pty": _PTY_HELP_DESC},
    iterable=["names", "markers"],
)
def service(
    ctx: Context,
    names: Sequence[str],
    markers: Sequence[str],
    restart_services: bool = False,
    pty: bool = True,
):
    """
    Startup a service, by referencing its name directly or by looking up a pytest marker.

    If a marker is specified, the services listed in `MARKER_DEPENDENCY_MAP` will be used.

    If restart_services was passed, the containers will be stopped and re-built.

    Note:
        The main reason this is a separate task is to make it easy to start services
        when running tests locally.
    """
    service_names = set(names)

    if markers:
        for test_deps in _get_marker_dependencies(markers):
            service_names.update(test_deps.services)

    if service_names:
        print(f"  Starting services for {', '.join(service_names)} ...")
        for service_name in service_names:
            cmds = []

            if service_name == "mercury" and os.environ.get("CI") != "true":
                cmds.extend(
                    [
                        "FORCE_NO_ALIAS=true",
                        "assume",
                        "dev",
                        "--exec",
                        "'aws ecr get-login-password --region us-east-1'",
                        "|",
                        "docker",
                        "login",
                        "--username",
                        "AWS",
                        "--password-stdin",
                        "258143015559.dkr.ecr.us-east-1.amazonaws.com",
                        "&&",
                    ]
                )

            if restart_services:
                print(f"  Removing existing containers and building latest for {service_name} ...")
                cmds.extend(
                    [
                        "docker",
                        "compose",
                        "-f",
                        f"assets/docker/{service_name}/docker-compose.yml",
                        "rm",
                        "-fsv",
                        "&&",
                        "docker",
                        "compose",
                        "-f",
                        f"assets/docker/{service_name}/docker-compose.yml",
                        "build",
                        "--pull",
                        "&&",
                    ]
                )

            cmds.extend(
                [
                    "docker",
                    "compose",
                    "-f",
                    f"assets/docker/{service_name}/docker-compose.yml",
                    "up",
                    "-d",
                    "--quiet-pull",
                    "--wait",
                    "--wait-timeout 120",
                ]
            )
            ctx.run(" ".join(cmds), echo=True, pty=pty)
        # TODO: Add healthchecks to services that require this sleep and then remove it.
        #       This is a temporary hack to give services enough time to come up before moving on.
        ctx.run("sleep 15")
    else:
        print("  No matching services to start")


@invoke.task()
def print_public_api(ctx: Context):
    """Prints to STDOUT all of our public api."""
    # Walk the GX package to make sure we import all submodules to ensure we
    # retrieve all things decorated with our public api decorator.
    import great_expectations

    for module_info in pkgutil.walk_packages(["great_expectations"], prefix="great_expectations."):
        importlib.import_module(module_info.name)
    print(great_expectations._docs_decorators.public_api_introspector)
