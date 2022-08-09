"""
PyInvoke developer task file
https://www.pyinvoke.org/

These tasks can be run using `invoke <NAME>` or `inv <NAME>` from the project root.

To show all available tasks `invoke --list`

To show task help page `invoke <NAME> --help`
"""
import json
import pathlib
import shutil

import invoke

from scripts import check_type_hint_coverage

try:
    from tests.integration.usage_statistics import usage_stats_utils

    is_ge_installed: bool = True
except ModuleNotFoundError:
    is_ge_installed = False

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
def sort(ctx, path=".", check=False, exclude=None):
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
def fmt(ctx, path=".", sort_=True, check=False, exclude=None):
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
def lint(ctx, path="."):
    """Run code linter"""
    cmds = ["flake8", path, "--statistics"]
    ctx.run(" ".join(cmds), echo=True)


@invoke.task(help={"path": _PATH_HELP_DESC})
def upgrade(ctx, path="."):
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
def hooks(ctx, all_files=False, diff=False, sync=False):
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


@invoke.task(aliases=["type-cov"])  # type: ignore
def type_coverage(ctx):
    """
    Check total type-hint coverage compared to `develop`.
    """
    try:
        check_type_hint_coverage.main()
    except AssertionError as err:
        raise invoke.Exit(message=str(err), code=1)


# numbers next to each package represent the last known number of typing errors.
# when errors reach 0 please uncomment the module so it becomes type-checked by default.
DEFAULT_PACKAGES_TO_TYPE_CHECK = [
    # "checkpoint",  # 78
    # "cli",  # 237
    # "core",  # 242
    "data_asset",  # 0
    # "data_context",  # 242
    # "data_context/data_context",  # 195
    # "data_context/store", # 83
    "data_context/store/ge_cloud_store_backend.py",  # 0
    "data_context/types",  # 0
    # "datasource",  # 98
    "exceptions",  # 0
    # "execution_engine",  # 109
    # "expectations",  # 462
    "jupyter_ux",  # 0
    # "marshmallow__shade",  # 14
    "profile",  # 0
    # "render",  # 87
    # "rule_based_profiler",  # 469
    "self_check",  # 0
    "types",  # 0
    # "validation_operators", # 47
    # "validator",  # 46
    # util.py # 28
]


@invoke.task(
    aliases=["types"],
    iterable=["packages"],
    help={
        "packages": f"One or more packages to type-check with mypy. (Default: {DEFAULT_PACKAGES_TO_TYPE_CHECK})",
        "show-default-packages": "Print the default packages to type-check and then exit.",
        "install-types": "Automatically install any needed types from `typeshed`.",
        "daemon": "Run mypy in daemon mode with faster analysis."
        " The daemon will be started and re-used for subsequent calls."
        " For detailed usage see `dmypy --help`.",
        "clear-cache": "Clear the local mypy cache directory.",
    },
)
def type_check(
    ctx,
    packages,
    install_types=False,
    show_default_packages=False,
    daemon=False,
    clear_cache=False,
):
    """Run mypy static type-checking on select packages."""
    if clear_cache:
        mypy_cache = pathlib.Path(".mypy_cache")
        print(f"  Clearing {mypy_cache} ... ", end="")
        try:
            shutil.rmtree(mypy_cache)
            print("✅"),
        except FileNotFoundError as exc:
            print(f"❌\n  {exc}")

    if show_default_packages:
        # Use this to keep the Type-checking section of the docs up to date.
        # https://docs.greatexpectations.io/docs/contributing/style_guides/code_style#type-checking
        print("\n".join(DEFAULT_PACKAGES_TO_TYPE_CHECK))
        raise invoke.Exit(code=0)

    if daemon:
        bin = "dmypy run --"
    else:
        bin = "mypy"

    packages = packages or DEFAULT_PACKAGES_TO_TYPE_CHECK
    # once we have sunsetted `type-coverage` and our typing has matured we should define
    # our packages to exclude (if any) in the mypy config file.
    # https://mypy.readthedocs.io/en/stable/config_file.html#confval-exclude
    ge_pkgs = [f"great_expectations/{p}" for p in packages]
    cmds = [
        bin,
        *ge_pkgs,
    ]
    if install_types:
        cmds.extend(["--install-types", "--non-interactive"])
    if daemon:
        # see related issue https://github.com/python/mypy/issues/9475
        cmds.extend(["--follow-imports=normal"])
    # use pseudo-terminal for colorized output
    ctx.run(" ".join(cmds), echo=True, pty=True)


@invoke.task(aliases=["get-stats"])
def get_usage_stats_json(ctx):
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
def mv_usage_stats_json(ctx):
    """
    Use databricks-cli lib to move usage stats event examples to dbfs:/
    """
    version = usage_stats_utils.get_gx_version()
    outfile = f"v{version}_example_events.json"
    cmd = "databricks fs cp --overwrite {0} dbfs:/schemas/{0}"
    cmd = cmd.format(outfile)
    ctx.run(cmd)
    print(f"'{outfile}' copied to dbfs.")
