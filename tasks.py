"""
PyInvoke developer task file
https://www.pyinvoke.org/

These tasks can be run using `invoke <NAME>` or `inv <NAME>` from the project root.

To show all available tasks `invoke --list`

To show task help page `invoke <NAME> --help`
"""
import invoke

from scripts import check_type_hint_coverage


@invoke.task
def sort(ctx, path=".", check=False, exclude=None):
    """Sort module imports"""
    cmds = ["isort", path, "--profile", "black"]
    if check:
        cmds.append("--check-only")
    if exclude:
        cmds.extend(["--skip", exclude])
    ctx.run(" ".join(cmds))


@invoke.task
def fmt(ctx, path=".", sort_=True, check=False, exclude=None):
    """Run code formatter"""
    if sort_:
        sort(ctx, path, check=check, exclude=exclude)

    cmds = ["black", path]
    if check:
        cmds.append("--check")
    if exclude:
        cmds.extend(["--exclude", exclude])
    ctx.run(" ".join(cmds))


@invoke.task
def lint(ctx, path="great_expectations/core"):
    """Run code linter"""
    ctx.run(" ".join(["flake8", path]))


@invoke.task
def hooks(ctx, changes_only=False, diff=False):
    """Run pre-commit hooks"""
    cmds = ["pre-commit", "run"]
    if diff:
        cmds.append("--show-diff-on-failure")
    # used in CI - runs faster and only checks files that have changed
    if changes_only:
        cmds.extend(["--from-ref", "origin/HEAD", "--to-ref", "HEAD"])
    else:
        cmds.extend(["--all-files"])
    ctx.run(" ".join(cmds))


@invoke.task
def type_coverage(ctx):
    """
    Check total type-hint coverage compared to `develop`.
    """
    try:
        check_type_hint_coverage.main()
    except AssertionError as err:
        raise invoke.Exit(message=str(err), code=1)


@invoke.task(iterable=["packages"])
def type_check(ctx, packages, install_types=False):
    """Run mypy static type-checking on select packages."""
    # numbers next to each package represent the last known number of typing errors.
    # when errors reach 0 please uncomment the module so it becomes type-checked by default.
    packages = packages or [
        # "checkpoint",  # 78
        # "cli",  # 237
        # "core",  # 237
        "data_asset",  # 1
        # "data_context",  # 228
        # "datasource",  # 98
        "exceptions",  # 2
        # "execution_engine",  # 111
        # "expectations",  # 453
        "jupyter_ux",  # 4
        # "marshmallow__shade",  # 19
        "profile",  # 9
        # "render",  # 87
        # "rule_based_profiler",  # 469
        "self_check",  # 10
        "types",  # 3
        # "validation_operators", # 47
        # "validator",  # 46
    ]
    ge_pkgs = [f"great_expectations/{p}" for p in packages]
    cmds = [
        "mypy",
        *ge_pkgs,
        "--ignore-missing-imports",
        "--follow-imports=silent",
    ]
    if install_types:
        cmds.extend(["--install-types", "--non-interactive"])
    ctx.run(" ".join(cmds), echo=True)
