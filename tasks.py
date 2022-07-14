"""
PyInvoke developer task file

These tasks can be run using `invoke <NAME>` or `inv <NAME>` from the project root.

To show all available tasks `invoke --list`

To show task help page `invoke <NAME> --help`
"""
import invoke

from scripts import check_type_hint_coverage


@invoke.task
def sort(ctx, path=".", check=False, exclude=None):
    """Sort module imports"""
    args = ["isort", path, "--profile", "black"]
    if check:
        args.append("--check-only")
    if exclude:
        args.extend(["--skip", exclude])
    ctx.run(" ".join(args))


@invoke.task
def fmt(ctx, path=".", sort_=True, check=False, exclude=None):
    """Run code formatter"""
    if sort_:
        sort(ctx, path, check=check, exclude=exclude)

    args = ["black", path]
    if check:
        args.append("--check")
    if exclude:
        args.extend(["--exclude", exclude])
    ctx.run(" ".join(args))


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


@invoke.task(iterable=["modules"])
def types(ctx, modules, coverage=False, install_types=False):
    """Static Type checking"""
    modules = modules or [
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
    # TODO: fix modules with low error count
    ge_modules = [f"great_expectations/{m}" for m in modules]
    args = [
        "mypy",
        *ge_modules,
        "--ignore-missing-imports",
        "--follow-imports=silent",
    ]
    if install_types:
        args.extend(["--install-types", "--non-interactive"])
    if coverage:
        check_type_hint_coverage.main()
    else:
        ctx.run(" ".join(args), echo=True)
