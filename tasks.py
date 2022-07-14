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
def hooks(ctx):
    """Run pre-commit hooks"""
    raise NotImplementedError


@invoke.task(iterable=["modules"])
def types(ctx, modules, coverage=False, install_types=False):
    """Static Type checking"""
    modules = modules or [
        "checkpoint",
        "cli",
        "core",
        "data_asset",
        "data_context",
        "datasource",
        "exceptions",
        # TODO indentify modules that are close to being 100% type correct
    ]
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
        print(args)
        ctx.run(" ".join(args))
