---
title: Code style guide
---

:::info Note
This style guide will be enforced for all incoming PRs. However, certain legacy areas within the repo do not yet fully adhere to the style guide. We welcome PRs to bring these areas up to code.
:::

### Code

* **Methods are almost always named using snake_case.**

* **Methods that behave as operators (e.g. comparison or equality) are named using camelCase.** These methods are rare and should be changed with great caution. Please reach out to us if you see the need for a change of this kind.

* **Experimental methods should log an experimental warning when called:** “Warning: some_method is experimental. Methods, APIs, and core behavior may change in the future.”

* **Experimental classes should log an experimental warning when initialized:** “Warning: great_expectations.some_module.SomeClass is experimental. Methods, APIs, and core behavior may change in the future.”

* **Docstrings are highly recommended.** We use the Sphinx’s [Napoleon extension](http://www.sphinx-doc.org/en/master/ext/napoleon.html) to build documentation from Google-style docstrings.

### Tasks

Common developer tasks such as linting, formatting, type-checking are defined in [`tasks.py`](https://github.com/great-expectations/great_expectations/blob/develop/tasks.py) and runnable via the [`invoke` task runner library](https://www.pyinvoke.org/).

To see the available task run `invoke --list` from the project root.

```console
$ invoke --list                                                                                                                                                                                                                                                                                                                                               in zsh at 14:47:20
Available tasks:

  fmt             Run code formatter.
  hooks           Run and manage pre-commit hooks.
  lint            Run code linter
  sort            Sort module imports.
  type-coverage   Check total type-hint coverage compared to `develop`.
  upgrade         Run code syntax upgrades.
```

For detailed usage guide, `invoke <TASK-NAME> --help`

```console
$ invoke fmt --help                                                                                                                                                                                                                                                                                                                                           in zsh at 14:58:01
Usage: inv[oke] [--core-opts] fmt [--options] [other tasks here ...]

Docstring:
  Run code formatter.

Options:
  -c, --check                   Only checks for needed changes without writing back. Exit with error code if changes needed.
  -e STRING, --exclude=STRING   Exclude files or directories
  -p STRING, --path=STRING      Target path. (Default: .)
  -s, --[no-]sort               Disable import sorting. Runs by default.
```

### Linting

Our CI system will check using `black`, and `ruff`.

If you have already committed files but are seeing errors during the continuous integration tests, you can run tests manually:

```console
black <PATH/TO/YOUR/CHANGES>
ruff <PATH/TO/YOUR/CHANGES> --fix
```

### Type Checking

Our CI system will perform static type-checking using [mypy](https://mypy.readthedocs.io/en/stable/index.html#).

`contrib` and other select `great_expectations/` packages are excluded from type-checking.
See the `mypy` section in [`pyproject.toml`](https://github.com/great-expectations/great_expectations/blob/develop/pyproject.toml) for more details.

To verify your code will pass the CI type-checker, run `invoke type-check --install-types`.
Or [run `mypy`](https://mypy.readthedocs.io/en/stable/running_mypy.html) directly against the packages listed above.

### Expectations

* **Use unambiguous Expectation names**, even if they’re a bit longer, e.g. `expect_columns_to_match_ordered_list` instead of `expect_columns_to_be`.

* **Avoid abbreviations**, e.g. `column_index` instead of `column_idx`.

* **Expectation names should be prefixed to reflect their base classes:**

| Base class                   |  prefix                         |
|------------------------------|---------------------------------|
| `Expectation`                |  `expect_...`                   | 
| `BatchExpectation`           |  `expect_table_...`             | 
| `ColumnMapExpectation`       |  `expect_column_values_...`     | 
| `ColumnAggregateExpectation` |  `expect_column_...`            | 
| `ColumnPairMapExpectation`   |  `expect_column_pair_values...` | 
| `MultiColumnMapExpectation`  |  `expect_multicolumn_values...` | 

