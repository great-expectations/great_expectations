.. _contributing_style_guide:


Style Guide
===========

* Ensure any new features or behavioral differences introduced by your changes are documented in the docs, and ensure you have docstrings on your contributions. We use the Sphinx's `Napoleon extension <http://www.sphinx-doc.org/en/master/ext/napoleon.html>`__ to build documentation from Google-style docstrings.
* Avoid abbreviations, e.g. use `column_index` instead of `column_idx`.
* Use unambiguous expectation names, even if they're a bit longer, e.g. use `expect_columns_to_match_ordered_list` instead of `expect_columns_to_be`.
* Expectation names should reflect their decorators:

    * `expect_table_...` for methods decorated directly with `@expectation`
    * `expect_column_values_...` for `@column_map_expectation`
    * `expect_column_...` for `@column_aggregate_expectation`
    * `expect_column_pair_values...` for `@column_pair_map_expectation`

These guidelines should be followed consistently for methods and variables exposed in the API. They aren't intended to be strict rules for every internal line of code in every function.

* Methods are usually named using snake_case.
* Methods that behave as operators (e.g. comparison or equality) are named using camelCase. These methods are rare and should be changed with great caution. Please reach out to James Campbell if you see the need for a change of this kind.
* Lint your contribution. Our CI system will check using ``black`` and ``isort``. We have a git pre-commit configuration in the repo, so you can just run ``pre-commit install`` to automatically run your changes through the linting process before submitting.

.rst files
----------

**Titles**

* Only the first word in each title is capitalized:

        * Yep: “File a bug report or feature request”
        * Nope: “File a Bug Report or Feature Request”

* For sections within “how to”-type guides, titles should be short, imperative sentences. Avoid extra words:

        * Good: “Configure data documentation”
        * Nope: “Configuring data documentation”
        * Avoid: “Configure documentation for your data”


**File names and RST refs**

* File names should parallel titles, so that URLs and titles are similar. Use snake case for file names.
    
    * For example: the page titled ``Initialize a project`` has this filename: ``initialize_a_project.rst``, which produces this URL: ``initialize_a_project.html``

* Refs are ``_{filename}`` or ``_{folder_name}__{filename}``.
    
    * Ex: `_getting_started__initialize_a_project`


**Classes**

* When using referencing an object that has a specific class name, prefer the class name. When using class names, use.

        * Acceptable: “You can create suites of Expectations as follows…”
        * Better: “You can create ExpectationSuites as follows…”
        * Wrong: “You can create expectation suites as follows…”

* (Not yet implemented: Link class names on first reference within each page.)


**Code formatting**

* For inline code in RST, make sure to use double backticks. This isn’t markdown, folks:

        * Yep: The ``init`` command will walk you through setting up a new project and connecting to your data.
        * Nope: The `init` command will walk you through setting up a new project and connecting to your data.
