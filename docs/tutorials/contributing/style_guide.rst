.. _contributing_style_guide:


Style Guide
===========

.. Note:: This Style Guide will be reviewed and enforced for an incoming PRs. However, certain legacy areas within the docs and code do not follow it fully. We welcome PRs to bring these ares up to code.


General conventions
-------------------

* The project name "Great Expectations" is always spaced and capitalized.

    * Good: "Great Expectations"
    * Bad: "great_expectations", "great expectations", "GE."

* Headers are capitalized like sentences.

    * Good: "Installing within a project." Bad: "Installing Within a Project."

* We refer to ourselves in the first person plural.

    * Good: "we", "our".
    * Bad: "I"
    * This helps us avoid awkward passive sentences.
    * Occasionally, we refer to ourselves as "the Great Expectations team" (or community) for clarity.

* We refer to developers and users as "you".

    * Good: "you can," "you should," "you might want to."

* Core concepts are always capitalized, and always are linked on first reference within each page.

    * Pretend the docs are a fantasy novel, and core concepts are magic.
    * Good: "Like assertions in traditional python unit tests, :ref:`Expectations` provide a flexible, declarative language for describing expected behavior."

* Class names are written in upper camel case, and always linked on first reference.

    * Good: "ValidationOperator."
    * Bad: "validationOperator", "validation operator".
    * If a word is both a core concept and a class name, link to the core concept unless the text refers specifically to the class.

* Dickens allusions, puns, and references are strongly encouraged.

    * Strict adherence to Dickens' canon is required.
    * "A Muppet Christmas Carol" is considered canon.

code
----

* Methods are usually named using snake_case.
* Methods that behave as operators (e.g. comparison or equality) are named using camelCase. These methods are rare and should be changed with great caution. Please reach out to James Campbell if you see the need for a change of this kind.
* Ensure any new features or behavioral differences introduced by your changes are documented in the docs, and ensure you have docstrings on your contributions. We use the Sphinx's `Napoleon extension <http://www.sphinx-doc.org/en/master/ext/napoleon.html>`__ to build documentation from Google-style docstrings.

**Expectations**

* Use unambiguous Expectation names, even if they're a bit longer, e.g. ``expect_columns_to_match_ordered_list`` instead of ``expect_columns_to_be``.
* Avoid abbreviations, e.g. ``column_index`` instead of ``column_idx``.
* Expectation names should reflect their decorators:

    * ``expect_table_...`` for methods decorated directly with ``@expectation``
    * ``expect_column_values_...`` for ``@column_map_expectation``
    * ``expect_column_...`` for ``@column_aggregate_expectation``
    * ``expect_column_pair_values...`` for ``@column_pair_map_expectation``

**The CLI**

The :ref:`CLI <command_line>` has some conventions of its own.

* The CLI never writes to disk without asking first.
* Questions are always phrased as conversational sentences.
* Sections are divided by headers: "========== Profiling =========="
* We use punctuation: Please finish sentences with periods, questions marks, or an occasional exclamation point.
* Keep indentation and line spacing consistent! (We're pythonistas, natch.)
* Include exactly one blank line after every question.
* Within those constraints, shorter is better. When in doubt, shorten.
* Clickable links (usually to documentation) are blue.
* Copyable bash commands are green.
* All top-level bash commands must be nouns: "docs build", not "build docs"


.rst files
----------

**Organization**

Within the table of contents, each section has specific role to play.

* *Introduction* explains the Why of Great Expectations, so that potential users can quickly decide whether or not the library can help them.
* *Getting started* helps users get started quickly. Along the way it briefly orients new users to concepts that will be important to learn later.
* *Community* helps expand the Great Expectations community by explaining how to get in touch to ask questions, make contributions, etc.
* *Core concepts* are always phrased as nouns. These docs provide more examples of usage, and deeper explanations for why Great Expectations is set up the way it is.
* *reference* are always phrased as verbs: "Creating custom Expectations", "Deploying Great Expectations in Spark", etc. They help users accomplish specific goals that go beyond the generic Getting Started tutorials.
* *Changelog and roadmap*
* *Module docs*


**Titles**

* Only the first word in each title is capitalized:

        * Yep: “File a bug report or feature request”
        * Nope: “File a Bug Report or Feature Request”

* For sections within “how to”-type guides, titles should be short, imperative sentences. Avoid extra words:

        * Good: “Configure data documentation”
        * Nope: “Configuring data documentation”
        * Avoid: “Configure documentation for your data”

* Please follow the `Sphinx guide for sections <http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#sections>`__ to determine which of the many, confusing .rst underlining conventions to use.



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

