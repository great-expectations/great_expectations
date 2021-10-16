---
title: Documentation style guide
---

:::info Note
This style guide will be enforced for all incoming PRs. However, certain legacy areas within the repo do not yet fully adhere to the style guide. We welcome PRs to bring these areas up to code.
:::

### General conventions

* The **project name “Great Expectations” is always spaced and capitalized.** Good: “Great Expectations”. Bad: “great_expectations”, “great expectations”, “GE.”

* **We refer to ourselves in the first person plural.** Good: “we”, “our”. Bad: “I”. This helps us avoid awkward passive sentences. Occasionally, we refer to ourselves as “the Great Expectations team” (or community) for clarity.

* **We refer to developers and users as “you”.** Good: “you can,” “you might want to.”

* **We reserve the word “should” for strong directives, not just helpful guidance.**

### Organization

Within the table of contents, each section has specific role to play. Broadly speaking, we follow Divio’s excellent Documentation System, with the caveat that our “Reference” section is their “Explanation” section, and our “Module docs” section is their “Reference section”.

* **Introduction** explains the Why of Great Expectations, so that potential users can quickly decide whether or not the library can help them.

* **Tutorials** help users and contributors get started quickly. Along the way they orient new users to concepts that will be important to know later.

* **How-to guides** help users accomplish specific goals that go beyond the generic tutorials. Article titles within this section always start with “How to”: “How to create custom Expectations”. They often reference specific tools or infrastructure: “How to validate Expectations from within a notebook”, “How to build Data Docs in S3.”

* **Reference** articles explain the architecture of Great Expectations. These articles explain core concepts, discuss alternatives and options, and provide context, history, and direction for the project. Reference articles avoid giving specific technical advice. They also avoid implementation details that can be captured in docstrings instead.

* **Community** helps expand the Great Expectations community by explaining how to get in touch to ask questions, make contributions, etc.

* **Module docs** link through to module docstrings themselves.

### Titles

* **Headers are capitalized like sentences**. Yep: “Installing within a project.” Nope: “Installing Within a Project.”

* **For sections within “how to”-type guides, titles should be short, imperative sentences**. Avoid extra words. Good: “Configure data documentation”. Nope: “Configuring data documentation”. Avoid: “Configure documentation for your data”

* **Please follow the Sphinx guide for sections to determine which of the many, confusing .rst underlining conventions to use**: [Sphinx guide for sections](http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#sections)

### Core concepts and classes

* **Core concepts are always capitalized, and always are linked on first reference within each page**. Pretend the docs are a fantasy novel, and core concepts are magic.

	* Wrong: “You can create expectation suites as follows…”
	* Better: “You can create [Expectation Suites](http://localhost:3000/docs/reference/glossary_of_expectations) as follows…”
	* Avoid: “You can create suites of Expectations as follows…”

* **Class names are written in upper camel case, and always linked on first reference.** 

	* Good: “ValidationOperator.” 
	* Bad: “validationOperator”, “validation operator”. 

If a word is both a core concept and a class name, prefer the core concept unless the text refers specifically to the class.

### File names, RST refs, and links

* **File names should parallel titles, so that URLs and titles are similar**. For example: the page titled `Initialize a project` has this filename: `initialize_a_project.rst`, which produces this URL: `initialize_a_project.html`

* **Use snake case for file names.**

* **Refs are \``_{filename}\`` or \``_{folder_name}__{filename}\``**. Ex: `_getting_started__initialize_a_project`

* **Links to docs in the API Reference section**

	* Link to a module: `:mod:great_expectations.data_context.data_context` `great_expectations.data_context.data_context`

	* Abbreviated link to a module: :mod:`~great_expectations.data_context.data_context` data_context

	* Link to a class: :py:class:`great_expectations.data_context.data_context.BaseDataContext` great_expectations.data_context.data_context.BaseDataContext

	* Abbreviated link to a class: :py:class:`~great_expectations.data_context.data_context.BaseDataContext` BaseDataContext

	* Link to a method in a class: :py:meth:`great_expectations.data_context.data_context.BaseDataContext.validate_config` great_expectations.data_context.data_context.BaseDataContext.validate_config()

	* Abbreviated link to a method in a class: :py:meth:`~great_expectations.data_context.data_context.BaseDataContext.validate_config` validate_config()

	* Link to an attribute in a class: :py:attr:`great_expectations.data_context.data_context.BaseDataContext.GE_DIR` great_expectations.data_context.data_context.BaseDataContext.GE_DIR

	* Abbreviated link to an attribute in a class: :py:attr:`~great_expectations.data_context.data_context.BaseDataContext.GE_DIR` GE_DIR

	* Link to a function in a module: :py:attr:`great_expectations.jupyter_ux.display_column_evrs_as_section` great_expectations.jupyter_ux.display_column_evrs_as_section

	* Abbreviated to a function in a module: :py:attr:`~great_expectations.jupyter_ux.display_column_evrs_as_section` display_column_evrs_as_section

### Code formatting

* **For inline code in RST, make sure to use double backticks**. 

	* Yep: The `init` command will walk you through setting up a new project and connecting to your data.

	* Nope: The *init* command will walk you through setting up a new project and connecting to your data.

	* **For inline bash blocks, do not include a leading $.** It makes it hard for users to copy-paste code blocks.
