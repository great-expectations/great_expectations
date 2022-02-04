---
title: Documentation style guide
---

### Organization

Within the table of contents, each section has specific role to play. Broadly speaking, we follow Divio’s excellent [Documentation System](https://documentation.divio.com/), with the caveat that our “Core concepts section is their “Explanation” section, and our API Reference” section is their “Reference section”.

* **Introduction** explains the Why of Great Expectations, so that potential users can quickly decide whether or not the library can help them.

* **Getting Started with Great Expectations** help users and contributors get started quickly. Along the way they orient new users to concepts that will be important to know later.

* **How-to guides** help users accomplish specific goals that go beyond the generic tutorials. Article titles within this section always start with “How to”: “How to create custom Expectations”. They often reference specific tools or infrastructure: “How to validate Expectations from within a notebook”, “How to build Data Docs in S3.” For additional information, please see [How to write a how-to-guide](../../guides/miscellaneous/how_to_write_a_how_to_guide.md).

* **Deployment patterns** explains how to deploy Great Expectations alongside other data tools.

* **Reference** articles explain the architecture of Great Expectations. Within this section, Core Concepts articles explain the essential elements of the project, discuss alternatives and options, and provide context, history, and direction for the project. Reference articles avoid giving specific technical advice. They also avoid implementation details that can be captured in docstrings instead. Docstrings themselves are surfaced in the API Reference.

* **Community resources** helps expand the Great Expectations community by explaining how to get in touch to ask questions, make contributions, etc.

* **Contributing** is all about how to improve Great Expectations as a contributor.

* **Changelog** contains the changelog for the project.


### General conventions

:::info Note
This style guide will be enforced for all incoming PRs. However, certain legacy areas within the repo do not yet fully adhere to the style guide. We welcome PRs to bring these areas up to code.
:::


* The **project name “Great Expectations” is always spaced and capitalized.** Good: “Great Expectations”. Bad: “great_expectations”, “great expectations”, “GE.”

* **We refer to ourselves in the first person plural.** Good: “we”, “our”. Bad: “I”. This helps us avoid awkward passive sentences. Occasionally, we refer to ourselves as “the Great Expectations team” (or community) for clarity.

* **We refer to developers and users as “you”.** Good: “you can,” “you might want to.”

* **We reserve the word “should” for strong directives, not just helpful guidance.**



### Titles

* **Titles and headers are capitalized like sentences**.

	* Yep: “Installing within a project.”
	* Nope: “Installing Within a Project.”

* **For sections within “how to”-type guides, titles should be short, imperative sentences**.

	Avoid extra words. Good: “Configure data documentation”. Nope: “Configuring data documentation”. Avoid: “Configure documentation for your data”



### Core concepts and classes

* **Core concepts are always capitalized, and always are linked on first reference within each page.**

	If it's important enough to show up in the Core Concepts section, it should be capitalized when it occurs in text. Pretend the docs are a fantasy novel, and core concepts are magic.

	* Wrong: “You can create expectation suites as follows…”
	* Better: “You can create Expectation Suites as follows…”
	* Avoid: “You can create suites of Expectations as follows…”

* **Class names are written in upper camel case, and always linked on first reference.** 

	* Good: “ValidationOperator.” 
	* Bad: “validationOperator”, “validation operator”. 

	If a word is both a core concept and a class name, prefer the core concept unless the text refers specifically to the class.

* **Core concepts and class names are always linked on first reference within each page&mdash;except within the Getting Started guides**.

	The first time a given core concept or class name shows up on any given page, it should be linked to the appropriate page. Additional references within that page should not be linked, unless the point of the text is specifically to link to the concept.

	Within the Getting Started section, we make an exception for this rule, to avoid distracting new users with many extraneous links.

