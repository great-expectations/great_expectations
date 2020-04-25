.. _improving_library_documentation:

================================================================================
Improve documentation for Great Expectations
================================================================================

Contributing to documentation is a great way to help the community and get your feet wet as an open source contributor.

If you see a typo/mistake/gap anywhere in the Great Expectation documentation, a quick PR would be much appreciated.

Style guide for documentation
-------------------------------------------

Note: as of 8/1/2019, this style guide is aspirational. It's applied unevenly across the repo. Look for this to tighten up a lot as we refine docs over the next couple of months.

**Basics**

* *The project name "Great Expectations" is always spaced and capitalized.* Good: "Great Expectations"; bad: "great_expectations", "great expectations", "GE."
* *Headers are capitalized like sentences.* Good: "Installing within a project." Bad: "Installing Within a Project."
* *We refer to ourselves in the first person plural*. Good: "we", "our". Bad: "I"   . This helps us avoid awkward passive sentences. Occasionally, we refer to ourselves as "the Great Expectations team" (or community) for clarity.
* *We refer to developers and users as "you"*: "you can," "you should," "you might want to."
* *Core concepts are always capitalized.* Pretend the docs are a fantasy novel, and core concepts are magic. Good: "Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior."
* *Core concepts are linked on first reference within each page.*
* *Class names are written in upper camel case and linked on first reference.* Good: "ValidationOperator." Bad: "validationOperator", "validation operator". If a word is both a core concept and a class name, link to the core concept unless the text refers specifically to the class.

**Organization**

Within the table of contents, each section has specific role to play.

* *Introduction* explains the Why of Great Expectations, so that potential users can quickly decide whether or not the library can help them.
* *Getting started* helps users get started quickly. Along the way it briefly orients new users to concepts that will be important to learn later.
* *Community* helps expand the Great Expectations community by explaining how to get in touch to ask questions, make contributions, etc.
* *Core concepts* are always phrased as nouns. These docs provide more examples of usage, and deeper explanations for why Great Expectations is set up the way it is.
* *reference* are always phrased as verbs: "Creating custom Expectations", "Deploying Great Expectations in Spark", etc. They help users accomplish specific goals that go beyond the generic Getting Started tutorials.
* *Changelog and roadmap*
* *Module docs*

**CLI**

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


Resources
===========
* We follow the
  `Sphinx guide for sections <http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#sections>`__.
