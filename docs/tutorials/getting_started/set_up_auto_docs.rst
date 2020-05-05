.. _getting_started__set_up_auto_docs:

Set up Auto Docs
================

:ref:`Auto Docs<data_docs>` translate :ref:`Expectations`, :ref:`Validation Results`, and other metadata into clean, human-readable documentation. Automatically compiling your data documentation from your data tests guarantees that your documentation will never go stale---a huge workflow change from the sprawling, chronically-out-of-date data wikis that many data teams otherwise have to maintain.

When you open the window, you'll see a static website that looks like this:

<<<SCREENSHOT GOES HERE>>>

What just happened?
-------------------

In the previous step, our data Profiler generated a set of new Expectations. We just used Auto Docs to compile those Expectations to HTML, which you can see here. This page is *prescriptive*: it describes how data *should* look:

<<<SCREENSHOT GOES HERE>>>

As mentioned in the previous step, our Profiler also generated Validation Results, which you can see here. This page is *descriptive*: it describes how a specific batch of data (in this case, the data we Profiled) actually looked when validated.

<<<SCREENSHOT GOES HERE>>>

Finally, your static site includes a set of pages for future data validations. These pages are *diagnostic*: they describe whether and how a specific batch of data differed from the ideal prescribed by a set of Expectations.

<<<SCREENSHOT GOES HERE>>>


This ability to translate between machine- and human-readable formats makes Great Expectations a powerful tool for both testing and communicating about data.

The Auto Docs build chain
-------------------------

There's :ref:`quite a bit going on under the hood`_ here, but for the moment the details don't matter. You can think of Auto Docs as an elaborate makefile. Its inputs are Expectations, Validation Results, and other metadata. Its output is a static website.

<<<DIAGRAM>>>

Possibilities to keep in mind for the future:

* **The same metadata can be rendered more than once.** For example, you might want to use the same Expectation in a bullet point, and as an entry within a data dictionary on a different page.
* **You can build more than one target.** For example, if you share data outside your company, you might want one site to support internal engineering and analytics, and another site to support downstream clients consuming the data.
* **The build target for rendering does not need to be HTML.** For example: Slack messages and email notifications.
* **All components of the build chain are pluggable and extensible.** Like everything else in Great Expectations, you can extend Renderers.

<<<DIAGRAM>>>

In the next step, we'll show how to render a disposable Validation notebook that you can use to review data and Validation Results and edit your Expectations.

.. attention:: Great Expectations Renderers are still an experimental feature. They're very useful, but expect changes in API, behavior, and XXX.

