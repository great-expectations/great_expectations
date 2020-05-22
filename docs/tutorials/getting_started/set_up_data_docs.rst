.. _getting_started__set_up_auto_docs:

Set up Data Docs
================

:ref:`Data Docs` translate :ref:`Expectations`, :ref:`Validation Results`, and other metadata into clean, human-readable documentation. Automatically compiling your data documentation from your data tests guarantees that your documentation will never go stale---a huge workflow change from the sprawling, chronically-out-of-date data wikis that many data teams otherwise have to maintain.

Go ahead and tell the CLI you want to build Data Docs:

.. code-block:: bash

    Would you like to build Data Docs? [Y/n]: 

    The following Data Docs sites will be built:

     - local_site: file:///Users/eugenemandel/projects/fellows/great_expectations/uncommitted/data_docs/local_site/index.html

    OK to proceed? [Y/n]: 

    Building Data Docs...

    Done building Data Docs

    Would you like to view your new Expectations in Data Docs? This will open a new browser window. [Y/n]: 


When you open the window, you'll see a static website that looks like this:

<<<SCREENSHOT GOES HERE>>>

What just happened?
-------------------

In the previous step, our data Profiler generated a set of new Expectations. We just used Data Docs to compile those Expectations to HTML, which you can see here. This page is *prescriptive*: it describes how data *should* look:

<<<SCREENSHOT GOES HERE>>>

As mentioned in the previous step, our Profiler also generated Validation Results, which you can see here. This page is *descriptive*: it describes how a specific batch of data (in this case, the data we Profiled) actually looked when validated.

<<<SCREENSHOT GOES HERE>>>

Finally, your static site includes a set of pages for future data validations. These pages are *diagnostic*: they describe whether and how a specific batch of data differed from the ideal prescribed by a set of Expectations.

<<<SCREENSHOT GOES HERE>>>

For now, your static site is built and stored locally. In the last step of the tutorial, we'll explain options for configuring, hosting and sharing it.


The Data Docs build chain
-------------------------

There's quite a bit going on under the hood here, but for the moment the details don't matter. You can think of Data Docs as an elaborate makefile. The steps in the makefile are called :ref:`Renderers`. Their inputs are Expectations, Validation Results, and other metadata. Their collective output is a static website.

.. figure:: ../../images/data_docs_conceptual_diagram.png

Possibilities to keep in mind for the future:

* **The same inputs can be rendered more than once.** For example, you might want to use the same Expectation in a bullet point, and as an entry within a data dictionary on a different page.
* **You can build more than one target.** For example, if you share data outside your company, you might want one site to support internal engineering and analytics, and another site to support downstream clients consuming the data.
* **The build target for rendering does not need to be HTML.** For example, you can use Renderers to generate Slack messages and email notifications.
* **All components of the build chain are pluggable and extensible.** Like everything else in Great Expectations, you can customize and extend Renderers.

This ability to translate between machine- and human-readable formats makes Great Expectations a powerful tool---not just for data testing, but also data communication and collaboration.

.. attention:: Great Expectations Renderers are still in beta. They're very useful, but expect changes in appearance, behavior, and API.

In the next step, we will complete set up by showing you how to Validate data, review the results, and edit your Expectations. Incidentally, this step will use another kind of renderer: a disposable notebook generated specifically to support this workflow.
