.. _how_to_guides__miscellaneous__how_to_template:

.. attention:: This doc is a template. Please do not attempt to follow these instructions.

TEMPLATE: How to {do something}
===============================

This guide will help you {do something.} {That something is important or useful, because of some reason.}

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - You have adopted a puppy

Steps
-----

#. First, do something bashy.

    .. code-block:: yaml

        something --foo bar

#. Next, do something with yml.

    .. code-block:: yaml

        site_section_builders:
          expectations: # Look, comments work, too!
            class_name: DefaultSiteSectionBuilder
            source_store_name: expectations_store
            renderer:
              module_name: great_expectations.render.renderer
              class_name: ExpectationSuitePageRenderer


#. Next, try a python snippet or two.

    .. code-block:: python

        batch.expect_table_row_count_to_be_between(min_value=1000, max_value=4000)

    Here's a pinch of connecting text.

    .. code-block:: python

        batch.expect_table_row_count_to_be_between(min_value=2000, max_value=5000)


Additional notes
----------------

How-to guides are not about teaching or explanation. They are about providing clear, bite-sized replication steps. If you **must** include a longer explanation, it should go in this section.

Additional resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.

Comments
--------

.. discourse::
   :topic_identifier: {{topic_id}}
