.. _how_to_guides__miscellaneous__how_to_write_a_how_to_guide:

How to write a how to guide
===========================

This guide shows how to create a new how-to guide in Great Expectations. By writing guides with consistent structure and styling, you can get your PRs approved faster and make the Great Expectations docs discoverable, useful, and maintainable.


Steps
-----

#. Copy the :ref:`How-to guide template file <how_to_guides__miscellaneous__how_to_template>` to the appropriate subdirectory of ``docs/guides/how_to_guides/``, and rename it.
#. Write a title and purpose paragraph.
#. Decide whether you're writing a :ref:`code-heavy or process-heavy <code_heavy_vs_process_heavy_guides>` guide, and :ref:`adjust your formatting <indentation_bolding_and_code_blocks>` appropriately.
#. Fill out the Prerequisites info box (see :ref:`How-to guide template file <how_to_guides__miscellaneous__how_to_template>`). The header of the info box says: "This how-to guide assumes you have already:". Place each prerequisite under its own bullet and phrase it using the style in the template: "done something".
#. Fill in the Steps section, making sure to include bash, yml, and code snippets as appropriate.

	- Hint: it's often most efficient to run through the technical steps in a notebook or at the command line before starting to write. Then you can simply copy-paste content into code blocks, and write headers and text to connect them.
	- After you finish writing, you should definitely follow your own steps from start to finish at least once, to make sure there aren’t any gaps.
	
#. If needed, add content to Additional Notes and/or Additional Resources. These sections supplement the article with information that would be distracting to include in Steps. It’s fine for them to be empty.
#. Enable comments for your How-to guide, by following :ref:`these instructions <how_to_guides__miscellaneous__how_to_add_comments_to_a_page_in_documentation>`.
#. Scan your article to make sure it follows the :ref:`Style guide <contributing__style_guide>`. If you’re not familiar with the Style Guide, that’s okay: your PR reviewer will also check for style and let you know if we find any issues.
#. Submit your PR!

Additional Notes
----------------

Purpose of a how-to guide
#########################

The purpose of a how-to guide is to *replicate*, NOT to *teach or explain*. Teaching and explaining Great Expectations concepts are covered in the :ref:`reference__core_concepts` reference section.

	* Assume that the user has already successfully run ``great_expectations init`` and has a working deployment of Great Expectations.
	* Assume that the user is already familiar with core concepts in Great Expectations: Expectations, Data Contexts, Validation, Datasources, etc. etc. You don’t need to spend any time explaining these things.
	* If you’re integrating with another system, assume that the user is familiar with that system. ie. If you’re writing the "How to configure a Snowflake Datasource," don’t spend any words explaining Snowflake or any of its core concepts.
	* If there are important, non-obvious principles for how Great Expectations relates to other systems, you may include them in the guide. If they are short (1-2 sentences, max), they can go in the Steps section. Otherwise, please put them in Additional Notes.

Remember, the goal is to help users successfully replicate specific steps as simply as possible. Surprisingly often, it turns out to best to not include explanation at all, since it can distract from the main purpose of the guide. If you feel you must include it, shorter is better.

Structure of a how-to guide
###########################

With rare exceptions, How-to guides follow this structure:

	1. Title
	2. Purpose
	3. Steps
	4. Additional Notes (optional)
	5. Additional Resources (optional)

**Title**: "How to X". See the :ref:`Style guide <contributing__style_guide>` for specific guidance on how to phrase and format titles.

**Purpose paragraph**: A single, short paragraph to state the purpose of the guide, and motivate it if necessary.

    "This guide will help you publish an Data Docs site directly to S3. Publishing a site this way makes reviewing and acting on Validation Results easy in a team, and provides a central location to review Expectations."

Sometimes motivation can be a simple statement of purpose:

    "This guide will help you connect to a MongoDB Datasource.”

If the user has data in Mongo and wants to configure a Datasource, no additional justification is needed.

**Steps**: Steps describe the golden path steps for successful replication.

* Most steps will include ``inline code``, such as a bash command, or an example yml snippet or two.
* Snippets should be as short as possible, but no shorter. In general, you can think of the snippet like a diff: what needs to change to accomplish this step?
* Steps should be linear. “Do A, then B, then C.” Avoid complex loops and/or branching. If loops or branching are needed, it is likely a sign that the scope of the guide is too big. In that case, consider options for splitting it into more than one how-to guide.

**Additional notes**: This section covers errata that would be distracting to include in Steps. It’s fine for it to be empty.

**Additional resources**: Additional resources, usually external (i.e. not within the Great Expectations documentation) and usually shown as a list. To avoid link rot, please use this section sparingly, and prefer links to stable, well-maintained resources.

.. _code_heavy_vs_process_heavy_guides:

Code-heavy vs process-heavy guides
##################################

Broadly speaking, there are two kinds of How-to Guides: code-heavy and process-heavy. All guides are about following a specific sequence of steps. In code-heavy guides, most or all of the steps are expressed in technical syntax: code snippets, JSON or YAML objects, CLI commands, etc. In process-heavy guides, many of the steps are things that must be done manually.

Most guides are code-heavy. When writing a guide that could go either way, please prefer code-heavy, since they tend to make for better replication. (This guide happens to be process-heavy, because it's about writing.)

.. _indentation_bolding_and_code_blocks:

Indentation, bolding, and code blocks
#####################################

**For code-heavy guides**

- Treat the first sentence of each step like a header.

	- Use short, complete, imperative sentences: ("Paste the YAML snippet into your config file", "Run great_expectations init")
	- Header text should be **bold**.
	- Avoid `links <https://greatexpectations.io>`_ or ``inline code`` in headers, since RST files do not support nesting them within bolded text. If your header must include text that would normally be a link or inline code, please repeat it in the body text, and use a link or code block there.

- Indent content within steps.
- Any time the user needs to do something, it should be in a code block.

	- Please follow this convention even if the text in the code block is somewhat redundant against the text of the step. 
	- Clear, sequential code blocks are easy for the eye to follow. They encourage a health copy-and-modify development pattern.

- All of these styles are modeled in the :ref:`How-to guide template file <how_to_guides__miscellaneous__how_to_template>`. If you use that template as your guide, you'll be off to a very good start.

**For process-heavy guides**

- Do not separate headers or bold first sentences.
- Avoid big blocks of text without visual cues for how to read it. Indentation and sub-bullets are your friends.
- When including a code block, please follow the same conventions as for code-heavy guides.
- All of these styles are modeled in the this .rst file.


Using tabs to differentiate guides for different APIs
#####################################################

During the process of writing documentation for Great Expectations 0.13, there rose a need to differentiate between documentation for GE up to 0.12.x, and GE 0.13 and beyond.

The use of ``content-tabs`` allows for both documentation to co-exist in the same how-to-doc.

The following code snippet shows how two tabs (``tab0`` and ``tab1``) can be created with the associated ``title`` and content. For more information on ``content-tabs``, please refer to the following link : `https://sphinxcontrib-contentui.readthedocs.io/en/latest/tabs.html <https://sphinxcontrib-contentui.readthedocs.io/en/latest/tabs.html>`_

    .. code-block:: rst

        .. content-tabs::

            .. tab-container:: tab0
                :title: Show Docs for V2 (Batch Kwargs) API

                # Content for Stable API #

            .. tab-container:: tab1
                :title: Show Docs for V3 (Batch Request) API

                # Content for Experimental API #


Additional Resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.

.. discourse::
   :topic_identifier: 230
