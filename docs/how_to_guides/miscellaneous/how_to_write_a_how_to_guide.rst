.. _how_to_guides__miscellaneous__how_to_write_a_how_to_guide:

How to write a how to guide
===========================

This guide shows how to create a new how-to guide in Great Expectations. By writing guides with consistent structure and styling, you can get your PRs approved faster and make the Great Expectations docs discoverable, useful, and maintainable.

Steps
-----

1. Copy the :ref:`How-to guide template file <how_to_guides__miscellaneous__how_to_template>` to the appropriate subdirectory of ``docs/how_to_guides/``, and rename it.
2. Write a title and purpose paragraph.
3. Fill in the Steps section, making sure to include bash, yml, and code snippets as appropriate.
4. Starting from a clean install of Great Expectations, follow your own steps from start to finish, to make sure there aren’t any gaps.
5. If needed, add content to Additional Notes and/or Additional Resources. These sections cover errata that would be distracting to include in Steps. It’s fine for them to be empty.
6. Scan your article to make sure it follows the :ref:`Style guide`. If you’re not familiar with the Style Guide, that’s okay: your PR reviewer will also check for style and let you know if we find any issues.
7. Submit your PR!

Additional Notes
----------------

Purpose of a how-to guide
#########################

The purpose of a how-to guide is to *replicate*, NOT to *teach or explain*.

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

**Title**: "How to X". See the :ref:`Style guide` for specific guidance on how to phrase and format titles.

**Purpose paragraph**: A single, short paragraph to state the purpose of the guide, and motivate it if necessary.

    "This guide will help you publish an Auto Docs site directly to S3. Publishing a site this way makes reviewing and acting on Validation Results easy in a team, and provides a central location to review Expectations."

Sometimes motivation can be a simple statement of purpose:
    
    "This guide will help you connect to a MongoDB Datasource.”
    
If the user has data in Mongo and wants to configure a Datasource, no additional justification is needed.

**Steps**: Steps describe the golden path steps for successful replication.

* Most steps will include ``inline code``, such as a bash command, or an example yml snippet or two.
* Snippets should be as short as possible, but no shorter. In general, you can think of the snippet like a diff: what needs to change to accomplish this step?
* Steps should be linear. “Do A, then B, then C.” Avoid complex loops and/or branching. If loops or branching are needed, it is likely a sign that the scope of the guide is too big. In that case, consider options for splitting it into more than one how-to guide.

**Additional notes**: This section covers errata that would be distracting to include in Steps. It’s fine for it to be empty.

**Additional resources**: Additional resources, usually external (i.e. not within the Great Expectations documentation) and usually shown as a list. To avoid link rot, please use this section sparingly, and prefer links to stable, well-maintained resources.

Additional Resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.